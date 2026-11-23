/*
Copyright 2025 YANDEX LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Bounds of the status worker pool. Each pass sizes the pool to the number of
	// BackupConfigs (statusWorkersFor); every worker runs one wal-g process at a time
	// (~46MB RSS on omega, 23.11.2026), so the ceiling also bounds the memory.
	minStatusReconcileWorkers = 3
	maxStatusReconcileWorkers = 16

	// Expected duration of one BackupConfig status reconcile, used to size the pool.
	// Omega 23.11.2026, 640 reconciles: mean 12.4s, p50 11s, p90 18s, max 26s.
	// ponytail: fixed estimate, not a measured average, so a slow storage cannot grow
	// the pool past what the small clusters' 512Mi limit holds; a slower pass is
	// only late, never dropped (deduplicated queue).
	statusReconcileEstimate = 15 * time.Second

	// Timeout for a single BackupConfig status reconciliation
	statusReconcileTimeout = 10 * time.Minute

	// Size of the shared reconciliation request queue
	statusReconcileQueueSize = 256

	// Attempts of each storage check (`wal-g st check read|write`) per reconcile
	// before its condition goes False: one retry rides out a storage latency blip.
	storageCheckAttempts = 2

	// Pause between two storage check attempts.
	defaultStorageCheckRetryBackoff = 5 * time.Second
)

// storageChecker is the part of *walg.Client the storage checks use; tests mock it.
type storageChecker interface {
	StorageCheckReadable(ctx context.Context, timeout time.Duration) (*cmd.RunResult, error)
	StorageCheckWritable(ctx context.Context, timeout time.Duration) (*cmd.RunResult, error)
}

// BackupConfigStatusController periodically reconciles BackupConfigStatus fields
// for all BackupConfig resources. It uses a static goroutine pool with a single
// shared queue to limit concurrency and prevent overloading S3 storage.
//
// Per-BackupConfig deduplication ensures that the same resource is not reconciled
// concurrently by multiple workers. Manual status updates can be enqueued
// via EnqueueStatusUpdate.
type BackupConfigStatusController struct {
	client        client.Client
	checkInterval time.Duration

	// storageCheckTimeout bounds a single `wal-g st check` call
	storageCheckTimeout time.Duration
	// storageCheckRetryBackoff is the pause between two storage check attempts
	storageCheckRetryBackoff time.Duration

	// queue is the single shared channel for all reconciliation requests
	queue chan types.NamespacedName

	// inFlight tracks BackupConfigs currently being reconciled to prevent
	// parallel reconciliation of the same resource
	inFlight map[string]struct{}
	// pending tracks BackupConfigs sitting in queue: a key is queued at most once,
	// so a pass that outlasts checkInterval neither duplicates nor drops keys
	pending    map[string]struct{}
	inFlightMu sync.Mutex

	// worker pool: workers running, wantWorkers the size set by the last pass
	poolMu      sync.Mutex
	workers     int
	wantWorkers int
	workerSeq   int
	workersWG   sync.WaitGroup

	// ctx is the root context for the controller, set during Start
	ctx    context.Context
	logger logr.Logger
}

// NewBackupConfigStatusController creates a new BackupConfigStatusController.
// storageCheckTimeout bounds a single `wal-g st check` call; <= 0 means
// walg.DefaultStorageCheckTimeout.
func NewBackupConfigStatusController(
	client client.Client,
	checkInterval time.Duration,
	storageCheckTimeout time.Duration,
) *BackupConfigStatusController {
	if storageCheckTimeout <= 0 {
		storageCheckTimeout = walg.DefaultStorageCheckTimeout
	}
	return &BackupConfigStatusController{
		client:                   client,
		checkInterval:            checkInterval,
		storageCheckTimeout:      storageCheckTimeout,
		storageCheckRetryBackoff: defaultStorageCheckRetryBackoff,
		queue:                    make(chan types.NamespacedName, statusReconcileQueueSize),
		inFlight:                 make(map[string]struct{}),
		pending:                  make(map[string]struct{}),
	}
}

// Start begins the status controller's periodic reconciliation loop and worker pool
func (c *BackupConfigStatusController) Start(ctx context.Context) error {
	c.logger = ctrl.Log.WithName("BackupConfigStatusController")
	c.logger.Info("Starting BackupConfig status controller",
		"checkInterval", c.checkInterval,
		"storageCheckTimeout", c.storageCheckTimeout,
		"storageCheckAttempts", storageCheckAttempts,
		"minWorkers", minStatusReconcileWorkers,
		"maxWorkers", maxStatusReconcileWorkers,
	)
	ctx = logr.NewContext(ctx, c.logger)
	c.ctx = ctx

	c.resizeWorkers(ctx, minStatusReconcileWorkers)

	// First pass at once: with a 10-minute interval a new leader would otherwise
	// leave new BackupConfigs without status for a whole interval.
	c.enqueueAllStatuses(ctx)

	// Periodic enqueue loop
	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping BackupConfig status controller, waiting for workers to finish")
			c.workersWG.Wait()
			c.logger.Info("All workers stopped")
			return nil
		case <-ticker.C:
			c.enqueueAllStatuses(ctx)
		}
	}
}

// NeedLeaderElection returns true if the Runnable needs to be run in the leader election mode.
// e.g. controllers need to be run in leader election mode, while webhook server doesn't.
func (c *BackupConfigStatusController) NeedLeaderElection() bool {
	return true
}

// EnqueueStatusUpdate enqueues a status reconciliation for a specific BackupConfig.
// This can be called externally (e.g. after backup creation or BackupConfig changes)
// to trigger an immediate status update. A key already in the queue is not queued
// again. Non-blocking: if the queue is full, the request is dropped (the periodic
// reconciliation will pick it up later). Returns whether the key was queued.
func (c *BackupConfigStatusController) EnqueueStatusUpdate(key types.NamespacedName) bool {
	queueKey := key.String()
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()
	if _, queued := c.pending[queueKey]; queued {
		c.logger.V(1).Info("Status update already queued", "backupConfig", key)
		return false
	}
	select {
	case c.queue <- key:
		c.pending[queueKey] = struct{}{}
		c.logger.V(1).Info("Enqueued status update", "backupConfig", key)
		return true
	default:
		c.logger.V(1).Info("Status reconciliation queue is full, dropping request", "backupConfig", key)
		return false
	}
}

// statusWorkersFor sizes the worker pool so that a pass over n BackupConfigs fits
// in 3/4 of interval, within [minStatusReconcileWorkers, maxStatusReconcileWorkers].
func statusWorkersFor(n int, interval time.Duration) int {
	budget := interval * 3 / 4
	workers := minStatusReconcileWorkers
	if budget > 0 {
		workers = max(workers, int((time.Duration(n)*statusReconcileEstimate+budget-1)/budget))
	}
	return min(workers, maxStatusReconcileWorkers)
}

// resizeWorkers starts workers up to want; surplus workers exit after their
// current reconcile (retireWorker).
func (c *BackupConfigStatusController) resizeWorkers(ctx context.Context, want int) {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	c.wantWorkers = want
	for c.workers < want {
		c.workers++
		c.workerSeq++
		c.workersWG.Add(1)
		go func(workerID int) {
			defer c.workersWG.Done()
			c.worker(ctx, workerID)
		}(c.workerSeq)
	}
}

// retireWorker reports whether the calling worker is surplus and must exit.
func (c *BackupConfigStatusController) retireWorker() bool {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	if c.workers > c.wantWorkers {
		c.workers--
		return true
	}
	return false
}

// enqueueAllStatuses lists all BackupConfig resources and enqueues status reconciliation for each
func (c *BackupConfigStatusController) enqueueAllStatuses(ctx context.Context) {
	backupConfigList := &v1beta1.BackupConfigList{}
	if err := c.client.List(ctx, backupConfigList); err != nil {
		c.logger.Error(err, "Failed to list BackupConfig resources")
		return
	}

	workers := statusWorkersFor(len(backupConfigList.Items), c.checkInterval)
	c.resizeWorkers(ctx, workers)

	// Keys still queued from the previous pass stay where they are; only the others
	// are queued, so a pass longer than checkInterval delays keys but never drops them.
	queued := 0
	for i := range backupConfigList.Items {
		backupConfig := &backupConfigList.Items[i]
		if c.EnqueueStatusUpdate(types.NamespacedName{
			Namespace: backupConfig.Namespace,
			Name:      backupConfig.Name,
		}) {
			queued++
		}
	}
	c.logger.Info("Running periodic BackupConfig status reconciliation",
		"backupConfigs", len(backupConfigList.Items),
		"queued", queued,
		"alreadyQueued", len(backupConfigList.Items)-queued,
		"workers", workers,
	)
}

// worker is a single worker goroutine that processes reconciliation requests from the shared queue
func (c *BackupConfigStatusController) worker(ctx context.Context, workerID int) {
	logger := c.logger.WithValues("worker", workerID)
	logger.V(1).Info("Worker started")

	for {
		if c.retireWorker() {
			logger.V(1).Info("Worker retired, pool shrunk")
			return
		}
		select {
		case <-ctx.Done():
			logger.V(1).Info("Worker stopping")
			return
		case key := <-c.queue:
			queueKey := key.String()

			// Check if this BackupConfig is already being reconciled
			if !c.tryAcquire(queueKey) {
				logger.V(1).Info("BackupConfig already being reconciled, skipping", "backupConfig", queueKey)
				continue
			}

			func() {
				defer c.release(queueKey)

				reconcileCtx, cancel := context.WithTimeout(ctx, statusReconcileTimeout)
				defer cancel()

				bcLogger := logger.WithValues("backupConfig", key.Name, "namespace", key.Namespace)
				if err := c.reconcileStatusByKey(reconcileCtx, key, bcLogger); err != nil {
					bcLogger.Error(err, "Failed to reconcile BackupConfig status")
				}
			}()
		}
	}
}

// tryAcquire takes a BackupConfig off the pending set and attempts to mark it
// as in-flight. Returns true if the BackupConfig was not already in-flight and is now marked.
func (c *BackupConfigStatusController) tryAcquire(queueKey string) bool {
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()

	delete(c.pending, queueKey)
	if _, exists := c.inFlight[queueKey]; exists {
		return false
	}
	c.inFlight[queueKey] = struct{}{}
	return true
}

// release removes a BackupConfig from the in-flight set
func (c *BackupConfigStatusController) release(queueKey string) {
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()
	delete(c.inFlight, queueKey)
}

// reconcileStatusByKey reconciles the status of a BackupConfig identified by NamespacedName
func (c *BackupConfigStatusController) reconcileStatusByKey(
	ctx context.Context,
	key types.NamespacedName,
	logger logr.Logger,
) error {
	backupConfig := &v1beta1.BackupConfig{}
	if err := c.client.Get(ctx, key, backupConfig); err != nil {
		return client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling BackupConfig status")

	// Prefetch secrets for wal-g client
	backupConfigWithSecrets, err := backupConfig.PrefetchSecretsData(ctx, c.client)
	if err != nil {
		logger.Error(err, "Failed to prefetch secrets data, marking credentials as invalid")
		setCondition(backupConfig, v1beta1.ConditionTypeCredentialsValid, metav1.ConditionFalse,
			"SecretsFetchFailed", fmt.Sprintf("Failed to prefetch secrets: %v", err))
		backupConfig.Status.Phase = v1beta1.BackupConfigPhaseFailed
		return c.client.Status().Update(ctx, backupConfig)
	}

	// Storage checks run in their own folder, outside every archive (walg.StorageCheckFolder)
	walgClient := walg.NewStorageCheckClient(backupConfigWithSecrets)

	// Mark credentials as valid since we successfully fetched secrets
	setCondition(backupConfig, v1beta1.ConditionTypeCredentialsValid, metav1.ConditionTrue,
		"CredentialsResolved", "All referenced secrets and configmaps resolved successfully")

	// Check storage readability
	readable := c.checkStorageReadable(ctx, backupConfig, walgClient, logger)

	// Check storage writability
	c.checkStorageWritable(ctx, backupConfig, walgClient, logger)

	// Timestamps and the deletion conditions (BackupDeletionRetainedByLock,
	// BackupDeletionFailing) come from the Backup resources alone: they must stay current
	// while the storage is unreachable, which is when a deletion fails.
	c.reconcileBackupTimestamps(ctx, backupConfig, logger)

	if readable {
		// Check WAL integrity and reconcile WAL-related status fields via wal-g wal-show
		c.reconcileWALStatus(ctx, backupConfig, backupConfigWithSecrets, logger)

		// Reconcile consumed storage from wal-g backup-list across all known PG versions
		c.reconcileConsumedStorage(ctx, backupConfig, backupConfigWithSecrets, logger)
	}

	// Determine overall phase from conditions
	backupConfig.Status.Phase = determinePhase(backupConfig)

	// Update the status
	if err := c.client.Status().Update(ctx, backupConfig); err != nil {
		return fmt.Errorf("failed to update BackupConfig status: %w", err)
	}

	logger.Info("Successfully reconciled BackupConfig status", "phase", backupConfig.Status.Phase)
	return nil
}

// checkStorageReadable checks if the storage is accessible for reading, returns check result as bool
func (c *BackupConfigStatusController) checkStorageReadable(
	ctx context.Context,
	backupConfig *v1beta1.BackupConfig,
	checker storageChecker,
	logger logr.Logger,
) bool {
	err := c.runStorageCheck(ctx, "read", checker.StorageCheckReadable, logger)
	if err != nil {
		logger.Error(err, "Storage read check failed", "attempts", storageCheckAttempts)
		setCondition(backupConfig, v1beta1.ConditionTypeStorageReadable, metav1.ConditionFalse,
			"StorageReadCheckFailed", err.Error())
	} else {
		setCondition(backupConfig, v1beta1.ConditionTypeStorageReadable, metav1.ConditionTrue,
			"StorageReadCheckPassed", "Storage is accessible for reading")
	}
	return err == nil
}

// checkStorageWritable checks if the storage is accessible for writing, returns check result as bool
func (c *BackupConfigStatusController) checkStorageWritable(
	ctx context.Context,
	backupConfig *v1beta1.BackupConfig,
	checker storageChecker,
	logger logr.Logger,
) bool {
	err := c.runStorageCheck(ctx, "write", checker.StorageCheckWritable, logger)
	if err != nil {
		logger.Error(err, "Storage write check failed", "attempts", storageCheckAttempts)
		setCondition(backupConfig, v1beta1.ConditionTypeStorageWritable, metav1.ConditionFalse,
			"StorageWriteCheckFailed", err.Error())
	} else {
		setCondition(backupConfig, v1beta1.ConditionTypeStorageWritable, metav1.ConditionTrue,
			"StorageWriteCheckPassed", "Storage is accessible for writing")
	}
	return err == nil
}

// runStorageCheck runs a storage check up to storageCheckAttempts times, each
// call bounded by storageCheckTimeout, pausing storageCheckRetryBackoff between
// attempts. All of it is bounded by ctx (the reconcile context). A failure that
// is retried is logged at Info; the caller logs the final failure at Error.
// It returns nil on the first success, else the last attempt's error.
func (c *BackupConfigStatusController) runStorageCheck(
	ctx context.Context,
	mode string,
	check func(context.Context, time.Duration) (*cmd.RunResult, error),
	logger logr.Logger,
) error {
	var err error
	for attempt := 1; attempt <= storageCheckAttempts; attempt++ {
		if _, err = check(ctx, c.storageCheckTimeout); err == nil {
			if attempt > 1 {
				logger.Info(fmt.Sprintf("Storage %s check passed on attempt %d/%d", mode, attempt, storageCheckAttempts))
			}
			return nil
		}
		if attempt == storageCheckAttempts || ctx.Err() != nil {
			break
		}
		logger.Info(fmt.Sprintf("Storage %s check attempt %d/%d failed, retrying", mode, attempt, storageCheckAttempts),
			"error", err.Error(), "retryIn", c.storageCheckRetryBackoff)
		if !sleepCtx(ctx, c.storageCheckRetryBackoff) {
			break
		}
	}
	return err
}

// sleepCtx waits for d or until ctx is done; it returns false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// reconcileWALStatus checks WAL integrity and evaluates recoverability points
// by running `wal-g wal-show --detailed-json` across all known PG major versions.
//
// Missing segments below the start segment of the earliest backup in storage
// are not counted: no backup can need them for a restore. They show up after a
// switch of the WAL archive (e.g. from barman to wal-g), where the new prefix
// starts with a stray segment pushed before the first backup and the gap after
// it holds segments the old archiver uploaded elsewhere. Gaps at or above the
// earliest backup's start still fail the check, and so does any gap when the
// prefix has no backup at all.
func (c *BackupConfigStatusController) reconcileWALStatus(
	ctx context.Context,
	backupConfig *v1beta1.BackupConfig,
	backupConfigWithSecrets *v1beta1.BackupConfigWithSecrets,
	logger logr.Logger,
) {
	total := walIntegrityResult{AllTimelinesOK: true}
	var earliestBackupTime *time.Time

	// Iterate over all known PG major versions to collect WAL info
	for pgVersion := 11; pgVersion <= 19; pgVersion++ {
		walgClient := walg.NewClientFromBackupConfig(backupConfigWithSecrets, pgVersion)
		timelines, err := walgClient.WALShow(ctx)
		if err != nil {
			// Not all PG versions will have WAL data, so we just log and continue
			logger.V(1).Info("Failed to get WAL info for PG version, skipping",
				"pgVersion", pgVersion, "error", err)
			continue
		}

		// The backup list is only needed to excuse missing segments.
		var backups []walg.BackupMetadata
		if lo.SomeBy(timelines, func(t walg.WALTimelineInfo) bool { return t.HasMissingSegments() }) {
			backups, err = walgClient.GetBackupsList(ctx)
			if err != nil {
				logger.Info("Failed to list backups, counting every missing WAL segment",
					"pgVersion", pgVersion, "error", err.Error())
				backups = nil
			}
		}

		result := evaluateWALIntegrity(timelines, backups)
		total.merge(result)
		if len(result.IgnoredMissingSegments) > 0 {
			logger.Info("Ignoring missing WAL segments below the earliest backup",
				"pgVersion", pgVersion, "earliestBackup", result.EarliestBackup,
				"ignored", len(result.IgnoredMissingSegments),
				"first", result.IgnoredMissingSegments[0],
				"last", result.IgnoredMissingSegments[len(result.IgnoredMissingSegments)-1])
		}

		// Find the earliest backup start time across all timelines for first recoverability point
		for i := range timelines {
			timeline := &timelines[i]
			for j := range timeline.Backups {
				startTime, err := timeline.Backups[j].StartTime()
				if err != nil {
					logger.V(1).Info("Failed to parse backup start time", "error", err)
					continue
				}
				if earliestBackupTime == nil || startTime.Before(*earliestBackupTime) {
					t := startTime
					earliestBackupTime = &t
				}
			}
		}
	}

	// Update first recoverability point from wal-g data if available
	if earliestBackupTime != nil {
		backupConfig.Status.FirstRecoverabilityPoint = &metav1.Time{Time: *earliestBackupTime}
	}

	// Set WAL integrity condition
	if total.AllTimelinesOK && !total.HasMissingSegments {
		message := "All WAL timelines are OK with no missing segments"
		if n := len(total.IgnoredMissingSegments); n > 0 {
			message += fmt.Sprintf(" (%d missing segments below the earliest backup ignored)", n)
		}
		setCondition(backupConfig, v1beta1.ConditionTypeWALIntegrityCheck, metav1.ConditionTrue,
			"WALIntegrityCheckPassed", message)
	} else {
		reason := "WALIntegrityCheckFailed"
		message := "WAL integrity issues detected:"
		if !total.AllTimelinesOK {
			message += " some timelines have non-OK status"
		}
		if total.HasMissingSegments {
			if !total.AllTimelinesOK {
				message += ","
			}
			message += " missing WAL segments detected"
		}
		setCondition(backupConfig, v1beta1.ConditionTypeWALIntegrityCheck, metav1.ConditionFalse,
			reason, message)
	}
}

// walIntegrityResult is the WAL integrity verdict for one or more wal-g prefixes.
type walIntegrityResult struct {
	AllTimelinesOK     bool
	HasMissingSegments bool
	// IgnoredMissingSegments are missing segments below the earliest backup.
	IgnoredMissingSegments []string
	// EarliestBackup is the backup whose start segment set the cut-off.
	EarliestBackup string
}

func (r *walIntegrityResult) merge(o walIntegrityResult) {
	r.AllTimelinesOK = r.AllTimelinesOK && o.AllTimelinesOK
	r.HasMissingSegments = r.HasMissingSegments || o.HasMissingSegments
	r.IgnoredMissingSegments = append(r.IgnoredMissingSegments, o.IgnoredMissingSegments...)
}

// evaluateWALIntegrity judges `wal-g wal-show --detailed-json` timelines.
// Missing segments whose position (log id, segment; the timeline is ignored)
// is below the start segment of the earliest backup in backups are ignored,
// and a timeline whose only problem was such segments counts as OK. Without
// any backup every missing segment counts, as before.
func evaluateWALIntegrity(timelines []walg.WALTimelineInfo, backups []walg.BackupMetadata) walIntegrityResult {
	result := walIntegrityResult{AllTimelinesOK: true}

	cutoff, haveCutoff := uint64(0), false
	for i := range backups {
		pos, ok := walSegmentPosition(backups[i].WalFileName)
		if !ok {
			continue
		}
		if !haveCutoff || pos < cutoff {
			cutoff, haveCutoff = pos, true
			result.EarliestBackup = backups[i].BackupName
		}
	}

	for i := range timelines {
		timeline := &timelines[i]
		counted := 0
		for _, segment := range timeline.MissingSegments {
			if pos, ok := walSegmentPosition(segment); haveCutoff && ok && pos < cutoff {
				result.IgnoredMissingSegments = append(result.IgnoredMissingSegments, segment)
				continue
			}
			counted++
		}
		if counted > 0 {
			result.HasMissingSegments = true
		}

		timelineOK := timeline.IsOK() ||
			(timeline.Status == walg.TimelineStatusLostSegments && timeline.HasMissingSegments() && counted == 0)
		if !timelineOK {
			result.AllTimelinesOK = false
		}
	}
	sort.Strings(result.IgnoredMissingSegments)
	return result
}

// walSegmentPosition returns the position of a WAL segment name in the WAL
// stream (log id and segment, the timeline left out), comparable across
// timelines of one cluster.
func walSegmentPosition(name string) (uint64, bool) {
	if len(name) != 24 {
		return 0, false
	}
	logID, err := strconv.ParseUint(name[8:16], 16, 32)
	if err != nil {
		return 0, false
	}
	seg, err := strconv.ParseUint(name[16:24], 16, 32)
	if err != nil {
		return 0, false
	}
	if _, err := strconv.ParseUint(name[0:8], 16, 32); err != nil {
		return 0, false
	}
	return logID<<32 | seg, true
}

// reconcileBackupTimestamps reconciles timestamp-related status fields from CNPG Backup resources
func (c *BackupConfigStatusController) reconcileBackupTimestamps(
	ctx context.Context,
	backupConfig *v1beta1.BackupConfig,
	logger logr.Logger,
) {
	// List all Backup resources in the same namespace
	backupList := &cnpgv1.BackupList{}
	if err := c.client.List(ctx, backupList, client.InNamespace(backupConfig.Namespace)); err != nil {
		logger.Error(err, "Failed to list Backup resources")
		return
	}

	// Filter backups owned by this BackupConfig
	var relevantBackups []cnpgv1.Backup
	for i := range backupList.Items {
		backup := &backupList.Items[i]
		if lo.ContainsBy(backup.OwnerReferences, func(ref metav1.OwnerReference) bool {
			return ref.Kind == "BackupConfig" && ref.Name == backupConfig.Name
		}) {
			relevantBackups = append(relevantBackups, *backup)
		}
	}

	reconcileRetainedDeletions(backupConfig, relevantBackups,
		c.backupsOfGoneConfigs(ctx, backupConfig.Namespace, backupList.Items, logger))

	if len(relevantBackups) == 0 {
		return
	}

	// Sort backups by start time (oldest first)
	sort.Slice(relevantBackups, func(i, j int) bool {
		timeI := getBackupTime(&relevantBackups[i])
		timeJ := getBackupTime(&relevantBackups[j])
		return timeI.Before(timeJ)
	})

	// Calculate first recoverability point (earliest successful backup start time)
	successfulBackups := lo.Filter(relevantBackups, func(b cnpgv1.Backup, _ int) bool {
		return b.Status.Phase == cnpgv1.BackupPhaseCompleted
	})
	if len(successfulBackups) > 0 {
		firstTime := getBackupTime(&successfulBackups[0])
		backupConfig.Status.FirstRecoverabilityPoint = &metav1.Time{Time: firstTime}

		lastTime := getBackupTime(&successfulBackups[len(successfulBackups)-1])
		backupConfig.Status.LastSuccessfulBackup = &metav1.Time{Time: lastTime}
	}

	// Find last failed backup
	failedBackups := lo.Filter(relevantBackups, func(b cnpgv1.Backup, _ int) bool {
		return b.Status.Phase == cnpgv1.BackupPhaseFailed
	})
	if len(failedBackups) > 0 {
		lastFailedTime := getBackupTime(&failedBackups[len(failedBackups)-1])
		backupConfig.Status.LastFailedBackup = &metav1.Time{Time: lastFailedTime}
	}
}

// reconcileConsumedStorage calculates total consumed storage space by fetching backup metadata
// from wal-g backup-list (for backup CompressedSize) and wal-g st ls (for WAL file sizes)
// across all known PG major versions
func (c *BackupConfigStatusController) reconcileConsumedStorage(
	ctx context.Context,
	backupConfig *v1beta1.BackupConfig,
	backupConfigWithSecrets *v1beta1.BackupConfigWithSecrets,
	logger logr.Logger,
) {
	var backupBytes int64
	var walBytes int64

	// Iterate over all known PG major versions to collect backup and WAL sizes
	// (same approach as in deleteBackupConfig)
	for pgVersion := 11; pgVersion <= 19; pgVersion++ {
		walgClient := walg.NewClientFromBackupConfig(backupConfigWithSecrets, pgVersion)

		// Sum backup compressed sizes from wal-g backup-list
		backupsList, err := walgClient.GetBackupsList(ctx)
		if err != nil {
			// Not all PG versions will have backups, so we just log and continue
			logger.V(1).Info("Failed to get backups list for PG version, skipping",
				"pgVersion", pgVersion, "error", err)
			continue
		}

		for i := range backupsList {
			backupBytes += int64(backupsList[i].CompressedSize)
		}

		// Sum WAL file sizes from wal-g st ls wal_005/
		// wal_005/ is the wal-g internal directory for WAL segments
		walSize, err := walgClient.StorageLsTotalSize(ctx, "wal_005/")
		if err != nil {
			logger.V(1).Info("Failed to get WAL directory size for PG version, skipping",
				"pgVersion", pgVersion, "error", err)
		} else {
			walBytes += walSize
		}
	}

	totalBytes := backupBytes + walBytes
	if totalBytes > 0 {
		storageInfo := &v1beta1.ConsumedStorageInfo{
			TotalBytes: &totalBytes,
			Total:      formatBytesHumanReadable(totalBytes),
		}
		if backupBytes > 0 {
			storageInfo.BackupsBytes = &backupBytes
			storageInfo.Backups = formatBytesHumanReadable(backupBytes)
		}
		if walBytes > 0 {
			storageInfo.WALBytes = &walBytes
			storageInfo.WAL = formatBytesHumanReadable(walBytes)
		}
		backupConfig.Status.ConsumedStorage = storageInfo
	}
}

// formatBytesHumanReadable converts bytes to a human-readable string using binary (IEC) units
func formatBytesHumanReadable(bytes int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
		tib = 1024 * gib
	)

	switch {
	case bytes >= tib:
		return fmt.Sprintf("%.2f TiB", float64(bytes)/float64(tib))
	case bytes >= gib:
		return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(gib))
	case bytes >= mib:
		return fmt.Sprintf("%.2f MiB", float64(bytes)/float64(mib))
	case bytes >= kib:
		return fmt.Sprintf("%.2f KiB", float64(bytes)/float64(kib))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// getBackupTime returns the backup start time, falling back to creation timestamp
func getBackupTime(backup *cnpgv1.Backup) time.Time {
	if backup.Status.StartedAt != nil {
		return backup.Status.StartedAt.Time
	}
	return backup.CreationTimestamp.Time
}

// setCondition sets or updates a condition on the BackupConfig status
func setCondition(backupConfig *v1beta1.BackupConfig, conditionType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	newCondition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: backupConfig.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}

	// Find existing condition and update it
	for i, existing := range backupConfig.Status.Conditions {
		if existing.Type == conditionType {
			// Only update LastTransitionTime if the status actually changed
			if existing.Status == status {
				newCondition.LastTransitionTime = existing.LastTransitionTime
			}
			backupConfig.Status.Conditions[i] = newCondition
			return
		}
	}

	// Condition not found, append it
	backupConfig.Status.Conditions = append(backupConfig.Status.Conditions, newCondition)
}

// determinePhase determines the overall phase based on conditions
func determinePhase(backupConfig *v1beta1.BackupConfig) v1beta1.BackupConfigPhase {
	if len(backupConfig.Status.Conditions) == 0 {
		return v1beta1.BackupConfigPhaseUnknown
	}

	hasFalse := false
	allTrue := true

	for _, condition := range backupConfig.Status.Conditions {
		if condition.Status == metav1.ConditionFalse {
			hasFalse = true
			allTrue = false
		} else if condition.Status != metav1.ConditionTrue {
			allTrue = false
		}
	}

	// If credentials are invalid or storage is not accessible for both read and write, it's Failed
	credentialsValid := getConditionStatus(backupConfig, v1beta1.ConditionTypeCredentialsValid)
	storageReadable := getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageReadable)
	storageWritable := getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageWritable)

	if credentialsValid == metav1.ConditionFalse {
		return v1beta1.BackupConfigPhaseFailed
	}

	if storageReadable == metav1.ConditionFalse && storageWritable == metav1.ConditionFalse {
		return v1beta1.BackupConfigPhaseFailed
	}

	if allTrue {
		return v1beta1.BackupConfigPhaseHealthy
	}

	if hasFalse {
		return v1beta1.BackupConfigPhaseDegraded
	}

	return v1beta1.BackupConfigPhaseUnknown
}

// getConditionStatus returns the status of a specific condition, or empty string if not found
func getConditionStatus(backupConfig *v1beta1.BackupConfig, conditionType string) metav1.ConditionStatus {
	for _, condition := range backupConfig.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status
		}
	}
	return ""
}

// reconcileRetainedDeletions sets ConditionTypeBackupDeletionRetainedByLock while any
// Backup of the BackupConfig is being deleted and its deletion was refused by a bucket
// lock (BackupRetainedByLockAnnotationName), and ConditionTypeBackupDeletionFailing while
// any deletion fails otherwise (BackupDeletionFailingAnnotationName). Each condition is
// removed when no Backup is in that state.
//
// orphans are the namespace's Backups whose own BackupConfig is gone (backupsOfGoneConfigs):
// their deletion cannot run and nothing else reports it, so every BackupConfig of the
// namespace carries them in BackupDeletionFailing with reason BackupConfigNotFound.
func reconcileRetainedDeletions(backupConfig *v1beta1.BackupConfig, backups, orphans []cnpgv1.Backup) {
	reconcileDeletionCondition(backupConfig, backups, v1beta1.BackupRetainedByLockAnnotationName,
		v1beta1.ConditionTypeBackupDeletionRetainedByLock, "RetainedByBucketLock",
		"Backup(s) kept in storage by the bucket lock, deletion retried")
	orphaned := lo.SliceToMap(orphans, func(b cnpgv1.Backup) (string, bool) { return b.Name, true })
	backups = lo.Reject(backups, func(b cnpgv1.Backup, _ int) bool { return orphaned[b.Name] })
	reconcileDeletionCondition(backupConfig, backups, v1beta1.BackupDeletionFailingAnnotationName,
		v1beta1.ConditionTypeBackupDeletionFailing, "DeletionFailing",
		"Backup(s) could not be deleted from storage (unreachable or refused), finalizer kept, deletion retried")
	if names := deletingMarked(orphans, v1beta1.BackupDeletionFailingAnnotationName); len(names) > 0 {
		message := fmt.Sprintf("%d Backup(s) whose BackupConfig is gone, still in storage, finalizer kept "+
			"until the Backup is annotated %s: %s",
			len(names), v1beta1.SkipStorageCleanupAnnotationName, strings.Join(names, ", "))
		if cond := meta.FindStatusCondition(backupConfig.Status.Conditions,
			v1beta1.ConditionTypeBackupDeletionFailing); cond != nil {
			message = cond.Message + "; " + message
		}
		setCondition(backupConfig, v1beta1.ConditionTypeBackupDeletionFailing, metav1.ConditionTrue,
			"BackupConfigNotFound", message)
	}
}

// backupsOfGoneConfigs returns the Backups owned by a BackupConfig that no longer exists.
func (c *BackupConfigStatusController) backupsOfGoneConfigs(ctx context.Context, namespace string,
	backups []cnpgv1.Backup, logger logr.Logger) []cnpgv1.Backup {
	configs := &v1beta1.BackupConfigList{}
	if err := c.client.List(ctx, configs, client.InNamespace(namespace)); err != nil {
		logger.Error(err, "Failed to list BackupConfigs")
		return nil
	}
	uids := lo.SliceToMap(configs.Items, func(bc v1beta1.BackupConfig) (string, types.UID) { return bc.Name, bc.UID })
	return lo.Filter(backups, func(b cnpgv1.Backup, _ int) bool {
		ref, owned := lo.Find(b.OwnerReferences, func(ref metav1.OwnerReference) bool { return ref.Kind == "BackupConfig" })
		return owned && uids[ref.Name] != ref.UID // gone, or re-created as another object
	})
}

func reconcileDeletionCondition(backupConfig *v1beta1.BackupConfig, backups []cnpgv1.Backup,
	annotation, conditionType, reason, what string) {
	names := deletingMarked(backups, annotation)
	if len(names) == 0 {
		meta.RemoveStatusCondition(&backupConfig.Status.Conditions, conditionType)
		return
	}
	setCondition(backupConfig, conditionType, metav1.ConditionTrue, reason,
		fmt.Sprintf("%d %s: %s", len(names), what, strings.Join(names, ", ")))
}

// deletingMarked returns the sorted names of the Backups being deleted that carry annotation.
func deletingMarked(backups []cnpgv1.Backup, annotation string) []string {
	names := make([]string, 0)
	for i := range backups {
		_, marked := backups[i].Annotations[annotation]
		if marked && !backups[i].DeletionTimestamp.IsZero() {
			names = append(names, backups[i].Name)
		}
	}
	sort.Strings(names)
	return names
}
