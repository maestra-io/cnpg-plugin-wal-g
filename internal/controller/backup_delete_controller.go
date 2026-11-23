package controller

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
	"golang.org/x/sync/semaphore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceType defines the type of resource being deleted
type ResourceType string

const (
	// ResourceTypeBackup represents a Backup resource
	ResourceTypeBackup ResourceType = "Backup"
	// ResourceTypeBackupConfig represents a BackupConfig resource
	ResourceTypeBackupConfig ResourceType = "BackupConfig"

	// Timeout for resource cleanup, if exceeded - deletion process will be aborted and retried
	// Needed to prevent stuck on deletion when incorrect BackupConfig / etc.
	DeletionRequestTimeout time.Duration = 5 * time.Minute
)

// DeletionRequest represents a request to delete a resource
type DeletionRequest struct {
	// Type of resource to delete
	Type ResourceType
	// NamespacedName of the resource
	Key types.NamespacedName
}

// BackupDeletionController handles Backup and BackupConfig deletion requests and processes them
// sequentially per BackupConfig to prevent operator from overloading
// and wal-g collisions at deleting backups at the same storage
type BackupDeletionController struct {
	client.Client

	// Deletion queues, each BackupConfig should have its own queue to make its deletions serialized
	// Key - BackupConfig.Namespace + "/" + BackupConfig.Name
	// Values - DeletionRequest objects
	deletionQueues    map[string](chan DeletionRequest)
	queueMX           sync.Mutex
	queueCtxCancels   map[string]context.CancelFunc // Cancel functions for queue processing goroutines contexts
	deletionSemaphore *semaphore.Weighted
	// APIReader reads the R2BucketLocks (uncached); nil reads them through the Client.
	APIReader client.Reader
	// logger is the base logger of the queue goroutines. Not the logger of the request
	// that starts a queue: that one carries its reconcile's Backup name and reconcileID,
	// which then labeled every later deletion of the queue.
	logger logr.Logger
}

// NewBackupDeletionController creates a new BackupDeletionController
func NewBackupDeletionController(client client.Client) *BackupDeletionController {
	deletionSemaphore := semaphore.NewWeighted(int64(10))
	return &BackupDeletionController{
		Client:            client,
		deletionQueues:    make(map[string]chan DeletionRequest),
		queueCtxCancels:   map[string]context.CancelFunc{},
		deletionSemaphore: deletionSemaphore,
		logger:            ctrl.Log.WithName("BackupDeletionController"),
	}
}

// Start begins the backup deletion controller's periodic check
func (b *BackupDeletionController) Start(ctx context.Context) error {
	logger := ctrl.Log.WithName("BackupDeletionController")
	logger.Info("Starting backup deletion controller")
	ctx = logr.NewContext(ctx, logger)

	// Wait for context cancellation
	for range ctx.Done() {
		// This block executes only once when context is done
		logger.Info("Stopping backup deletion controller")
		break
	}

	// Cleaning up all queues on exit
	for queueKey, cancelFunc := range b.queueCtxCancels {
		func() { // Wrap in separate function to use "defer mutex.Unlock()"
			b.queueMX.Lock()
			defer b.queueMX.Unlock()

			// Cancel the context for this queue's goroutine
			cancelFunc()
			// Remove queue
			delete(b.deletionQueues, queueKey)
		}()
	}

	return nil
}

// NeedLeaderElection returns true if the Runnable needs to be run in the leader election mode.
// e.g. controllers need to be run in leader election mode, while webhook server doesn't.
func (b *BackupDeletionController) NeedLeaderElection() bool {
	return true
}

// EnqueueBackupDeletion adds a Backup to the deletion queue for its BackupConfig
// If the queue doesn't exist, it creates a new one and starts a goroutine to process it
func (b *BackupDeletionController) EnqueueBackupDeletion(ctx context.Context, backup *cnpgv1.Backup) error {
	// Get BackupConfig with secrets for the backup
	backupConfig, err := v1beta1.GetBackupConfigForBackup(ctx, b.Client, backup)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to get BackupConfig for Backup: %w", err)
	}
	if err != nil || !ownedBy(backup, backupConfig) {
		return b.backupConfigGone(ctx, backup)
	}

	backupKey := client.ObjectKeyFromObject(backup)

	// Create queue key from BackupConfig namespace and name
	queueKey := backupConfig.Namespace + "/" + backupConfig.Name

	// Create deletion request
	request := DeletionRequest{
		Type: ResourceTypeBackup,
		Key:  backupKey,
	}

	// Enqueue the deletion request
	return b.enqueueDeletionRequest(ctx, queueKey, request)
}

// EnqueueBackupConfigDeletion adds a BackupConfig to the deletion queue
// If the queue doesn't exist, it creates a new one and starts a goroutine to process it
func (b *BackupDeletionController) EnqueueBackupConfigDeletion(ctx context.Context, backupConfig *v1beta1.BackupConfig) error {
	backupConfigKey := client.ObjectKeyFromObject(backupConfig)

	// Create queue key from BackupConfig namespace and name
	queueKey := backupConfig.Namespace + "/" + backupConfig.Name

	// Create deletion request
	request := DeletionRequest{
		Type: ResourceTypeBackupConfig,
		Key:  backupConfigKey,
	}

	// Enqueue the deletion request
	return b.enqueueDeletionRequest(ctx, queueKey, request)
}

// enqueueDeletionRequest adds a deletion request to the appropriate queue
// If the queue doesn't exist, it creates a new one and starts a goroutine to process it
func (b *BackupDeletionController) enqueueDeletionRequest(
	ctx context.Context,
	queueKey string,
	request DeletionRequest,
) error {
	logger := logr.FromContextOrDiscard(ctx).WithName("BackupDeletionController")
	// Check if queue exists, if not create it
	b.queueMX.Lock()
	defer b.queueMX.Unlock()
	_, exists := b.deletionQueues[queueKey]

	if !exists {
		// Need to create a new queue
		// Create a new buffered queue with size 128
		b.deletionQueues[queueKey] = make(chan DeletionRequest, 128)

		// Create a derived context for this queue's processing goroutine
		queueCtx, cancelFunc := context.WithCancel(
			logr.NewContext(context.Background(), b.logger.WithValues("queue", queueKey)))

		b.queueCtxCancels[queueKey] = cancelFunc

		// Start a goroutine to process this queue
		go b.processDeletionQueue(queueCtx, queueKey)
	}

	// Try to add the request to the queue
	select {
	case b.deletionQueues[queueKey] <- request:
		logger.Info("Enqueued resource for deletion", "type", request.Type, "key", request.Key)
		return nil
	default:
		// Queue is full
		return fmt.Errorf("deletion queue for %s is full", queueKey)
	}
}

// processDeletionQueue processes deletion requests from the queue
func (b *BackupDeletionController) processDeletionQueue(
	ctx context.Context,
	queueKey string,
) {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Starting deletion queue processor")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping deletion queue processor")
			return
		case request := <-b.deletionQueues[queueKey]:
			requestLogger := logger.WithValues("type", request.Type, "backup", request.Key.String())
			requestLogger.Info("Processing deletion request")

			// cancel() at the end of the iteration: a defer in this endless loop never ran
			ctxWithTimeout, cancel := context.WithTimeout(logr.NewContext(ctx, requestLogger), DeletionRequestTimeout)

			var err error
			switch request.Type {
			case ResourceTypeBackup:
				err = b.deleteWALGBackup(ctxWithTimeout, request.Key)
			case ResourceTypeBackupConfig:
				err = b.deleteBackupConfig(ctxWithTimeout, request.Key)
			default:
				err = fmt.Errorf("unknown resource type: %s", request.Type)
			}

			cancel()

			switch {
			case errors.Is(err, walg.ErrRetainedByLock):
				// Not a failure: storage keeps the backup until it is older than the lock age.
				requestLogger.Info("Deletion refused by the bucket lock, the resource keeps its finalizer and is retried")
			case err != nil:
				requestLogger.Error(err, "Failed to process deletion request")
			}
		}
	}
}

// deleteWALGBackup deletes the physical backup via WAL-G (could be long running) and removes finalizer from Backup resource after successful deletion
func (b *BackupDeletionController) deleteWALGBackup(ctx context.Context, backupKey client.ObjectKey) error {
	logger := logr.FromContextOrDiscard(ctx)
	err := b.deletionSemaphore.Acquire(ctx, 1)
	if err != nil {
		return fmt.Errorf("acquire deletion semaphore for backup %v: %w", backupKey, err)
	}
	defer b.deletionSemaphore.Release(1)

	logger.Info("Started processing wal-g backup deletion")
	backup := &cnpgv1.Backup{}
	err = b.Get(ctx, backupKey, backup)
	if err != nil {
		// NotFound: an earlier request of a requeued deletion already finished it
		return client.IgnoreNotFound(err)
	}

	if backup.Labels == nil || backup.Annotations == nil {
		logger.Info(
			"Cannot delete backup because it has missing plugin labels or annotations",
			"backup", backup.Name, "dependents", backup.Annotations[v1beta1.BackupAllDependentsAnnotationName],
		)
		return nil
	}

	if backup.Annotations[v1beta1.BackupAllDependentsAnnotationName] != "" {
		// TODO: emit event for backup
		logger.Info(
			"Cannot delete backup because it still has dependent backups",
			"backup", backup.Name, "dependents", backup.Annotations[v1beta1.BackupAllDependentsAnnotationName],
		)
		return nil
	}

	if !containsString(backup.Finalizers, v1beta1.BackupFinalizerName) {
		return nil // Nothing to do if finalizer is not specified
	}

	if backup.Status.BackupID == "" {
		// if .Status.BackupID not present - physical backup does not exist and just removing finalizer
		return removeBackupFinalizer(ctx, backup, b.Client)
	}

	pgVersion, err := strconv.Atoi(backup.Labels[v1beta1.BackupPgVersionLabelName])
	if err != nil {
		return fmt.Errorf(
			"while wal-g backup removal: error cannot parse pgVersion from Backup label %s: '%s': %w",
			v1beta1.BackupPgVersionLabelName, backup.Labels[v1beta1.BackupPgVersionLabelName], err,
		)
	}

	backupConfig, err := v1beta1.GetBackupConfigForBackup(ctx, b, backup)
	if apierrors.IsNotFound(err) {
		return b.backupConfigGone(ctx, backup)
	}
	if err != nil {
		return fmt.Errorf("while getting backupconfig for backup %v: %w", backupKey, err)
	}
	if !ownedBy(backup, backupConfig) {
		return b.backupConfigGone(ctx, backup)
	}

	if skipStorageCleanup(backup) || skipStorageCleanup(backupConfig) {
		logger.Info("Skipping Backup data cleanup: requested by annotation, the backup stays in storage",
			"backup", backupKey, "annotation", v1beta1.SkipStorageCleanupAnnotationName)
		return removeBackupFinalizer(ctx, backup, b.Client)
	}

	// Storage that is unreachable or refuses us (StorageWritable False, 403, timeout, 5xx)
	// is not a reason to give up: the finalizer stays and the deletion is retried, otherwise
	// an outage (or whoever can fake one) turns the deletion into an orphaned backup.
	backupConfigWithSecrets, err := backupConfig.PrefetchSecretsData(ctx, b)
	if err != nil {
		return b.deletionFailed(ctx, backup,
			fmt.Errorf("while prefetching backup secrets data for backup %v: %w", backupKey, err))
	}

	// Check if deletion management is disabled for individual backups
	if backupConfigWithSecrets.Spec.Retention.IgnoreForBackupDeletion {
		logger.Info("Deletion management disabled for individual backups, skipping storage cleanup", "backupID", backup.Status.BackupID)
		return removeBackupFinalizer(ctx, backup, b.Client)
	}

	// Delete the backup using WAL-G
	logger.Info("Deleting backup from storage via WAL-G", "backupID", backup.Status.BackupID)
	walgClient := walg.NewClientFromBackupConfig(backupConfigWithSecrets, pgVersion)
	result, err := walgClient.DeleteBackup(ctx, backup.Status.BackupID)
	switch {
	case errors.Is(err, walg.ErrRetainedByLock):
		// Do not claim a deletion that did not happen: the finalizer stays, the Backup is
		// marked, and the BackupConfig status counts it (status controller).
		return b.retainedByLock(ctx, backup, walgClient, storagePrefix(backupConfigWithSecrets))
	case errors.Is(err, walg.ErrStorageGone):
		logger.Info("The bucket does not exist, nothing left to delete")
	case errors.Is(err, walg.ErrBackupNotInStorage):
		// Written before the BackupConfig moved to another prefix: the switch to libsodium
		// left the plain archive <bucket>/walg/<name> next to <bucket>/walg-enc/<name>, and
		// the bucket lifecycle rule walg-plain-expiry (32 days) deletes it there, a day after
		// retention (31 days) gets to it. Nothing for wal-g to delete in this storage.
		logger.Info("Backup is not in the storage of its BackupConfig (written before the BackupConfig "+
			"moved to another prefix, such as the plain archive the switch to walg-enc left behind, "+
			"which the bucket lifecycle expires): nothing to delete, releasing the Backup",
			"backupID", backup.Status.BackupID, "storage", storagePrefix(backupConfigWithSecrets))
	case err != nil:
		return b.deletionFailed(ctx, backup, fmt.Errorf(
			"while wal-g backup removal: error %w\nWAL-G stdout: %s\nWAL-G stderr: %s",
			err,
			string(result.Stdout()),
			string(result.Stderr()),
		))
	}

	return removeBackupFinalizer(ctx, backup, b.Client)
}

// lockExpiryMargin is added to the computed lock expiry: R2 evaluates the lock age itself.
const lockExpiryMargin = 10 * time.Minute

var r2BucketLockGVK = schema.GroupVersionKind{Group: "r2.cloudflare.maestra.io", Version: "v1alpha2", Kind: "R2BucketLock"}

// retainedByLock marks a Backup whose deletion the bucket lock refused and, when the lock
// age of the bucket is known, records when the lock releases the backup's newest object,
// so the deletion is retried then and not every hour until then. It returns
// walg.ErrRetainedByLock: the finalizer stays.
func (b *BackupDeletionController) retainedByLock(
	ctx context.Context, backup *cnpgv1.Backup, walgClient *walg.Client, prefix string,
) error {
	logger := logr.FromContextOrDiscard(ctx)
	if err := markBackup(ctx, b.Client, backup, v1beta1.BackupRetainedByLockAnnotationName,
		v1beta1.BackupDeletionFailingAnnotationName, time.Now()); err != nil {
		return fmt.Errorf("while marking backup %s retained by bucket lock: %w", backup.Name, err)
	}

	lockAge, err := b.bucketLockAge(ctx, backup.Namespace, prefix)
	if err != nil {
		logger.Info("Lock age of the bucket unknown, the deletion is retried with backoff", "reason", err.Error())
		return walg.ErrRetainedByLock
	}
	written, err := walgClient.BackupLastModified(ctx, backup.Status.BackupID)
	if err != nil {
		logger.Info("Last write of the backup unknown, the deletion is retried with backoff", "reason", err.Error())
		return walg.ErrRetainedByLock
	}
	until := written.Add(lockAge + lockExpiryMargin)
	if !until.After(time.Now()) {
		// the lock should be gone already: no schedule to trust, back off as before
		return walg.ErrRetainedByLock
	}
	patch := client.MergeFrom(backup.DeepCopy())
	backup.Annotations[v1beta1.BackupRetainedUntilAnnotationName] = until.UTC().Format(time.RFC3339)
	if err := b.Patch(ctx, backup, patch); err != nil {
		return fmt.Errorf("while recording the lock expiry of backup %s: %w", backup.Name, err)
	}
	logger.Info("Deletion is retried when the bucket lock releases the backup",
		"backupID", backup.Status.BackupID, "lastWrite", written, "lockAge", lockAge, "retryAt", until.UTC())
	return walg.ErrRetainedByLock
}

// bucketLockAge returns the longest age lock covering the storage at prefix, read from
// the R2BucketLocks of the namespace (Crossplane provider-cloudflare). Any rule covering
// it that is not an age rule makes the expiry unknowable from here.
func (b *BackupDeletionController) bucketLockAge(ctx context.Context, namespace, prefix string) (time.Duration, error) {
	location, err := url.Parse(prefix)
	if err != nil || location.Host == "" {
		return 0, fmt.Errorf("no bucket in prefix %q", prefix)
	}
	reader := b.APIReader
	if reader == nil {
		reader = b.Client
	}
	locks := &unstructured.UnstructuredList{}
	locks.SetGroupVersionKind(r2BucketLockGVK.GroupVersion().WithKind(r2BucketLockGVK.Kind + "List"))
	if err := reader.List(ctx, locks, client.InNamespace(namespace)); err != nil {
		return 0, fmt.Errorf("listing R2BucketLocks: %w", err)
	}
	// every object key of the storage starts with this
	storageKeys := strings.Trim(location.Path, "/") + "/"
	var days int64
	for _, lock := range locks.Items {
		if bucket, _, _ := unstructured.NestedString(lock.Object, "spec", "forProvider", "bucketName"); bucket != location.Host {
			continue
		}
		rules, _, _ := unstructured.NestedSlice(lock.Object, "spec", "forProvider", "rules")
		for _, r := range rules {
			rule, _ := r.(map[string]any)
			if enabled, _, _ := unstructured.NestedBool(rule, "enabled"); !enabled {
				continue
			}
			// a rule covers the keys under its prefix (all keys without one); one covering
			// only a part of the storage may or may not cover this backup: no schedule
			prefix, _, _ := unstructured.NestedString(rule, "prefix")
			if strings.HasPrefix(prefix, storageKeys) && prefix != storageKeys {
				return 0, fmt.Errorf("R2BucketLock %s has a rule for a part of the storage (%s)", lock.GetName(), prefix)
			}
			if !strings.HasPrefix(storageKeys, prefix) {
				continue
			}
			condition, _, _ := unstructured.NestedMap(rule, "condition")
			age, isAge := condition["ageDays"].(int64)
			if !isAge || len(condition) != 1 {
				return 0, fmt.Errorf("R2BucketLock %s has a rule that is not an age rule", lock.GetName())
			}
			days = max(days, age)
		}
	}
	if days == 0 {
		return 0, fmt.Errorf("no age lock rule for bucket %s", location.Host)
	}
	return time.Duration(days) * 24 * time.Hour, nil
}

// storagePrefix is the resolved storage prefix of the BackupConfig.
func storagePrefix(backupConfig *v1beta1.BackupConfigWithSecrets) string {
	switch {
	case backupConfig.Spec.Storage.S3 != nil:
		return backupConfig.Spec.Storage.S3.ResolvedPrefix
	case backupConfig.Spec.Storage.GCS != nil:
		return backupConfig.Spec.Storage.GCS.ResolvedPrefix
	}
	return ""
}

// waitForLockExpiry returns how long a deletion refused by the bucket lock still waits for
// the recorded lock expiry (BackupRetainedUntilAnnotationName), if it is in the future.
// A Backup annotated to skip the storage cleanup, or whose BackupConfig (nil when not
// found) is, does not wait: it is released now.
func waitForLockExpiry(backup *cnpgv1.Backup, backupConfig *v1beta1.BackupConfig, now time.Time) (time.Duration, bool) {
	until, err := time.Parse(time.RFC3339, backup.Annotations[v1beta1.BackupRetainedUntilAnnotationName])
	if err != nil || !until.After(now) || skipStorageCleanup(backup) ||
		(backupConfig != nil && skipStorageCleanup(backupConfig)) {
		return 0, false
	}
	return until.Sub(now), true
}

// ownedBy reports whether the Backup's owner reference names this very BackupConfig
// object, not an earlier one of the same name.
func ownedBy(backup *cnpgv1.Backup, backupConfig *v1beta1.BackupConfig) bool {
	for _, ref := range backup.OwnerReferences {
		if ref.Kind == "BackupConfig" && ref.Name == backupConfig.Name {
			return ref.UID == backupConfig.UID
		}
	}
	return false
}

// backupConfigGone handles a Backup being deleted whose BackupConfig no longer exists (it
// was deleted first: namespace teardown, a delete by hand; the owner reference then makes
// the garbage collector delete the Backup). No storage settings are left to delete with,
// but the data is still in the bucket, where retention (which works from Backup resources)
// never sees it again. So the finalizer stays and the Backup is marked deletion-failing
// (condition reason BackupConfigNotFound) until the Backup carries the skip-storage-cleanup
// annotation. A BackupConfig re-created under the same name is another object (UID) and
// does not delete the data: deleting a BackupConfig never deletes backups by itself.
func (b *BackupDeletionController) backupConfigGone(ctx context.Context, backup *cnpgv1.Backup) error {
	logger := logr.FromContextOrDiscard(ctx)
	switch {
	case !containsString(backup.Finalizers, v1beta1.BackupFinalizerName):
		return nil
	case backup.Status.BackupID == "", skipStorageCleanup(backup):
		// No physical backup, or the operator decided the data stays (or is removed by hand).
		return removeBackupFinalizer(ctx, backup, b.Client)
	}
	logger.Info("The Backup's BackupConfig is gone, keeping the finalizer: the backup is still in storage. "+
		"Annotate the Backup to release it",
		"backup", client.ObjectKeyFromObject(backup), "backupID", backup.Status.BackupID,
		"annotation", v1beta1.SkipStorageCleanupAnnotationName)
	if err := markBackup(ctx, b.Client, backup, v1beta1.BackupDeletionFailingAnnotationName,
		v1beta1.BackupRetainedByLockAnnotationName, time.Now()); err != nil {
		return fmt.Errorf("while marking backup deletion failing: %w", err)
	}
	return nil
}

// deletionFailed marks the Backup with BackupDeletionFailingAnnotationName (the status
// controller turns it into ConditionTypeBackupDeletionFailing) and returns err; the
// finalizer stays and the deletion is retried.
func (b *BackupDeletionController) deletionFailed(ctx context.Context, backup *cnpgv1.Backup, err error) error {
	if markErr := markBackup(ctx, b.Client, backup, v1beta1.BackupDeletionFailingAnnotationName,
		v1beta1.BackupRetainedByLockAnnotationName, time.Now()); markErr != nil {
		return errors.Join(err, fmt.Errorf("while marking backup deletion failing: %w", markErr))
	}
	return err
}

func skipStorageCleanup(obj metav1.Object) bool {
	return obj.GetAnnotations()[v1beta1.SkipStorageCleanupAnnotationName] == "true"
}

// markBackup sets annotation set to now unless it is already set (it keeps the time of
// the first occurrence) and removes annotation unset: a deletion is either retained by
// the lock or failing, never both.
func markBackup(ctx context.Context, c client.Client, backup *cnpgv1.Backup, set, unset string, now time.Time) error {
	_, isSet := backup.Annotations[set]
	_, isUnset := backup.Annotations[unset]
	if isSet && !isUnset {
		return nil
	}
	patch := client.MergeFrom(backup.DeepCopy())
	if backup.Annotations == nil {
		backup.Annotations = map[string]string{}
	}
	if !isSet {
		backup.Annotations[set] = now.UTC().Format(time.RFC3339)
	}
	delete(backup.Annotations, unset)
	return c.Patch(ctx, backup, patch)
}

func removeBackupFinalizer(ctx context.Context, backup *cnpgv1.Backup, client client.Client) error {
	if !containsString(backup.Finalizers, v1beta1.BackupFinalizerName) {
		return nil
	}

	backup.Finalizers = removeString(backup.Finalizers, v1beta1.BackupFinalizerName)
	if err := client.Update(ctx, backup); err != nil {
		return fmt.Errorf("while removing cleanup finalizer from Backup: %w", err)
	}
	return nil
}

// deleteBackupConfig cleans up backup storage specified in BackupConfig (if needed) and removes finalizer from BackupConfig object
func (b *BackupDeletionController) deleteBackupConfig(ctx context.Context, backupConfigKey client.ObjectKey) error {
	logger := logr.FromContextOrDiscard(ctx)
	backupConfig := &v1beta1.BackupConfig{}
	err := b.Get(ctx, backupConfigKey, backupConfig)
	if err != nil {
		// If the BackupConfig is already gone, there's nothing to do
		return client.IgnoreNotFound(err)
	}

	// Check if deletion management is disabled for BackupConfig
	if backupConfig.Spec.Retention.IgnoreForBackupConfigDeletion {
		logger.Info("Deletion management disabled for BackupConfig, skipping storage cleanup", "backupConfig", backupConfigKey)
	} else if skipStorageCleanup(backupConfig) {
		logger.Info("Skipping BackupConfig storage cleanup: requested by annotation, the data stays in storage",
			"backupConfig", backupConfigKey, "annotation", v1beta1.SkipStorageCleanupAnnotationName)
	} else {
		// Unreachable or refusing storage keeps the finalizer and is retried, as for a Backup.
		backupConfigWithSecrets, err := backupConfig.PrefetchSecretsData(ctx, b)
		if err != nil {
			return fmt.Errorf(
				"while fetching secrets data for backupConfig %v: %w",
				client.ObjectKeyFromObject(backupConfig),
				err,
			)
		}

		// Performing cleanup for all known PG versions (to remove both old-version backups and current backups)
		for pgVersion := 11; pgVersion <= 19; pgVersion++ {
			result, err := walg.NewClientFromBackupConfig(backupConfigWithSecrets, pgVersion).DeleteAllBackupsAndWALsInStorage(ctx)
			if errors.Is(err, walg.ErrStorageGone) {
				logger.Info("The bucket does not exist, nothing left to delete", "backupConfig", backupConfigKey)
				break
			} else if err != nil && result != nil {
				return fmt.Errorf(
					"while wal-g storage cleanup: error %w\nWAL-G stdout: %s\nWAL-G stderr: %s",
					err,
					string(result.Stdout()),
					string(result.Stderr()),
				)
			} else if err != nil {
				return fmt.Errorf("while wal-g storage cleanup: %w", err)
			}
		}
	}

	// Remove our finalizer from the list and update
	if containsString(backupConfig.Finalizers, v1beta1.BackupConfigFinalizerName) {
		backupConfig.Finalizers = removeString(backupConfig.Finalizers, v1beta1.BackupConfigFinalizerName)
		if err := b.Update(ctx, backupConfig); err != nil {
			logger.Error(err, "while removing cleanup finalizer from BackupConfig")
			return fmt.Errorf("while removing cleanup finalizer from BackupConfig: %w", err)
		}
	}

	return nil
}
