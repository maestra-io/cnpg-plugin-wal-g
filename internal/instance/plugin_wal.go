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

package instance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
)

// walFetchNotFoundExitCode is wal-g wal-fetch's exit code for "no such WAL in storage".
const walFetchNotFoundExitCode = 74

// errSwitchoverGuardRefused is returned (as "not found") for a segment the
// switchover guard does not serve.
var errSwitchoverGuardRefused = errors.New("switchover WAL guard: segment withheld during switchover")

var (
	lastArchivedWALTime   *v1.Time // IMPORTANT: Do not access directly, use only thread-safe methods SetLastArchivedWALTime/GetLastArchivedWALTime
	lastArchivedWALTimeMu sync.Mutex
)

// WALServiceImplementation is the implementation of the WAL Service
type WALServiceImplementation struct {
	wal.UnimplementedWALServer
	Client client.Client
	// PodName is the instance this sidecar runs in (POD_NAME); the
	// switchover guard compares it with status.targetPrimary.
	PodName string
}

// GetCapabilities implements the WALService interface
func (w WALServiceImplementation) GetCapabilities(
	_ context.Context,
	_ *wal.WALCapabilitiesRequest,
) (*wal.WALCapabilitiesResult, error) {
	return &wal.WALCapabilitiesResult{
		Capabilities: []*wal.WALCapability{
			{
				Type: &wal.WALCapability_Rpc{
					Rpc: &wal.WALCapability_RPC{
						Type: wal.WALCapability_RPC_TYPE_ARCHIVE_WAL,
					},
				},
			},
			{
				Type: &wal.WALCapability_Rpc{
					Rpc: &wal.WALCapability_RPC{
						Type: wal.WALCapability_RPC_TYPE_RESTORE_WAL,
					},
				},
			},
		},
	}, nil
}

// Archive implements the WALService interface
func (w WALServiceImplementation) Archive(
	ctx context.Context,
	request *wal.WALArchiveRequest,
) (*wal.WALArchiveResult, error) {
	if viper.GetString("mode") != common.PluginModeNormal {
		return nil, fmt.Errorf("WAL archivation restricted while running not in 'normal' mode")
	}

	pgMajorVersion := viper.GetInt("pg_major")
	if pgMajorVersion == 0 {
		return nil, fmt.Errorf("backup request failed: no PG_MAJOR env variable specified")
	}

	logger := logr.FromContextOrDiscard(ctx).WithName("plugin_wal").WithValues("method", "Archive")
	childrenCtx := logr.NewContext(ctx, logger)

	cluster, err := common.CnpgClusterFromJSON(request.ClusterDefinition)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluser: %w", err)
	}

	backupConfig, err := w.getBackupConfig(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup config: %w", err)
	}

	walgClient := walg.NewClientFromBackupConfig(backupConfig, pgMajorVersion)
	result, err := walgClient.WALPush(childrenCtx, request.SourceFileName)

	logger = logger.WithValues("stdout", string(result.Stdout()), "stderr", string(result.Stderr()))

	if err != nil {
		return nil, fmt.Errorf(
			"'wal-g wal-push' for wal %s: %w\nstdout: %s\n stderr: %s",
			request.SourceFileName, err, string(result.Stdout()), string(result.Stderr()),
		)
	}

	now := v1.Now()
	SetLastArchivedWALTime(&now)

	logger.Info(fmt.Sprintf("Successful run wal-g wal-push %s", request.SourceFileName))
	return &wal.WALArchiveResult{}, nil
}

// Restore implements the WALService interface
func (w WALServiceImplementation) Restore(
	ctx context.Context,
	request *wal.WALRestoreRequest,
) (*wal.WALRestoreResult, error) {
	logger := logr.FromContextOrDiscard(ctx).WithName("plugin_wal").WithValues("method", "Restore")
	childrenCtx := logr.NewContext(ctx, logger)

	pgMajorVersion := viper.GetInt("pg_major")
	if pgMajorVersion == 0 {
		return nil, fmt.Errorf("backup request failed: no PG_MAJOR env variable specified")
	}

	cluster, err := common.CnpgClusterFromJSON(request.ClusterDefinition)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cluser: %w", err)
	}

	backupConfig, err := w.getBackupConfig(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup config: %w", err)
	}

	action := restoreNormally
	decidedOn := cluster
	if viper.GetString("mode") == common.PluginModeNormal {
		// Two views of the Cluster: the sidecar's own watch and the instance
		// manager's copy in the request. Either may lag; a switchover seen in
		// either one counts, since guarding a bit too long only delays a fetch.
		var reason string
		// The opt-out parameters are taken from the fresher (watched) copy.
		decidedOn = w.liveCluster(ctx, cluster)
		opts := switchoverGuardOptionsFromCluster(decidedOn)
		action, reason = decideRestoreAction(decidedOn, w.PodName, request.SourceWalName, opts)
		if action == restoreNormally && decidedOn != cluster {
			if a, r := decideRestoreAction(cluster, w.PodName, request.SourceWalName, opts); a != restoreNormally {
				action, reason, decidedOn = a, r+" (instance manager's view)", cluster
			}
		}
		if action != restoreNormally {
			logger = logger.WithValues(
				"switchoverGuard", action.String(), "reason", reason,
				"targetPrimary", decidedOn.Status.TargetPrimary, "currentPrimary", decidedOn.Status.CurrentPrimary,
				"timelineID", decidedOn.Status.TimelineID,
			)
		}
	}

	if action == restoreRefuse {
		logger.Info(fmt.Sprintf(
			"Switchover in progress: not serving %s, a WAL segment on the old primary's timeline, to a non-target replica; "+
				"it will stream from the new primary instead", request.SourceWalName))
		return &wal.WALRestoreResult{}, fmt.Errorf("%w: %s", errSwitchoverGuardRefused, request.SourceWalName)
	}

	walgClient := walg.NewClientFromBackupConfig(backupConfig, pgMajorVersion)
	fetch := func(fetchCtx context.Context) (*cmd.RunResult, error) {
		return walgClient.WALFetch(fetchCtx, request.SourceWalName, request.DestinationFileName)
	}

	var result *cmd.RunResult
	if action == restoreRetryOnMiss {
		result, err = w.fetchAsTargetPrimary(childrenCtx, logger, decidedOn, request.SourceWalName, fetch)
	} else {
		result, err = fetch(childrenCtx)
	}

	logger = logger.WithValues("stdout", string(result.Stdout()), "stderr", string(result.Stderr()))

	if err != nil && result.State().ExitCode() == walFetchNotFoundExitCode {
		logger.Info(fmt.Sprintf("Run wal-g fetch %s %s: no WAL archive found in storage", request.SourceWalName, request.DestinationFileName))
		return &wal.WALRestoreResult{}, err
	}
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to run wal-g fetch %s %s", request.SourceWalName, request.DestinationFileName))
		return &wal.WALRestoreResult{}, err
	}
	logger.Info(fmt.Sprintf("Successful run wal-g fetch %s %s", request.SourceWalName, request.DestinationFileName))
	return &wal.WALRestoreResult{}, nil
}

// liveCluster returns the Cluster as the sidecar's own watch cache has it,
// which tracks status.targetPrimary/currentPrimary independently of the
// instance manager's copy in the request. On error it falls back to the
// request's copy.
func (w WALServiceImplementation) liveCluster(ctx context.Context, fromRequest *cnpgv1.Cluster) *cnpgv1.Cluster {
	if w.Client == nil {
		return fromRequest
	}
	live := &cnpgv1.Cluster{}
	if err := w.Client.Get(ctx, client.ObjectKeyFromObject(fromRequest), live); err != nil {
		logr.FromContextOrDiscard(ctx).V(1).Info("Cannot read the Cluster from the cache, using the request's copy", "error", err.Error())
		return fromRequest
	}
	return live
}

// fetchAsTargetPrimary fetches walName for the switchover target primary,
// polling the archive on "not found" within the switchover's retry budget
// (see switchover_guard.go).
func (w WALServiceImplementation) fetchAsTargetPrimary(
	ctx context.Context,
	logger logr.Logger,
	cluster *cnpgv1.Cluster,
	walName string,
	fetch func(ctx context.Context) (*cmd.RunResult, error),
) (*cmd.RunResult, error) {
	cfg := targetRetryConfigFromViper()
	key := cluster.Status.TargetPrimary + "@" + cluster.Status.TargetPrimaryTimestamp
	budget := targetRetryState.begin(key, cfg.Timeout)
	var (
		result *cmd.RunResult
		err    error
	)
	res := fetchWithTargetRetry(ctx, cfg, realRetryEnv(), budget, func(fetchCtx context.Context) fetchOutcome {
		result, err = fetch(fetchCtx)
		switch {
		case err == nil:
			return fetchFound
		case result.State().ExitCode() == walFetchNotFoundExitCode:
			return fetchNotFound
		default:
			return fetchFailed
		}
	})
	targetRetryState.end(key, res.Waited)
	switch res.Outcome {
	case fetchFound:
		if res.Attempts > 1 {
			logger.Info(fmt.Sprintf("Switchover target primary: %s appeared in the archive after %d attempts (%s)",
				walName, res.Attempts, res.Waited.Round(time.Millisecond)))
		}
	case fetchNotFound:
		logger.Info(fmt.Sprintf("Switchover target primary: %s not in the archive after %d attempts over %s (%s)",
			walName, res.Attempts, res.Waited.Round(time.Millisecond), res.Stop),
			"budgetLeft", (budget - res.Waited).Round(time.Millisecond).String())
	}
	return result, err
}

func (w WALServiceImplementation) getBackupConfig(ctx context.Context, cluster *cnpgv1.Cluster) (*v1beta1.BackupConfigWithSecrets, error) {
	var backupConfig *v1beta1.BackupConfig
	var err error

	if viper.GetString("mode") == common.PluginModeRecovery {
		backupConfig, err = v1beta1.GetBackupConfigForClusterRecovery(ctx, w.Client, cluster)
	} else {
		backupConfig, err = v1beta1.GetBackupConfigForCluster(ctx, w.Client, cluster)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch BackupConfig object: %w", err)
	}

	backupConfigWithSecrets, err := backupConfig.PrefetchSecretsData(ctx, w.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch BackupConfig secrets: %w", err)
	}

	return backupConfigWithSecrets, nil
}

// Status implements the WALService interface
func (w WALServiceImplementation) Status(
	_ context.Context,
	_ *wal.WALStatusRequest,
) (*wal.WALStatusResult, error) {
	panic("implement me")
}

// SetFirstRequired implements the WALService interface
func (w WALServiceImplementation) SetFirstRequired(
	_ context.Context,
	_ *wal.SetFirstRequiredRequest,
) (*wal.SetFirstRequiredResult, error) {
	panic("implement me")
}

// GetLastArchivedWALTime returns time of last successfully uploaded WAL thread-safely
func GetLastArchivedWALTime() *v1.Time {
	lastArchivedWALTimeMu.Lock()
	defer lastArchivedWALTimeMu.Unlock()

	if lastArchivedWALTime == nil {
		return nil
	}

	return lastArchivedWALTime.DeepCopy()
}

// SetLastArchivedWALTime sets the time of last successfully uploaded WAL thread-safely
func SetLastArchivedWALTime(time *v1.Time) {
	lastArchivedWALTimeMu.Lock()
	defer lastArchivedWALTimeMu.Unlock()

	lastArchivedWALTime = time
}
