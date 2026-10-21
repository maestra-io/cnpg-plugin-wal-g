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

// Switchover WAL guard.
//
// The problem (cloudnative-pg/cloudnative-pg#11715): during a switchover the
// old primary's fast shutdown can stall, for example while a logical walsender
// waits for a CDC client that never confirms the trailing WAL. When the stall
// outlasts wal_sender_timeout, the physical walsenders are reaped too, so the
// old primary's last WAL records (the shutdown XLOG_SWITCH that ends segment
// YY, then the shutdown checkpoint) never reach the standbys by streaming. The
// old primary does archive segment YY, complete, in its final archiver cycle.
//
//   - The target primary may promote without YY. Its last restore attempt for
//     YY was decided before (or just as) the object landed in the archive, and
//     PostgreSQL acts on a promote request right after a failed restore. The new
//     timeline then forks mid-segment, before the XLOG_SWITCH.
//   - Another replica, which lost its stream at the same moment, restores YY
//     from the archive, replays the XLOG_SWITCH, and ends up past the fork for
//     good: "new timeline N forked off current database system timeline N-1
//     before current recovery point", "requested starting point ... is not in
//     this server's history". CNPG keeps reporting the cluster as healthy.
//
// Upstream CNPG has no fix yet, so the plugin closes the race in its WAL
// restore path while a switchover is in progress (status.targetPrimary !=
// status.currentPrimary), and only for WAL segments on the cluster's current
// timeline (status.timelineID), which is still the old primary's timeline:
//
//   - A replica that is not the target gets "not found" for those segments
//     without touching the archive. PostgreSQL then keeps retrying streaming and
//     follows the new primary once it is promoted, instead of replaying WAL of
//     the old timeline that may be past the fork. The archive is the only way
//     such a record can reach it: its stream from the old primary is gone.
//   - The target primary, on "not found", keeps polling the archive for a
//     bounded time instead of answering at once, so the old primary's final
//     segment is replayed before the promotion if it lands meanwhile. The fork
//     then sits at the segment boundary, after the XLOG_SWITCH, and nothing in
//     the archive is past it. The wait ends early once PostgreSQL has a promote
//     request ($PGDATA/promote.signal, written by pg_ctl promote) and a fetch
//     that started a grace period after that request also found nothing: CNPG
//     promotes only after the old primary has shut down, and its final archiver
//     cycle runs before it exits, so by then the segment is either there or
//     never coming.
//
// Both behaviors are on by default and can be turned off per Cluster with the
// plugin parameters below. Timeline history files, .partial and .backup files
// are never affected, nor are replica clusters (their WAL comes from another
// cluster's archive) or the recovery-mode sidecar of a full-recovery job.

import (
	"context"
	"encoding/hex"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/spf13/viper"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
)

const (
	// SwitchoverReplicaGuardParameter is the plugin parameter that turns the
	// non-target replica guard off ("false"). Default: on.
	SwitchoverReplicaGuardParameter = "switchoverReplicaWALGuard"
	// SwitchoverTargetRetryParameter is the plugin parameter that turns the
	// target primary retry off ("false"). Default: on.
	SwitchoverTargetRetryParameter = "switchoverTargetWALRetry"

	// DefaultSwitchoverTargetRetryTimeout bounds how long one restore request
	// of the target primary keeps polling for a missing segment.
	DefaultSwitchoverTargetRetryTimeout = 10 * time.Second
	// DefaultSwitchoverTargetRetryInterval is the pause between two polls.
	DefaultSwitchoverTargetRetryInterval = 1 * time.Second
	// DefaultSwitchoverTargetPromoteGrace is how long after a promote request
	// was first seen a fetch must have started for its "not found" to be final.
	DefaultSwitchoverTargetPromoteGrace = 3 * time.Second

	// walSegmentNameLength is the length of a WAL segment file name:
	// 8 hex digits of timeline, 8 of log id, 8 of segment.
	walSegmentNameLength = 24
	// promoteSignalFile is the file pg_ctl promote creates in PGDATA.
	promoteSignalFile = "promote.signal"
	// promoteWatchStep is how often the target checks for promote.signal
	// between two polls of the archive.
	promoteWatchStep = 100 * time.Millisecond
)

// restoreAction is what Restore does with one request.
type restoreAction int

const (
	// restoreNormally fetches once and answers, the pre-guard behavior.
	restoreNormally restoreAction = iota
	// restoreRefuse answers "not found" without fetching (non-target replica).
	restoreRefuse
	// restoreRetryOnMiss keeps polling the archive on "not found" (target primary).
	restoreRetryOnMiss
)

func (a restoreAction) String() string {
	switch a {
	case restoreRefuse:
		return "refuse"
	case restoreRetryOnMiss:
		return "retry-on-miss"
	default:
		return "normal"
	}
}

// switchoverGuardOptions are the per-Cluster switches of the guard.
type switchoverGuardOptions struct {
	ReplicaGuard bool
	TargetRetry  bool
}

// switchoverGuardOptionsFromCluster reads the guard switches from the
// Cluster's plugin parameters. Anything but an explicit "off" value keeps a
// behavior on.
func switchoverGuardOptionsFromCluster(cluster *cnpgv1.Cluster) switchoverGuardOptions {
	opts := switchoverGuardOptions{ReplicaGuard: true, TargetRetry: true}
	pluginConfig := common.GetPluginConfigFromCluster(cluster)
	if pluginConfig == nil || pluginConfig.Parameters == nil {
		return opts
	}
	if v, ok := pluginConfig.Parameters[SwitchoverReplicaGuardParameter]; ok && isOffValue(v) {
		opts.ReplicaGuard = false
	}
	if v, ok := pluginConfig.Parameters[SwitchoverTargetRetryParameter]; ok && isOffValue(v) {
		opts.TargetRetry = false
	}
	return opts
}

func isOffValue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "false", "0", "no", "off", "disabled":
		return true
	}
	return false
}

// walSegmentTimeline returns the timeline of a WAL segment file name, and
// false for anything that is not a plain segment name (history files,
// .partial, .backup, ...).
func walSegmentTimeline(walName string) (int, bool) {
	name := path.Base(walName)
	if len(name) != walSegmentNameLength {
		return 0, false
	}
	if _, err := hex.DecodeString(name); err != nil {
		return 0, false
	}
	tli, err := strconv.ParseUint(name[0:8], 16, 32)
	if err != nil || tli == 0 {
		return 0, false
	}
	return int(tli), true
}

// decideRestoreAction decides how a restore request for walName, made by the
// instance podName, is served given the Cluster's status. It returns the
// action and a short reason for the log.
func decideRestoreAction(
	cluster *cnpgv1.Cluster,
	podName string,
	walName string,
	opts switchoverGuardOptions,
) (restoreAction, string) {
	if cluster == nil || podName == "" {
		return restoreNormally, "no cluster status or pod name"
	}
	if cluster.IsReplica() {
		return restoreNormally, "replica cluster"
	}
	status := &cluster.Status
	if status.TargetPrimary == "" || status.CurrentPrimary == "" ||
		status.TargetPrimary == status.CurrentPrimary {
		return restoreNormally, "no switchover in progress"
	}
	tli, ok := walSegmentTimeline(walName)
	if !ok {
		return restoreNormally, "not a WAL segment"
	}
	if status.TimelineID == 0 || tli != status.TimelineID {
		return restoreNormally, "segment is not on the cluster's current timeline"
	}
	if podName == status.TargetPrimary {
		if opts.TargetRetry {
			return restoreRetryOnMiss, "target primary during switchover"
		}
		return restoreNormally, "target primary, retry disabled"
	}
	if opts.ReplicaGuard {
		return restoreRefuse, "non-target replica during switchover"
	}
	return restoreNormally, "non-target replica, guard disabled"
}

// targetRetryConfig bounds the target primary's polling.
type targetRetryConfig struct {
	Timeout      time.Duration
	Interval     time.Duration
	PromoteGrace time.Duration
}

// fetchOutcome is the result of one fetch attempt.
type fetchOutcome int

const (
	fetchFound fetchOutcome = iota
	fetchNotFound
	fetchFailed
)

// retryEnv makes the retry loop testable without real time or files.
type retryEnv struct {
	now              func() time.Time
	sleep            func(ctx context.Context, d time.Duration) error
	promoteRequested func() bool
}

// Viper keys (sidecar flags / env) of the target primary retry.
const (
	SwitchoverTargetRetryTimeoutKey  = "switchover-target-retry-timeout"
	SwitchoverTargetRetryIntervalKey = "switchover-target-retry-interval"
	SwitchoverTargetPromoteGraceKey  = "switchover-target-promote-grace"
)

// targetRetryConfigFromViper reads the retry bounds from the sidecar's flags
// or environment, falling back to the defaults for unset or invalid values.
// A timeout of 0 means a single fetch (no retry).
func targetRetryConfigFromViper() targetRetryConfig {
	cfg := targetRetryConfig{
		Timeout:      DefaultSwitchoverTargetRetryTimeout,
		Interval:     DefaultSwitchoverTargetRetryInterval,
		PromoteGrace: DefaultSwitchoverTargetPromoteGrace,
	}
	if d, ok := durationFromViper(SwitchoverTargetRetryTimeoutKey); ok && d >= 0 {
		cfg.Timeout = d
	}
	if d, ok := durationFromViper(SwitchoverTargetRetryIntervalKey); ok && d > 0 {
		cfg.Interval = d
	}
	if d, ok := durationFromViper(SwitchoverTargetPromoteGraceKey); ok && d >= 0 {
		cfg.PromoteGrace = d
	}
	return cfg
}

// durationFromViper reads a duration set by flag or environment. A bare
// number means seconds ("15" = 15s), not nanoseconds as in viper's own cast.
func durationFromViper(key string) (time.Duration, bool) {
	if !viper.IsSet(key) {
		return 0, false
	}
	raw := strings.TrimSpace(viper.GetString(key))
	if n, err := strconv.ParseFloat(raw, 64); err == nil {
		return time.Duration(n * float64(time.Second)), true
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, false
	}
	return d, true
}

func realRetryEnv(pgData string) retryEnv {
	return retryEnv{
		now: time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			if d <= 0 {
				return nil
			}
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
		promoteRequested: func() bool {
			if pgData == "" {
				return false
			}
			_, err := os.Stat(path.Join(pgData, promoteSignalFile))
			return err == nil
		},
	}
}

// retryStopReason says why fetchWithTargetRetry stopped on a miss.
type retryStopReason string

const (
	stopNone          retryStopReason = ""
	stopDeadline      retryStopReason = "retry timeout reached"
	stopPromote       retryStopReason = "promote requested and grace period passed"
	stopContextClosed retryStopReason = "request context closed"
)

// fetchWithTargetRetry calls fetch until it finds the segment, fails with an
// error other than "not found", or a stop condition holds on a miss:
//   - the retry timeout since the first attempt has passed, or
//   - a promote request was seen, and the attempt that just missed started at
//     least PromoteGrace after it was first seen.
//
// It returns the last outcome, the number of attempts and, on a miss, why it
// stopped.
func fetchWithTargetRetry(
	ctx context.Context,
	cfg targetRetryConfig,
	env retryEnv,
	fetch func(ctx context.Context) fetchOutcome,
) (fetchOutcome, int, retryStopReason) {
	start := env.now()
	deadline := start.Add(cfg.Timeout)
	var promoteSeen time.Time

	for attempt := 1; ; attempt++ {
		attemptStart := env.now()
		if promoteSeen.IsZero() && env.promoteRequested() {
			promoteSeen = attemptStart
		}

		outcome := fetch(ctx)
		if outcome != fetchNotFound {
			return outcome, attempt, stopNone
		}

		now := env.now()
		if promoteSeen.IsZero() && env.promoteRequested() {
			promoteSeen = now
		}
		if !now.Before(deadline) {
			return outcome, attempt, stopDeadline
		}
		if !promoteSeen.IsZero() && !attemptStart.Before(promoteSeen.Add(cfg.PromoteGrace)) {
			return outcome, attempt, stopPromote
		}

		wake := now.Add(cfg.Interval)
		if wake.After(deadline) {
			wake = deadline
		}
		if !promoteSeen.IsZero() {
			if graceEnd := promoteSeen.Add(cfg.PromoteGrace); graceEnd.After(now) && graceEnd.Before(wake) {
				wake = graceEnd
			}
		}
		// Sleep in short steps, watching for the promote request: the grace
		// period starts when it is first seen, not at the next poll.
		for now.Before(wake) {
			step := wake.Sub(now)
			if step > promoteWatchStep {
				step = promoteWatchStep
			}
			if err := env.sleep(ctx, step); err != nil {
				return outcome, attempt, stopContextClosed
			}
			now = env.now()
			if promoteSeen.IsZero() && env.promoteRequested() {
				promoteSeen = now
				if graceEnd := now.Add(cfg.PromoteGrace); graceEnd.Before(wake) {
					wake = graceEnd
				}
			}
		}
	}
}
