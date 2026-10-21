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
	"os"
	"path/filepath"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
)

// guardCluster builds a Cluster with the given switchover status; the old
// primary is on timeline 8, as email-scoring was in the incident.
func guardCluster(currentPrimary, targetPrimary string, timelineID int, params map[string]string) *cnpgv1.Cluster {
	return &cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "ns"},
		Spec: cnpgv1.ClusterSpec{
			Plugins: []cnpgv1.PluginConfiguration{{
				Name:       common.PluginNameDeprecated,
				Parameters: params,
			}},
		},
		Status: cnpgv1.ClusterStatus{
			CurrentPrimary: currentPrimary,
			TargetPrimary:  targetPrimary,
			TimelineID:     timelineID,
		},
	}
}

var allOn = switchoverGuardOptions{ReplicaGuard: true, TargetRetry: true}

const (
	oldTLSegment  = "000000080000004D00000018" // the old primary's final segment (timeline 8)
	nextTLSegment = "000000090000004D00000018" // same position on the new timeline
	olderSegment  = "000000070000004D00000017" // a segment of an earlier timeline
)

var _ = Describe("Switchover WAL guard", func() {
	Describe("walSegmentTimeline", func() {
		DescribeTable("parses only plain segment names",
			func(name string, wantTLI int, wantOK bool) {
				tli, ok := walSegmentTimeline(name)
				Expect(ok).To(Equal(wantOK))
				Expect(tli).To(Equal(wantTLI))
			},
			Entry("segment", oldTLSegment, 8, true),
			Entry("segment with a directory", "pg_wal/"+oldTLSegment, 8, true),
			Entry("lower-case hex", "0000000a0000004d00000018", 10, true),
			Entry("history file", "00000009.history", 0, false),
			Entry("partial", oldTLSegment+".partial", 0, false),
			Entry("backup label", oldTLSegment+".00000028.backup", 0, false),
			Entry("not hex", "00000008000000ZZ00000018", 0, false),
			Entry("timeline 0", "000000000000004D00000018", 0, false),
			Entry("empty", "", 0, false),
		)
	})

	Describe("decideRestoreAction", func() {
		DescribeTable("decides from the Cluster status",
			func(cluster *cnpgv1.Cluster, pod, wal string, opts switchoverGuardOptions, want restoreAction) {
				got, reason := decideRestoreAction(cluster, pod, wal, opts)
				Expect(got).To(Equal(want), "reason: %s", reason)
				Expect(reason).NotTo(BeEmpty())
			},
			// The incident: db-3 restored the old timeline's final segment while db-2 was being promoted.
			Entry("non-target replica, switchover, current-timeline segment: refuse",
				guardCluster("db-1", "db-2", 8, nil), "db-3", oldTLSegment, allOn, restoreRefuse),
			Entry("the demoted old primary is a non-target replica too: refuse",
				guardCluster("db-1", "db-2", 8, nil), "db-1", oldTLSegment, allOn, restoreRefuse),
			Entry("target primary, switchover, current-timeline segment: retry on miss",
				guardCluster("db-1", "db-2", 8, nil), "db-2", oldTLSegment, allOn, restoreRetryOnMiss),
			Entry("failover pending (no target yet): refuse",
				guardCluster("db-1", cnpgv1.PendingFailoverMarker, 8, nil), "db-3", oldTLSegment, allOn, restoreRefuse),

			Entry("no switchover: normal",
				guardCluster("db-1", "db-1", 8, nil), "db-3", oldTLSegment, allOn, restoreNormally),
			Entry("switchover finished, timelineID not yet updated: normal",
				guardCluster("db-2", "db-2", 8, nil), "db-3", oldTLSegment, allOn, restoreNormally),
			Entry("bootstrap (no current primary yet): normal",
				guardCluster("", "db-1", 0, nil), "db-1", oldTLSegment, allOn, restoreNormally),
			Entry("segment of an older timeline (replica catching up from a backup): normal",
				guardCluster("db-1", "db-2", 8, nil), "db-3", olderSegment, allOn, restoreNormally),
			Entry("segment of a newer timeline: normal",
				guardCluster("db-1", "db-2", 8, nil), "db-3", nextTLSegment, allOn, restoreNormally),
			Entry("timeline history file: normal",
				guardCluster("db-1", "db-2", 8, nil), "db-3", "00000009.history", allOn, restoreNormally),
			Entry("partial segment: normal",
				guardCluster("db-1", "db-2", 8, nil), "db-3", oldTLSegment+".partial", allOn, restoreNormally),
			Entry("unknown timelineID: normal",
				guardCluster("db-1", "db-2", 0, nil), "db-3", oldTLSegment, allOn, restoreNormally),
			Entry("unknown pod name: normal",
				guardCluster("db-1", "db-2", 8, nil), "", oldTLSegment, allOn, restoreNormally),
			Entry("replica guard disabled: normal",
				guardCluster("db-1", "db-2", 8, nil), "db-3", oldTLSegment,
				switchoverGuardOptions{ReplicaGuard: false, TargetRetry: true}, restoreNormally),
			Entry("target retry disabled: normal",
				guardCluster("db-1", "db-2", 8, nil), "db-2", oldTLSegment,
				switchoverGuardOptions{ReplicaGuard: true, TargetRetry: false}, restoreNormally),
		)

		It("leaves replica clusters alone", func() {
			cluster := guardCluster("db-1", "db-2", 8, nil)
			cluster.Spec.ReplicaCluster = &cnpgv1.ReplicaClusterConfiguration{Enabled: ptrTo(true), Source: "origin"}
			got, _ := decideRestoreAction(cluster, "db-3", oldTLSegment, allOn)
			Expect(got).To(Equal(restoreNormally))
		})

		It("handles a nil cluster", func() {
			got, _ := decideRestoreAction(nil, "db-3", oldTLSegment, allOn)
			Expect(got).To(Equal(restoreNormally))
		})
	})

	Describe("switchoverGuardOptionsFromCluster", func() {
		It("is on by default", func() {
			Expect(switchoverGuardOptionsFromCluster(guardCluster("db-1", "db-1", 1, nil))).To(Equal(allOn))
			Expect(switchoverGuardOptionsFromCluster(&cnpgv1.Cluster{})).To(Equal(allOn))
		})
		DescribeTable("turns a behaviour off only on an explicit off value",
			func(params map[string]string, want switchoverGuardOptions) {
				Expect(switchoverGuardOptionsFromCluster(guardCluster("db-1", "db-1", 1, params))).To(Equal(want))
			},
			Entry("replica guard false", map[string]string{SwitchoverReplicaGuardParameter: "false"},
				switchoverGuardOptions{ReplicaGuard: false, TargetRetry: true}),
			Entry("target retry disabled", map[string]string{SwitchoverTargetRetryParameter: "Disabled"},
				switchoverGuardOptions{ReplicaGuard: true, TargetRetry: false}),
			Entry("both off", map[string]string{SwitchoverReplicaGuardParameter: "off", SwitchoverTargetRetryParameter: "0"},
				switchoverGuardOptions{}),
			Entry("true keeps it on", map[string]string{SwitchoverReplicaGuardParameter: "true"}, allOn),
			Entry("garbage keeps it on", map[string]string{SwitchoverTargetRetryParameter: "maybe"}, allOn),
		)
	})

	Describe("fetchWithTargetRetry", func() {
		var (
			clock    time.Time
			promoted *time.Time // promote.signal appears at this time
			env      retryEnv
			cfg      targetRetryConfig
			starts   []time.Duration // attempt start times, relative to t0
			t0       time.Time
		)

		BeforeEach(func() {
			t0 = time.Date(2026, 10, 21, 0, 13, 58, 0, time.UTC)
			clock = t0
			promoted = nil
			starts = nil
			cfg = targetRetryConfig{Timeout: 10 * time.Second, Interval: time.Second, PromoteGrace: 3 * time.Second}
			env = retryEnv{
				now: func() time.Time { return clock },
				sleep: func(_ context.Context, d time.Duration) error {
					clock = clock.Add(d)
					return nil
				},
				promoteRequested: func() bool { return promoted != nil && !clock.Before(*promoted) },
			}
		})

		// fetchAppearingAt simulates a segment that lands in the archive at t0+landAt;
		// each fetch takes fetchTime.
		fetchAppearingAt := func(landAt, fetchTime time.Duration) func(context.Context) fetchOutcome {
			return func(context.Context) fetchOutcome {
				starts = append(starts, clock.Sub(t0))
				begin := clock
				clock = clock.Add(fetchTime)
				// A fetch only sees an object that existed when it started.
				if landAt >= 0 && !begin.Before(t0.Add(landAt)) {
					return fetchFound
				}
				return fetchNotFound
			}
		}

		It("returns at once when the segment is there", func() {
			outcome, attempts, stop := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(0, 200*time.Millisecond))
			Expect(outcome).To(Equal(fetchFound))
			Expect(attempts).To(Equal(1))
			Expect(stop).To(Equal(stopNone))
		})

		It("waits for the old primary's final segment (the race of the incident)", func() {
			// email-scoring: the target's miss was decided 150 ms before the upload.
			outcome, attempts, _ := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(1500*time.Millisecond, 200*time.Millisecond))
			// Polls at 0, 1.2 and 2.4 s (1 s pause after each 200 ms fetch): the third one finds it.
			Expect(outcome).To(Equal(fetchFound))
			Expect(attempts).To(Equal(3))
			Expect(clock.Sub(t0)).To(BeNumerically("<", 3*time.Second))
		})

		It("gives up after the retry timeout when the segment never comes", func() {
			outcome, attempts, stop := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(outcome).To(Equal(fetchNotFound))
			Expect(stop).To(Equal(stopDeadline))
			Expect(attempts).To(BeNumerically(">=", 9))
			Expect(clock.Sub(t0)).To(BeNumerically(">=", 10*time.Second))
			Expect(clock.Sub(t0)).To(BeNumerically("<", 10*time.Second+time.Second))
		})

		It("stops early once a promote request is pending and a fetch after the grace missed", func() {
			p := t0.Add(1 * time.Second)
			promoted = &p
			outcome, _, stop := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(outcome).To(Equal(fetchNotFound))
			Expect(stop).To(Equal(stopPromote))
			// The promote request is seen within one watch step, and the deciding fetch
			// started PromoteGrace after that.
			last := starts[len(starts)-1]
			Expect(last).To(BeNumerically(">=", 1*time.Second+cfg.PromoteGrace))
			Expect(last).To(BeNumerically("<=", 1*time.Second+promoteWatchStep+cfg.PromoteGrace))
			Expect(clock.Sub(t0)).To(BeNumerically("<", 4500*time.Millisecond))
		})

		It("still finds a segment that lands during the promote grace", func() {
			p := t0.Add(500 * time.Millisecond)
			promoted = &p
			outcome, _, _ := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(2500*time.Millisecond, 200*time.Millisecond))
			Expect(outcome).To(Equal(fetchFound))
		})

		It("does not retry other errors", func() {
			calls := 0
			outcome, attempts, _ := fetchWithTargetRetry(context.Background(), cfg, env, func(context.Context) fetchOutcome {
				calls++
				return fetchFailed
			})
			Expect(outcome).To(Equal(fetchFailed))
			Expect(attempts).To(Equal(1))
			Expect(calls).To(Equal(1))
		})

		It("makes a single attempt with a zero timeout", func() {
			cfg.Timeout = 0
			outcome, attempts, stop := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(outcome).To(Equal(fetchNotFound))
			Expect(attempts).To(Equal(1))
			Expect(stop).To(Equal(stopDeadline))
		})

		It("stops when the request context is closed", func() {
			env.sleep = func(context.Context, time.Duration) error { return context.Canceled }
			_, attempts, stop := fetchWithTargetRetry(context.Background(), cfg, env, fetchAppearingAt(-1, 0))
			Expect(attempts).To(Equal(1))
			Expect(stop).To(Equal(stopContextClosed))
		})
	})

	Describe("promote request detection", func() {
		It("sees promote.signal in PGDATA", func() {
			dir := GinkgoT().TempDir()
			env := realRetryEnv(dir)
			Expect(env.promoteRequested()).To(BeFalse())
			Expect(os.WriteFile(filepath.Join(dir, promoteSignalFile), nil, 0o600)).To(Succeed())
			Expect(env.promoteRequested()).To(BeTrue())
			Expect(realRetryEnv("").promoteRequested()).To(BeFalse())
		})
	})

	Describe("targetRetryConfigFromViper", func() {
		AfterEach(func() {
			viper.Set(SwitchoverTargetRetryTimeoutKey, nil)
			viper.Set(SwitchoverTargetRetryIntervalKey, nil)
			viper.Set(SwitchoverTargetPromoteGraceKey, nil)
		})

		It("uses the defaults when nothing is set", func() {
			Expect(targetRetryConfigFromViper()).To(Equal(targetRetryConfig{
				Timeout:      DefaultSwitchoverTargetRetryTimeout,
				Interval:     DefaultSwitchoverTargetRetryInterval,
				PromoteGrace: DefaultSwitchoverTargetPromoteGrace,
			}))
		})

		It("reads durations and bare seconds, and ignores invalid values", func() {
			viper.Set(SwitchoverTargetRetryTimeoutKey, "15")
			viper.Set(SwitchoverTargetRetryIntervalKey, "500ms")
			viper.Set(SwitchoverTargetPromoteGraceKey, "soon")
			Expect(targetRetryConfigFromViper()).To(Equal(targetRetryConfig{
				Timeout:      15 * time.Second,
				Interval:     500 * time.Millisecond,
				PromoteGrace: DefaultSwitchoverTargetPromoteGrace,
			}))
		})

		It("accepts 0 to disable the retry but not a zero interval", func() {
			viper.Set(SwitchoverTargetRetryTimeoutKey, "0")
			viper.Set(SwitchoverTargetRetryIntervalKey, "0s")
			cfg := targetRetryConfigFromViper()
			Expect(cfg.Timeout).To(Equal(time.Duration(0)))
			Expect(cfg.Interval).To(Equal(DefaultSwitchoverTargetRetryInterval))
		})
	})
})

func ptrTo[T any](v T) *T { return &v }
