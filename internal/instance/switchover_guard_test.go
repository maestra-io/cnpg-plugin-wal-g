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
			clock time.Time
			env   retryEnv
			cfg   targetRetryConfig
			t0    time.Time
		)

		BeforeEach(func() {
			t0 = time.Date(2026, 10, 21, 0, 13, 58, 0, time.UTC)
			clock = t0
			cfg = targetRetryConfig{Timeout: 10 * time.Second, Interval: time.Second}
			env = retryEnv{
				now: func() time.Time { return clock },
				sleep: func(_ context.Context, d time.Duration) error {
					clock = clock.Add(d)
					return nil
				},
			}
		})

		// fetchAppearingAt simulates a segment that lands in the archive at t0+landAt
		// (never if landAt < 0); each fetch takes fetchTime.
		fetchAppearingAt := func(landAt, fetchTime time.Duration) func(context.Context) fetchOutcome {
			return func(context.Context) fetchOutcome {
				begin := clock
				clock = clock.Add(fetchTime)
				// A fetch only sees an object that existed when it started.
				if landAt >= 0 && !begin.Before(t0.Add(landAt)) {
					return fetchFound
				}
				return fetchNotFound
			}
		}
		retry := func(budget time.Duration, fetch func(context.Context) fetchOutcome) retryResult {
			return fetchWithTargetRetry(context.Background(), cfg, env, budget, fetch)
		}

		It("returns at once when the segment is there", func() {
			res := retry(cfg.Timeout, fetchAppearingAt(0, 200*time.Millisecond))
			Expect(res.Outcome).To(Equal(fetchFound))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Stop).To(Equal(stopNone))
			Expect(res.Waited).To(BeZero())
		})

		It("waits for the old primary's final segment (the race of the incident)", func() {
			// The incident: the old primary's final segment landed ~3.6 s after the
			// target's stream ended. Polls start at 0, 1.2, 2.4 and 3.6 s (a 1 s pause
			// after each 200 ms fetch); the fourth one finds it.
			res := retry(cfg.Timeout, fetchAppearingAt(3600*time.Millisecond, 200*time.Millisecond))
			Expect(res.Outcome).To(Equal(fetchFound))
			Expect(res.Attempts).To(Equal(4))
			Expect(res.Waited).To(BeNumerically("<", 4*time.Second))
		})

		It("gives up when the budget is used up and the segment never comes", func() {
			res := retry(cfg.Timeout, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(res.Outcome).To(Equal(fetchNotFound))
			Expect(res.Stop).To(Equal(stopDeadline))
			Expect(res.Attempts).To(BeNumerically(">=", 9))
			Expect(res.Waited).To(BeNumerically(">=", 10*time.Second))
			Expect(res.Waited).To(BeNumerically("<", 11*time.Second))
		})

		It("uses only the budget left over from earlier requests", func() {
			res := retry(2500*time.Millisecond, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(res.Stop).To(Equal(stopDeadline))
			Expect(res.Waited).To(BeNumerically(">=", 2500*time.Millisecond))
			Expect(res.Waited).To(BeNumerically("<", 3500*time.Millisecond))
		})

		It("makes a single fetch when the budget is spent", func() {
			res := retry(0, fetchAppearingAt(-1, 200*time.Millisecond))
			Expect(res.Outcome).To(Equal(fetchNotFound))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Stop).To(Equal(stopDeadline))
		})

		It("does not retry other errors", func() {
			calls := 0
			res := retry(cfg.Timeout, func(context.Context) fetchOutcome {
				calls++
				return fetchFailed
			})
			Expect(res.Outcome).To(Equal(fetchFailed))
			Expect(res.Attempts).To(Equal(1))
			Expect(calls).To(Equal(1))
		})

		It("stops when the request context is closed", func() {
			env.sleep = func(context.Context, time.Duration) error { return context.Canceled }
			res := retry(cfg.Timeout, fetchAppearingAt(-1, 0))
			Expect(res.Attempts).To(Equal(1))
			Expect(res.Stop).To(Equal(stopContextClosed))
		})
	})

	Describe("switchoverRetryState", func() {
		It("budgets the wait per switchover", func() {
			var st switchoverRetryState
			Expect(st.begin("db-2@t1", 10*time.Second)).To(Equal(10 * time.Second))
			st.end("db-2@t1", 7*time.Second)
			Expect(st.begin("db-2@t1", 10*time.Second)).To(Equal(3 * time.Second))
			st.end("db-2@t1", 5*time.Second)
			Expect(st.begin("db-2@t1", 10*time.Second)).To(BeZero())

			// A new switchover starts afresh; a late report of the old one is dropped.
			Expect(st.begin("db-3@t2", 10*time.Second)).To(Equal(10 * time.Second))
			st.end("db-2@t1", 9*time.Second)
			Expect(st.begin("db-3@t2", 10*time.Second)).To(Equal(10 * time.Second))
		})
	})

	Describe("targetRetryConfigFromViper", func() {
		AfterEach(func() {
			viper.Set(SwitchoverTargetRetryTimeoutKey, nil)
			viper.Set(SwitchoverTargetRetryIntervalKey, nil)
		})

		It("uses the defaults when nothing is set", func() {
			Expect(targetRetryConfigFromViper()).To(Equal(targetRetryConfig{
				Timeout:  DefaultSwitchoverTargetRetryTimeout,
				Interval: DefaultSwitchoverTargetRetryInterval,
			}))
		})

		It("reads durations and bare seconds, and ignores invalid values", func() {
			viper.Set(SwitchoverTargetRetryTimeoutKey, "15")
			viper.Set(SwitchoverTargetRetryIntervalKey, "soon")
			Expect(targetRetryConfigFromViper()).To(Equal(targetRetryConfig{
				Timeout:  15 * time.Second,
				Interval: DefaultSwitchoverTargetRetryInterval,
			}))
			viper.Set(SwitchoverTargetRetryIntervalKey, "500ms")
			Expect(targetRetryConfigFromViper().Interval).To(Equal(500 * time.Millisecond))
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
