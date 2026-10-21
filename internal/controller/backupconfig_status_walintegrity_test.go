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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
)

// walShowAfterArchiveSwitch is `wal-g wal-show --detailed-json` of a production
// prefix after its archive moved from barman to wal-g (trimmed): timeline 9
// starts at D4/81, a stray header-only segment wal-g pushed before the switch;
// D4/82..89 were archived by barman elsewhere; the first wal-g backup starts at
// D4/8C. No backup needs the gap, and wal-g wal-verify reports integrity OK.
const walShowAfterArchiveSwitch = `[{"id":9,"parent_id":8,"switch_point_lsn":542709383168,
 "start_segment":"00000009000000D400000081","end_segment":"00000009000000D600000080","segments_count":504,
 "missing_segments":["00000009000000D400000089","00000009000000D400000088","00000009000000D400000087",
  "00000009000000D400000086","00000009000000D400000085","00000009000000D400000084",
  "00000009000000D400000083","00000009000000D400000082"],
 "backups":[
  {"backup_name":"base_00000009000000D600000012","wal_file_name":"00000009000000D600000012","start_time":"2026-10-21T00:40:01.674618Z","start_lsn":919424991272},
  {"backup_name":"base_00000009000000D4000000F2","wal_file_name":"00000009000000D4000000F2","start_time":"2026-10-20T00:40:00.697261Z","start_lsn":914593153120},
  {"backup_name":"base_00000009000000D40000008C","wal_file_name":"00000009000000D40000008C","start_time":"2026-10-19T16:17:49.991379Z","start_lsn":912881877032}],
 "segment_range_size":512,"status":"LOST_SEGMENTS"}]`

func parseWalShow(raw string) []walg.WALTimelineInfo {
	var timelines []walg.WALTimelineInfo
	Expect(json.Unmarshal([]byte(raw), &timelines)).To(Succeed())
	return timelines
}

// backupsOf returns the backups wal-show lists, standing in for wal-g backup-list --detail.
func backupsOf(timelines []walg.WALTimelineInfo) []walg.BackupMetadata {
	var backups []walg.BackupMetadata
	for i := range timelines {
		backups = append(backups, timelines[i].Backups...)
	}
	return backups
}

var _ = Describe("evaluateWALIntegrity", func() {
	It("ignores the gap below the earliest backup after an archive switch", func() {
		timelines := parseWalShow(walShowAfterArchiveSwitch)
		result := evaluateWALIntegrity(timelines, backupsOf(timelines))
		Expect(result.AllTimelinesOK).To(BeTrue())
		Expect(result.HasMissingSegments).To(BeFalse())
		Expect(result.EarliestBackup).To(Equal("base_00000009000000D40000008C"))
		Expect(result.IgnoredMissingSegments).To(HaveLen(8))
		Expect(result.IgnoredMissingSegments[0]).To(Equal("00000009000000D400000082"))
		Expect(result.IgnoredMissingSegments[7]).To(Equal("00000009000000D400000089"))
	})

	It("still fails on a genuine gap above the earliest backup", func() {
		timelines := parseWalShow(walShowAfterArchiveSwitch)
		// D5/10 lies between the second and the third backup: PITR across it is broken.
		timelines[0].MissingSegments = append(timelines[0].MissingSegments, "00000009000000D500000010")
		result := evaluateWALIntegrity(timelines, backupsOf(timelines))
		Expect(result.HasMissingSegments).To(BeTrue())
		Expect(result.AllTimelinesOK).To(BeFalse())
		Expect(result.IgnoredMissingSegments).To(HaveLen(8))
	})

	It("counts the earliest backup's own start segment", func() {
		timelines := parseWalShow(walShowAfterArchiveSwitch)
		timelines[0].MissingSegments = []string{"00000009000000D40000008C"}
		result := evaluateWALIntegrity(timelines, backupsOf(timelines))
		Expect(result.HasMissingSegments).To(BeTrue())
		Expect(result.AllTimelinesOK).To(BeFalse())
	})

	It("keeps the old behaviour when there are no backups", func() {
		timelines := parseWalShow(walShowAfterArchiveSwitch)
		result := evaluateWALIntegrity(timelines, nil)
		Expect(result.HasMissingSegments).To(BeTrue())
		Expect(result.AllTimelinesOK).To(BeFalse())
		Expect(result.IgnoredMissingSegments).To(BeEmpty())
	})

	It("ignores backups without a usable start segment", func() {
		timelines := parseWalShow(walShowAfterArchiveSwitch)
		result := evaluateWALIntegrity(timelines, []walg.BackupMetadata{{BackupName: "broken", WalFileName: "xyz"}})
		Expect(result.HasMissingSegments).To(BeTrue())
	})

	It("compares positions across timelines", func() {
		// A gap on the parent timeline below a backup taken on the child timeline.
		timelines := []walg.WALTimelineInfo{
			{ID: 8, Status: walg.TimelineStatusLostSegments, MissingSegments: []string{"0000000800000010000000FE"}},
			{ID: 9, ParentID: 8, Status: walg.TimelineStatusOK},
		}
		backups := []walg.BackupMetadata{{BackupName: "base_000000090000001100000002", WalFileName: "000000090000001100000002"}}
		result := evaluateWALIntegrity(timelines, backups)
		Expect(result.AllTimelinesOK).To(BeTrue())
		Expect(result.HasMissingSegments).To(BeFalse())
	})

	It("does not excuse a non-OK status other than lost segments", func() {
		timelines := []walg.WALTimelineInfo{{ID: 1, Status: "SOMETHING_ELSE"}}
		backups := []walg.BackupMetadata{{BackupName: "b", WalFileName: "000000010000000000000010"}}
		Expect(evaluateWALIntegrity(timelines, backups).AllTimelinesOK).To(BeFalse())
	})

	It("passes a healthy prefix", func() {
		timelines := []walg.WALTimelineInfo{{ID: 1, Status: walg.TimelineStatusOK}}
		result := evaluateWALIntegrity(timelines, nil)
		Expect(result.AllTimelinesOK).To(BeTrue())
		Expect(result.HasMissingSegments).To(BeFalse())
	})
})

var _ = Describe("walSegmentPosition", func() {
	It("orders by log id, then segment", func() {
		a, okA := walSegmentPosition("00000009000000D400000089")
		b, okB := walSegmentPosition("00000001000000D40000008C")
		c, okC := walSegmentPosition("00000009000000D500000000")
		Expect(okA && okB && okC).To(BeTrue())
		Expect(a).To(BeNumerically("<", b))
		Expect(b).To(BeNumerically("<", c))
	})
	It("rejects other names", func() {
		for _, name := range []string{"", "00000009.history", "00000009000000D400000089.partial", "00000009000000ZZ00000089"} {
			_, ok := walSegmentPosition(name)
			Expect(ok).To(BeFalse(), name)
		}
	})
})
