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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
)

// fakeStorageChecker answers storage checks from a script of results, in order;
// once the script runs out every call succeeds.
type fakeStorageChecker struct {
	mu       sync.Mutex
	results  []error
	calls    int
	timeouts []time.Duration
	// onCall runs inside each call (e.g. to cancel the reconcile context).
	onCall func(call int)
}

func (f *fakeStorageChecker) next(timeout time.Duration) (*cmd.RunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.timeouts = append(f.timeouts, timeout)
	if f.onCall != nil {
		f.onCall(f.calls)
	}
	if len(f.results) == 0 {
		return nil, nil
	}
	err := f.results[0]
	f.results = f.results[1:]
	return nil, err
}

func (f *fakeStorageChecker) StorageCheckReadable(_ context.Context, timeout time.Duration) (*cmd.RunResult, error) {
	return f.next(timeout)
}

func (f *fakeStorageChecker) StorageCheckWritable(_ context.Context, timeout time.Duration) (*cmd.RunResult, error) {
	return f.next(timeout)
}

// logEntry is one line seen by recordingSink.
type logEntry struct {
	isError bool
	msg     string
}

// recordingSink is a logr.LogSink that keeps every Info/Error message.
type recordingSink struct {
	mu      sync.Mutex
	entries *[]logEntry
}

func (s *recordingSink) Init(logr.RuntimeInfo)               {}
func (s *recordingSink) Enabled(int) bool                    { return true }
func (s *recordingSink) WithValues(...any) logr.LogSink      { return s }
func (s *recordingSink) WithName(string) logr.LogSink        { return s }
func (s *recordingSink) Info(_ int, msg string, _ ...any)    { s.add(false, msg) }
func (s *recordingSink) Error(_ error, msg string, _ ...any) { s.add(true, msg) }
func (s *recordingSink) add(isError bool, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.entries = append(*s.entries, logEntry{isError: isError, msg: msg})
}

func newRecordingLogger() (logr.Logger, *[]logEntry) {
	entries := &[]logEntry{}
	return logr.New(&recordingSink{entries: entries}), entries
}

func errorMessages(entries []logEntry) []string {
	var out []string
	for _, e := range entries {
		if e.isError {
			out = append(out, e.msg)
		}
	}
	return out
}

func infoMessages(entries []logEntry) []string {
	var out []string
	for _, e := range entries {
		if !e.isError {
			out = append(out, e.msg)
		}
	}
	return out
}

var _ = Describe("BackupConfigStatusController storage checks", func() {
	var (
		controller   *BackupConfigStatusController
		backupConfig *v1beta1.BackupConfig
		killed       error
	)

	BeforeEach(func() {
		controller = NewBackupConfigStatusController(nil, 2*time.Minute, 0)
		// The 5 s production backoff is not what is under test.
		controller.storageCheckRetryBackoff = 0
		backupConfig = &v1beta1.BackupConfig{}
		killed = walg.StorageCheckError("write", walg.DefaultStorageCheckTimeout,
			errors.New("signal: killed"), context.DeadlineExceeded, nil)
	})

	It("defaults the per-call budget to 60s", func() {
		Expect(walg.DefaultStorageCheckTimeout).To(Equal(60 * time.Second))
		Expect(controller.storageCheckTimeout).To(Equal(60 * time.Second))
		Expect(NewBackupConfigStatusController(nil, time.Minute, 90*time.Second).storageCheckTimeout).
			To(Equal(90 * time.Second))
		Expect(NewBackupConfigStatusController(nil, time.Minute, 0).storageCheckRetryBackoff).
			To(Equal(5 * time.Second))
	})

	It("passes on the first attempt without a retry", func() {
		checker := &fakeStorageChecker{}
		logger, entries := newRecordingLogger()
		Expect(controller.checkStorageWritable(ctx, backupConfig, checker, logger)).To(BeTrue())
		Expect(checker.calls).To(Equal(1))
		Expect(checker.timeouts).To(Equal([]time.Duration{60 * time.Second}))
		Expect(getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageWritable)).To(Equal(metav1.ConditionTrue))
		Expect(*entries).To(BeEmpty())
	})

	It("fail then succeed: retries once and leaves StorageWritable True, no Error log", func() {
		checker := &fakeStorageChecker{results: []error{killed}}
		logger, entries := newRecordingLogger()
		Expect(controller.checkStorageWritable(ctx, backupConfig, checker, logger)).To(BeTrue())
		Expect(checker.calls).To(Equal(2))
		Expect(checker.timeouts).To(Equal([]time.Duration{60 * time.Second, 60 * time.Second}))
		Expect(getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageWritable)).To(Equal(metav1.ConditionTrue))
		Expect(errorMessages(*entries)).To(BeEmpty())
		Expect(infoMessages(*entries)).To(ContainElement("Storage write check attempt 1/2 failed, retrying"))
		Expect(infoMessages(*entries)).To(ContainElement("Storage write check passed on attempt 2/2"))
	})

	It("fail then succeed on the read check: StorageReadable True", func() {
		checker := &fakeStorageChecker{results: []error{errors.New("storage read check failed: exit status 1")}}
		logger, entries := newRecordingLogger()
		Expect(controller.checkStorageReadable(ctx, backupConfig, checker, logger)).To(BeTrue())
		Expect(checker.calls).To(Equal(2))
		Expect(getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageReadable)).To(Equal(metav1.ConditionTrue))
		Expect(errorMessages(*entries)).To(BeEmpty())
		Expect(infoMessages(*entries)).To(ContainElement("Storage read check attempt 1/2 failed, retrying"))
	})

	It("fail, fail: StorageWritable False with the final error, one Error log", func() {
		final := errors.New("storage write check failed: exit status 1")
		checker := &fakeStorageChecker{results: []error{killed, final}}
		logger, entries := newRecordingLogger()
		Expect(controller.checkStorageWritable(ctx, backupConfig, checker, logger)).To(BeFalse())
		Expect(checker.calls).To(Equal(2))
		Expect(getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageWritable)).To(Equal(metav1.ConditionFalse))
		cond := findCondition(backupConfig, v1beta1.ConditionTypeStorageWritable)
		Expect(cond.Reason).To(Equal("StorageWriteCheckFailed"))
		Expect(cond.Message).To(Equal(final.Error()))
		Expect(errorMessages(*entries)).To(Equal([]string{"Storage write check failed"}))
		Expect(infoMessages(*entries)).To(Equal([]string{"Storage write check attempt 1/2 failed, retrying"}))
	})

	It("fail, fail by timeout: StorageReadable False with 'timed out after 60s', not 'signal: killed'", func() {
		timedOut := walg.StorageCheckError("read", walg.DefaultStorageCheckTimeout,
			errors.New("subprocess run error while waiting cmd signal: killed: context deadline exceeded"),
			context.DeadlineExceeded, nil)
		checker := &fakeStorageChecker{results: []error{timedOut, timedOut}}
		logger, _ := newRecordingLogger()
		Expect(controller.checkStorageReadable(ctx, backupConfig, checker, logger)).To(BeFalse())
		cond := findCondition(backupConfig, v1beta1.ConditionTypeStorageReadable)
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Message).To(HavePrefix("storage read check timed out after 60s"))
		Expect(cond.Message).NotTo(ContainSubstring("signal: killed"))
	})

	It("does not retry once the reconcile context has ended", func() {
		reconcileCtx, cancelReconcile := context.WithCancel(ctx)
		defer cancelReconcile()
		checker := &fakeStorageChecker{
			results: []error{fmt.Errorf("storage write check failed: %w", context.Canceled)},
			onCall:  func(int) { cancelReconcile() },
		}
		logger, _ := newRecordingLogger()
		Expect(controller.checkStorageWritable(reconcileCtx, backupConfig, checker, logger)).To(BeFalse())
		Expect(checker.calls).To(Equal(1))
		Expect(getConditionStatus(backupConfig, v1beta1.ConditionTypeStorageWritable)).To(Equal(metav1.ConditionFalse))
	})

	It("the retry backoff returns at once when the reconcile context has ended", func() {
		ended, cancelEnded := context.WithCancel(ctx)
		cancelEnded()
		Expect(sleepCtx(ended, time.Hour)).To(BeFalse())
		Expect(sleepCtx(ctx, 0)).To(BeTrue())
	})
})

// findCondition returns the condition of the given type, failing the test if absent.
func findCondition(backupConfig *v1beta1.BackupConfig, conditionType string) metav1.Condition {
	for _, c := range backupConfig.Status.Conditions {
		if c.Type == conditionType {
			return c
		}
	}
	Fail("condition " + conditionType + " not set")
	return metav1.Condition{}
}
