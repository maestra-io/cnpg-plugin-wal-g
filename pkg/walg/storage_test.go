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

package walg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StorageCheckError", func() {
	runErr := errors.New("subprocess run cmd result signal: killed")

	It("maps a kill by the check's own deadline to 'timed out after <budget>'", func() {
		err := StorageCheckError("write", 60*time.Second, runErr, context.DeadlineExceeded, nil)
		Expect(err).To(MatchError("storage write check timed out after 60s: wal-g st check exceeded its deadline"))
		Expect(errors.Is(err, ErrStorageCheckTimeout)).To(BeTrue())
		Expect(err.Error()).NotTo(ContainSubstring("signal: killed"))
	})

	It("renders a fractional budget in seconds", func() {
		err := StorageCheckError("read", 1500*time.Millisecond, runErr, context.DeadlineExceeded, nil)
		Expect(err.Error()).To(HavePrefix("storage read check timed out after 1.5s"))
	})

	It("keeps the raw error when the parent context ended (reconcile timeout / shutdown)", func() {
		err := StorageCheckError("read", 60*time.Second, runErr, context.DeadlineExceeded, context.DeadlineExceeded)
		Expect(err).To(MatchError("storage read check failed: subprocess run cmd result signal: killed"))
		Expect(errors.Is(err, ErrStorageCheckTimeout)).To(BeFalse())
	})

	It("keeps the raw error of a check that failed on its own", func() {
		exitErr := errors.New("subprocess run cmd result exit status 1")
		err := StorageCheckError("write", 60*time.Second, exitErr, nil, nil)
		Expect(err).To(MatchError("storage write check failed: subprocess run cmd result exit status 1"))
		Expect(errors.Is(err, exitErr)).To(BeTrue())
	})
})

var _ = Describe("Client.StorageCheckWritable", func() {
	It("reports 'timed out after <budget>' when wal-g hangs past the budget", func() {
		// A wal-g on PATH that never answers; exec so the deadline's SIGKILL hits it directly.
		dir := GinkgoT().TempDir()
		script := filepath.Join(dir, "wal-g")
		Expect(os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 300\n"), 0o755)).To(Succeed())
		GinkgoT().Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

		config := NewConfigWithDefaults()
		client := NewClient(&config)
		start := time.Now()
		_, err := client.StorageCheckWritable(ctx, 200*time.Millisecond)
		Expect(err).To(MatchError(ErrStorageCheckTimeout))
		Expect(err.Error()).To(HavePrefix("storage write check timed out after 0.2s"))
		Expect(err.Error()).NotTo(ContainSubstring("signal: killed"))
		// Bounded by the budget, not by the 300 s child.
		Expect(time.Since(start)).To(BeNumerically("<", 30*time.Second))
	})
})
