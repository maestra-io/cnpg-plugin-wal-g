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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// WALTimelineInfo represents a single timeline entry returned by `wal-g wal-show --detailed-json`
type WALTimelineInfo struct {
	ID               int              `json:"id"`
	ParentID         int              `json:"parent_id"`
	SwitchPointLSN   uint64           `json:"switch_point_lsn"`
	StartSegment     string           `json:"start_segment"`
	EndSegment       string           `json:"end_segment"`
	SegmentsCount    int              `json:"segments_count"`
	MissingSegments  []string         `json:"missing_segments"`
	Backups          []BackupMetadata `json:"backups"`
	SegmentRangeSize int              `json:"segment_range_size"`
	Status           string           `json:"status"`
}

// Timeline statuses of `wal-g wal-show`.
const (
	TimelineStatusOK           = "OK"
	TimelineStatusLostSegments = "LOST_SEGMENTS"
)

// HasMissingSegments returns true if there are any missing WAL segments in this timeline
func (w *WALTimelineInfo) HasMissingSegments() bool {
	return len(w.MissingSegments) > 0
}

// IsOK returns true if the timeline status is "OK"
func (w *WALTimelineInfo) IsOK() bool {
	return w.Status == TimelineStatusOK
}

// DefaultStorageCheckTimeout is the default budget of a single `wal-g st check`
// call. A storage latency blip (seen on Cloudflare R2) can stretch a healthy
// check well past 15 s; 60 s keeps such a blip from failing the check.
const DefaultStorageCheckTimeout = 60 * time.Second

// StorageCheckReadable verifies that the configured storage is accessible for reading
// by running `wal-g st check read`, bounded by timeout (DefaultStorageCheckTimeout if <= 0).
func (c *Client) StorageCheckReadable(ctx context.Context, timeout time.Duration) (*cmd.RunResult, error) {
	return c.storageCheck(ctx, "read", timeout)
}

// StorageCheckWritable verifies that the configured storage is accessible for writing
// by running `wal-g st check write`, bounded by timeout (DefaultStorageCheckTimeout if <= 0).
func (c *Client) StorageCheckWritable(ctx context.Context, timeout time.Duration) (*cmd.RunResult, error) {
	return c.storageCheck(ctx, "write", timeout)
}

// storageCheck runs `wal-g st check <mode>`. It does not log at Error level:
// the caller decides whether a failure is final (it may retry).
func (c *Client) storageCheck(ctx context.Context, mode string, timeout time.Duration) (*cmd.RunResult, error) {
	logger := logr.FromContextOrDiscard(ctx)
	if timeout <= 0 {
		timeout = DefaultStorageCheckTimeout
	}

	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := cmd.New("wal-g", "st", "check", mode).
		WithContext(commandCtx).
		WithEnv(c.config.ToEnvMap()).
		Run()

	if err != nil {
		err = StorageCheckError(mode, timeout, err, commandCtx.Err(), ctx.Err())
		logger.V(1).Info(
			fmt.Sprintf("'wal-g st check %s' failed", mode), "error", err.Error(),
			"stdout", string(result.Stdout()), "stderr", string(result.Stderr()),
		)
		return result, err
	}
	return result, nil
}

// StorageCheckError builds the error of a failed `wal-g st check <mode>`.
// When the check's own deadline killed wal-g (commandCtxErr is DeadlineExceeded
// while the parent context is still live), the raw error is "signal: killed" /
// "context deadline exceeded"; it is replaced by "timed out after <timeout>".
func StorageCheckError(mode string, timeout time.Duration, runErr, commandCtxErr, parentCtxErr error) error {
	if errors.Is(commandCtxErr, context.DeadlineExceeded) && parentCtxErr == nil {
		return fmt.Errorf("storage %s check timed out after %s: %w", mode, formatSeconds(timeout), ErrStorageCheckTimeout)
	}
	return fmt.Errorf("storage %s check failed: %w", mode, runErr)
}

// ErrStorageCheckTimeout marks a storage check killed by its own deadline.
var ErrStorageCheckTimeout = errors.New("wal-g st check exceeded its deadline")

// formatSeconds renders a duration as whole or fractional seconds ("60s", "1.5s").
func formatSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

// WALShow retrieves detailed WAL timeline information by running `wal-g wal-show --detailed-json`.
// Returns a list of WALTimelineInfo entries, each representing a timeline with its segments,
// backups, and integrity status.
func (c *Client) WALShow(ctx context.Context) ([]WALTimelineInfo, error) {
	logger := logr.FromContextOrDiscard(ctx)

	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result, err := cmd.New("wal-g", "wal-show", "--detailed-json").
		WithContext(commandCtx).
		WithEnv(c.config.ToEnvMap()).
		Run()

	if err != nil {
		logger.Error(
			err, "Error while 'wal-g wal-show --detailed-json'",
			"stdout", string(result.Stdout()), "stderr", string(result.Stderr()),
		)
		return nil, fmt.Errorf("wal-show failed: %w", err)
	}

	var timelines []WALTimelineInfo
	if err := json.Unmarshal(result.Stdout(), &timelines); err != nil {
		logger.Error(
			err, "Cannot unmarshal wal-g wal-show output",
			"stdout", string(result.Stdout()),
			"stderr", string(result.Stderr()),
		)
		return nil, fmt.Errorf("cannot unmarshal wal-g wal-show output: %w", err)
	}

	return timelines, nil
}

// StorageLsTotalSize runs `wal-g st ls <path>` and sums up the sizes of all listed objects.
// The output format is: "obj  <size> <date> <time> <timezone> <filename>"
// Returns total size in bytes.
func (c *Client) StorageLsTotalSize(ctx context.Context, path string) (int64, error) {
	logger := logr.FromContextOrDiscard(ctx)

	commandCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result, err := cmd.New("wal-g", "st", "ls", path).
		WithContext(commandCtx).
		WithEnv(c.config.ToEnvMap()).
		Run()

	if err != nil {
		logger.Error(
			err, fmt.Sprintf("Error while 'wal-g st ls %s'", path),
			"stdout", string(result.Stdout()), "stderr", string(result.Stderr()),
		)
		return 0, fmt.Errorf("storage ls failed for path %s: %w", path, err)
	}

	return parseStorageLsOutput(result.Stdout())
}

// parseStorageLsOutput parses the output of `wal-g st ls` and sums up file sizes.
// Each line has the format: "obj  <size> <date> <time> <timezone> <filename>"
// Example: "obj  5327891 2026-04-27 12:18:55.841 +0000 UTC 000000020000000A00000034.br"
func parseStorageLsOutput(output []byte) (int64, error) {
	var totalSize int64
	scanner := bufio.NewScanner(bytes.NewReader(output))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Split by whitespace; first field is "obj", second is size
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		// Skip non-object lines (e.g. directory listings)
		if fields[0] != "obj" {
			continue
		}

		size, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue // Skip lines with unparseable sizes
		}

		totalSize += size
	}

	if err := scanner.Err(); err != nil {
		return totalSize, fmt.Errorf("error reading storage ls output: %w", err)
	}

	return totalSize, nil
}
