package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// Backups written before a BackupConfig switched to libsodium stay in the plain archive
// <bucket>/walg/<name>; the BackupConfig now points at <bucket>/walg-enc/<name>. Retention
// of such a Backup ran `backup-mark -i` (an error logged on every try) and `delete target`
// against walg-enc, which only answered "does not exist" (issues-maestra#1786, omega
// llm-internal-proxy 20.11.2026). The plain archive is expired by the bucket lifecycle rule
// walg-plain-expiry; the Backup is released without a deletion attempt.

const thisBackup = "base_00000003000000000000002C"

// fakeWalgEncrypted puts a `wal-g` script first on PATH whose storage holds none of the
// backups asked about. It returns every call.
func fakeWalgEncrypted(t *testing.T) func() []string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$*\" >> " + logPath + "\n" +
		"case \"$1\" in\n" +
		"  backup-mark) echo \"ERROR: Failed to get previous backups: object 'walg-enc/db/17/basebackups_005/$3/metadata.json' not found in storage\" >&2; exit 1 ;;\n" +
		"  delete) [ \"$2\" = target ] && { echo \"ERROR: Backup '$3' does not exist.\" >&2; exit 1; } ;;\n" +
		"esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)

	return func() []string {
		data, _ := os.ReadFile(logPath)
		return strings.Split(strings.TrimSpace(string(data)), "\n")
	}
}

func deleteB(t *testing.T, dc *BackupDeletionController) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return dc.deleteWALGBackup(ctx, client.ObjectKey{Namespace: "ns", Name: "b"})
}

func TestDeleteWALGBackup_BackupNotInStorageIsReleasedWithoutADeletion(t *testing.T) {
	calls := fakeWalgEncrypted(t)
	dc, c := writableStorageBackup(t)
	if err := deleteB(t, dc); err != nil {
		t.Fatalf("deleteWALGBackup: %v", err)
	}
	if kept, _ := finalizerKept(t, c); kept {
		t.Fatal("nothing to delete in this storage: the finalizer must go")
	}
	for _, call := range calls() {
		if strings.HasPrefix(call, "delete") {
			t.Fatalf("no deletion in a storage that does not hold the backup, got %q", call)
		}
	}
}

// The deletion queue of a BackupConfig is a goroutine started by the first Backup that
// needed it. It kept that reconcile's logger, so every later deletion of the queue was
// logged with the first Backup's name and reconcileID: omega logged "backup-mark" errors
// for Backups deleted hours before (issues-maestra#1786, 20.11.2026).
func TestDeletionQueue_LogsTheBackupItProcesses(t *testing.T) {
	fakeWalgOnPath(t, "1|ERROR: operation error S3: ListObjectsV2, StatusCode: 503")
	dc, _ := deletingBackup(t, nil, nil, true)

	var mu sync.Mutex
	var out bytes.Buffer
	sink := funcr.NewJSON(func(obj string) {
		mu.Lock()
		defer mu.Unlock()
		out.WriteString(obj + "\n")
	}, funcr.Options{})
	dc.logger = sink

	backup := &cnpgv1.Backup{}
	if err := dc.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}, backup); err != nil {
		t.Fatal(err)
	}
	// the reconcile of another Backup creates the queue
	enqueueCtx := logr.NewContext(context.Background(),
		sink.WithValues("Backup", map[string]string{"name": "first", "namespace": "ns"}, "reconcileID", "rid-first"))
	if err := dc.EnqueueBackupDeletion(enqueueCtx, backup); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		logs := out.String()
		mu.Unlock()
		if strings.Contains(logs, "Failed to process deletion request") {
			for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
				// the enqueue line is the reconcile's own; every queue line must not carry it
				if !strings.Contains(line, "Enqueued resource for deletion") && strings.Contains(line, "rid-first") {
					t.Fatalf("the queue logged with the reconcile that created it:\n%s", logs)
				}
				if strings.Contains(line, "Deleting backup from storage") && !strings.Contains(line, `"backup":"ns/b"`) {
					t.Fatalf("a deletion line does not name the Backup it deletes: %s", line)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no deletion processed:\n%s", logs)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
