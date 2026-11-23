package walg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// A backup written before its BackupConfig switched to libsodium (and moved from
// <bucket>/walg/<name> to <bucket>/walg-enc/<name>) is not in the storage the client points
// at (issues-maestra#1786, omega llm-internal-proxy 20.11.2026).
func TestDeleteBackup_BackupNotInStorage(t *testing.T) {
	// Not in this storage: "delete target" would answer "does not exist" and the
	// deletion would be claimed while the data stays in the plain archive.
	calls := fakeWalg(t, map[string]string{"backup-mark -i": "1|" + notFoundStderr("base_000000010000000000000004")})
	if err := runDelete(t); !errors.Is(err, ErrBackupNotInStorage) {
		t.Fatalf("want ErrBackupNotInStorage, got %v", err)
	}
	for _, call := range calls() {
		if strings.HasPrefix(call, "delete") {
			t.Fatalf("nothing may be deleted in a storage that does not hold the backup, got %q", call)
		}
	}
}

func TestDeleteBackup_OtherBackupMissingStillDeletes(t *testing.T) {
	// The base of a delta is missing: the delta itself is here and is deleted
	calls := fakeWalg(t, map[string]string{"backup-mark -i": "1|" + notFoundStderr("base_000000010000000000000001")})
	if err := runDelete(t); err != nil {
		t.Fatalf("want a deletion, got %v", err)
	}
	if !slices.ContainsFunc(calls(), func(c string) bool { return strings.HasPrefix(c, "delete target") }) {
		t.Fatalf("delete target not called: %v", calls())
	}
}

func TestGetBackupsList_EmptyStorage(t *testing.T) {
	// wal-g prints nothing on stdout when the storage holds no backups
	fakeWalg(t, map[string]string{"backup-list --detail": "0|INFO: 2026/11/23 12:00:00.000000 No backups found"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	list, err := NewClient(&Config{}).GetBackupsList(ctx)
	if err != nil || len(list) != 0 {
		t.Fatalf("want an empty list, got %v, %v", list, err)
	}
}

// fakeWalgLs serves `wal-g st ls <path>` from listings (path -> stdout) and fails any
// other path the way wal-g does for a folder it cannot list.
func fakeWalgLs(t *testing.T, listings map[string]string) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\n[ \"$1 $2\" = \"st ls\" ] || exit 0\ncase \"$3\" in\n"
	for path, out := range listings {
		file := filepath.Join(dir, strings.ReplaceAll(path, "/", "_")+".out")
		if err := os.WriteFile(file, []byte(out), 0644); err != nil {
			t.Fatal(err)
		}
		script += "  \"" + path + "\") /bin/cat " + file + "; exit 0 ;;\n"
	}
	script += "esac\necho 'ERROR: no such folder' >&2; exit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)
}

// A bucket lock counts from each object's last write: the newest object of the backup
// (its metadata.json, rewritten by backup-mark) decides when it can go.
func TestBackupLastModified(t *testing.T) {
	fakeWalgLs(t, map[string]string{
		"basebackups_005/": "dir  0 0001-01-01 00:00:00 +0000 UTC base_000000010000000000000004\n" +
			"obj  412 2026-11-20 21:05:20.120 +0000 UTC base_000000010000000000000004_backup_stop_sentinel.json\n" +
			"obj  415 2026-11-22 21:05:20.000 +0000 UTC base_000000010000000000000009_backup_stop_sentinel.json\n",
		"basebackups_005/base_000000010000000000000004/": "dir  0 0001-01-01 00:00:00 +0000 UTC tar_partitions\n" +
			"obj  901 2026-11-21 08:00:01.5 +0000 UTC metadata.json\n",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := NewClient(&Config{}).BackupLastModified(ctx, "base_000000010000000000000004")
	want := time.Date(2026, 11, 21, 8, 0, 1, 500_000_000, time.UTC)
	if err != nil || !got.Equal(want) {
		t.Fatalf("got %v, %v; want %v", got, err, want)
	}
	if _, err := NewClient(&Config{}).BackupLastModified(ctx, "base_00000001000000000000000F"); err == nil {
		t.Fatal("a backup with no objects has no last write: want an error")
	}
}
