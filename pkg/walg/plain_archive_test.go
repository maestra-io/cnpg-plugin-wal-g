package walg

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// The switch to libsodium moved every BackupConfig from <bucket>/walg/<name> to
// <bucket>/walg-enc/<name>; the backups written before it stay in the plain archive
// (issues-maestra#1786, omega llm-internal-proxy 20.11.2026).
func TestPlainArchivePrefix(t *testing.T) {
	cases := map[string]string{
		"s3://db-x-backups/walg-enc/db-x":  "s3://db-x-backups/walg/db-x",
		"s3://db-x-backups/walg-enc/db-x/": "s3://db-x-backups/walg/db-x/",
		"s3://b/team/walg-enc/db-x":        "s3://b/team/walg/db-x",
		"gs://b/walg-enc/db-x":             "gs://b/walg/db-x",
		"s3://db-x-backups/walg/db-x":      "",
		"s3://walg-enc/db-x":               "", // a bucket named walg-enc is not the encrypted folder
		"s3://b/walg-encrypted/db-x":       "",
		"":                                 "",
	}
	for prefix, want := range cases {
		got, ok := PlainArchivePrefix(prefix)
		if ok != (want != "") || got != want {
			t.Errorf("PlainArchivePrefix(%q) = %q, %v; want %q", prefix, got, ok, want)
		}
	}
}

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

func runDeleteBackups(t *testing.T, backups []BackupMetadata, emptied bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := NewClient(&Config{}).DeleteBackupsThenGarbage(ctx, backups, emptied)
	return err
}

func TestDeleteBackupsThenGarbage(t *testing.T) {
	backups := []BackupMetadata{
		{BackupName: "base_000000010000000000000004"},
		{BackupName: "base_000000010000000000000008", IsPermanent: true},
	}
	for emptied, garbage := range map[bool]string{
		false: "delete garbage --confirm",
		true:  "delete garbage --without-backup-check --confirm",
	} {
		calls := fakeWalg(t, nil)
		if err := runDeleteBackups(t, backups, emptied); err != nil {
			t.Fatalf("emptied=%v: %v", emptied, err)
		}
		want := []string{
			"delete target base_000000010000000000000004 --confirm",
			"backup-mark -i base_000000010000000000000008",
			"delete target base_000000010000000000000008 --confirm",
			garbage,
		}
		if got := calls(); !slices.Equal(got, want) {
			t.Fatalf("emptied=%v: calls\n%v\nwant\n%v", emptied, got, want)
		}
	}
}

func TestDeleteBackupsThenGarbage_StopsAtTheLock(t *testing.T) {
	calls := fakeWalg(t, map[string]string{
		"delete target": "0|WARNING: Objects retained by bucket lock, not deleted: count=3 of 3",
	})
	err := runDeleteBackups(t, []BackupMetadata{
		{BackupName: "base_000000010000000000000004"}, {BackupName: "base_000000010000000000000008"},
	}, true)
	if !errors.Is(err, ErrRetainedByLock) {
		t.Fatalf("want ErrRetainedByLock, got %v", err)
	}
	if got := calls(); len(got) != 1 {
		t.Fatalf("nothing after the first refusal (no garbage either), got %v", got)
	}
}

func TestDeleteBackupsThenGarbage_StorageErrorIsAnError(t *testing.T) {
	fakeWalg(t, map[string]string{"delete target": "1|ERROR: operation error S3: ListObjectsV2, StatusCode: 503"})
	err := runDeleteBackups(t, []BackupMetadata{{BackupName: "base_000000010000000000000004"}}, false)
	if err == nil || errors.Is(err, ErrRetainedByLock) {
		t.Fatalf("want a plain error, got %v", err)
	}
}
