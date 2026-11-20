package walg

import (
	"context"
	"errors"
	"testing"
	"time"
)

// notFoundStderr is what wal-g backup-mark prints for a backup whose metadata the
// storage does not hold (the plain archive left behind by the switch to walg-enc/,
// issues-maestra#1788). The single quotes are escaped for fakeWalg's echo '...'.
func notFoundStderr(backupName string) string {
	return "ERROR: Failed to get previous backups: object '\\''walg-enc/db-x/18/basebackups_005/" +
		backupName + "/metadata.json'\\'' not found in storage"
}

func runMark(t *testing.T, backupName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return NewClient(&Config{}).MarkBackupPermanent(ctx, backupName)
}

func TestMarkBackupPermanent_BackupNotInStorage(t *testing.T) {
	fakeWalg(t, map[string]string{"backup-mark base_0000000300000043000000A0": "1|" + notFoundStderr("base_0000000300000043000000A0")})
	if err := runMark(t, "base_0000000300000043000000A0"); !errors.Is(err, ErrBackupNotInStorage) {
		t.Fatalf("want ErrBackupNotInStorage, got %v", err)
	}
}

func TestMarkBackupPermanent_OtherBackupMissingIsAnError(t *testing.T) {
	// The base of a delta is missing: the delta itself is in storage, so this is a real failure
	fakeWalg(t, map[string]string{"backup-mark base_000000010000000100000046_D_000000010000000100000040": "1|" + notFoundStderr("base_000000010000000100000040")})
	err := runMark(t, "base_000000010000000100000046_D_000000010000000100000040")
	if err == nil || errors.Is(err, ErrBackupNotInStorage) {
		t.Fatalf("want a plain error, got %v", err)
	}
}

func TestMarkBackupPermanent_StorageErrorIsAnError(t *testing.T) {
	fakeWalg(t, map[string]string{"backup-mark base_0000000300000043000000A0": "1|ERROR: operation error S3: GetObject, StatusCode: 403, api error AccessDenied"})
	err := runMark(t, "base_0000000300000043000000A0")
	if err == nil || errors.Is(err, ErrBackupNotInStorage) {
		t.Fatalf("want a plain error, got %v", err)
	}
}

func TestIsBackupNotInStorage(t *testing.T) {
	stderr := []byte("ERROR: Failed to get previous backups: object 'walg-enc/db-x/18/basebackups_005/base_1/metadata.json' not found in storage")
	cases := map[string]bool{"base_1": true, "base_2": false, "": false, "base_": false}
	for name, want := range cases {
		if got := IsBackupNotInStorage(stderr, name); got != want {
			t.Errorf("IsBackupNotInStorage(%q) = %v, want %v", name, got, want)
		}
	}
}
