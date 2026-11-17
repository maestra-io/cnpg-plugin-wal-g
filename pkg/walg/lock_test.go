package walg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// fakeWalg puts a `wal-g` script first on PATH. It appends its arguments to a log
// and answers each subcommand ("backup-mark", "delete target", ...) from answers:
// "<exit code>|<stderr>". Returns a function reading the log.
func fakeWalg(t *testing.T, answers map[string]string) func() []string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	script := "#!/bin/sh\necho \"$*\" >> " + logPath + "\ncase \"$1 $2\" in\n"
	for sub, answer := range answers {
		code, stderr, _ := strings.Cut(answer, "|")
		script += "  \"" + sub + "\"*) echo '" + stderr + "' >&2; exit " + code + " ;;\n"
	}
	script += "esac\nexit 0\n"
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

func runDelete(t *testing.T) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := NewClient(&Config{}).DeleteBackup(ctx, "base_000000010000000000000004")
	return err
}

func TestDeleteBackup_RetainedByLockIsNotADeletion(t *testing.T) {
	calls := fakeWalg(t, map[string]string{
		// wal-g with the lock patch: per-key refusals logged, exit 0
		"delete target": "0|WARNING: Objects retained by bucket lock, not deleted: count=3 of 3",
	})
	err := runDelete(t)
	if !errors.Is(err, ErrRetainedByLock) {
		t.Fatalf("want ErrRetainedByLock, got %v", err)
	}
	for _, c := range calls() {
		if strings.HasPrefix(c, "delete garbage") {
			t.Fatalf("garbage collection must not run while the backup is retained: %v", calls())
		}
	}
}

func TestDeleteBackup_UnmarkRefusedByLock(t *testing.T) {
	calls := fakeWalg(t, map[string]string{
		"backup-mark -i": "1|ERROR: Failed to mark backups: operation error S3: PutObject, StatusCode: 409, api error ObjectLockedByBucketPolicy",
	})
	err := runDelete(t)
	if !errors.Is(err, ErrRetainedByLock) {
		t.Fatalf("want ErrRetainedByLock, got %v", err)
	}
	if got := calls(); len(got) != 1 {
		t.Fatalf("nothing may run after a refused unmark, got %v", got)
	}
}

func TestDeleteBackup_Deleted(t *testing.T) {
	calls := fakeWalg(t, map[string]string{
		"delete target": "0|INFO: Objects deleted successfully: count=3",
	})
	if err := runDelete(t); err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if got := calls(); len(got) != 3 || !strings.HasPrefix(got[2], "delete garbage") {
		t.Fatalf("want unmark, delete target, delete garbage; got %v", got)
	}
}

func TestDeleteBackup_OtherFailureIsAnError(t *testing.T) {
	fakeWalg(t, map[string]string{"delete target": "1|ERROR: AccessDenied"})
	err := runDelete(t)
	if err == nil || errors.Is(err, ErrRetainedByLock) {
		t.Fatalf("want a plain failure, got %v", err)
	}
}

func TestStorageCheckClientUsesItsOwnFolder(t *testing.T) {
	config := newConfigFromBackupConfig(testBackupConfigWithS3Prefix("s3://bucket/walg/db-x/"), StorageCheckFolder)
	if config.WaleS3Prefix != "s3://bucket/walg/db-x/storage-check" {
		t.Fatalf("got %q", config.WaleS3Prefix)
	}
	archive := NewConfigFromBackupConfig(testBackupConfigWithS3Prefix("s3://bucket/walg/db-x/"), 18)
	if archive.WaleS3Prefix != "s3://bucket/walg/db-x/18" {
		t.Fatalf("archive prefix changed: %q", archive.WaleS3Prefix)
	}
}

func testBackupConfigWithS3Prefix(prefix string) *v1beta1.BackupConfigWithSecrets {
	bc := &v1beta1.BackupConfigWithSecrets{}
	bc.Spec.Storage.StorageType = v1beta1.StorageTypeS3
	bc.Spec.Storage.S3 = &v1beta1.S3StorageConfigWithSecrets{ResolvedPrefix: prefix}
	return bc
}
