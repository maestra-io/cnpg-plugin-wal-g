package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// A Backup deleted while the R2 bucket lock still covers its objects (the walgPermanent
// lock28-*/sc10-* verification Backups) was retried every hour until the lock expired:
// ~1,500 refusals a day on omega, each logged as an error (issues-maestra#1786, 21-23.11.2026).
// The lock counts from each object's last write, so the retry belongs at that write plus
// the lock age of the bucket's R2BucketLock.

// fakeWalgLocked refuses every deletion with the bucket lock and lists the Backup's
// objects with metadata.json written at metadataWritten.
func fakeWalgLocked(t *testing.T, metadataWritten time.Time) {
	t.Helper()
	dir := t.TempDir()
	stamp := metadataWritten.UTC().Format("2006-01-02 15:04:05.000 -0700 MST")
	root := filepath.Join(dir, "root.out")
	sub := filepath.Join(dir, "sub.out")
	for file, out := range map[string]string{
		root: "obj  412 " + metadataWritten.Add(-time.Hour).UTC().Format("2006-01-02 15:04:05.000 -0700 MST") + " " + thisBackup + "_backup_stop_sentinel.json\n",
		sub:  "obj  901 " + stamp + " metadata.json\n",
	} {
		if err := os.WriteFile(file, []byte(out), 0644); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/bin/sh\ncase \"$1 $2\" in\n" +
		"  \"delete target\") echo 'WARNING: Objects retained by bucket lock, not deleted: count=3 of 3' >&2; exit 0 ;;\n" +
		"  \"st ls\") case \"$3\" in\n" +
		"    basebackups_005/) /bin/cat " + root + " ;;\n" +
		"    basebackups_005/" + thisBackup + "/) /bin/cat " + sub + " ;;\n" +
		"    *) exit 1 ;;\n  esac ;;\nesac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)
}

func r2BucketLock(bucket string, rules ...map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(r2BucketLockGVK)
	u.SetNamespace("ns")
	u.SetName(bucket + "-0123")
	ruleList := make([]any, 0, len(rules))
	for _, r := range rules {
		ruleList = append(ruleList, r)
	}
	u.Object["spec"] = map[string]any{"forProvider": map[string]any{"bucketName": bucket, "rules": ruleList}}
	return u
}

func ageRule(days int64) map[string]any {
	return map[string]any{"id": "backups-age", "enabled": true, "condition": map[string]any{"ageDays": days}}
}

func writableStorageBackup(t *testing.T, locks ...*unstructured.Unstructured) (*BackupDeletionController, client.Client) {
	t.Helper()
	dc, c := deletingBackup(t, nil, nil, true)
	// the storage-test BackupConfig reports the storage as not writable; this one is fine
	bc := &v1beta1.BackupConfig{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "bc"}, bc); err != nil {
		t.Fatal(err)
	}
	bc.Status.Conditions = nil
	if err := c.Status().Update(context.Background(), bc); err != nil {
		t.Fatal(err)
	}
	c.Scheme().AddKnownTypeWithName(r2BucketLockGVK, &unstructured.Unstructured{})
	c.Scheme().AddKnownTypeWithName(r2BucketLockGVK.GroupVersion().WithKind("R2BucketLockList"), &unstructured.UnstructuredList{})
	for _, l := range locks {
		if err := c.Create(context.Background(), l); err != nil {
			t.Fatal(err)
		}
	}
	return dc, c
}

func retainedUntil(t *testing.T, c client.Client) (string, bool) {
	t.Helper()
	backup := &cnpgv1.Backup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}, backup); err != nil {
		t.Fatal(err)
	}
	if _, ok := backup.Annotations[v1beta1.BackupRetainedByLockAnnotationName]; !ok {
		t.Fatalf("the refusal is not marked: %v", backup.Annotations)
	}
	until, ok := backup.Annotations[v1beta1.BackupRetainedUntilAnnotationName]
	return until, ok
}

func TestDeleteWALGBackup_LockRefusalRetriesAtTheLockExpiry(t *testing.T) {
	written := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	fakeWalgLocked(t, written)
	otherPrefix := ageRule(2190)
	otherPrefix["prefix"] = "archive/" // another part of the same bucket
	ownPrefix := ageRule(28)
	ownPrefix["prefix"] = "test-prefix/"
	dc, c := writableStorageBackup(t, r2BucketLock("other-bucket", ageRule(2190)), r2BucketLock("test-bucket", otherPrefix, ownPrefix))
	_ = deleteB(t, dc)

	until, ok := retainedUntil(t, c)
	if !ok {
		t.Fatal("no lock expiry recorded")
	}
	got, err := time.Parse(time.RFC3339, until)
	want := written.Add(28*24*time.Hour + lockExpiryMargin)
	if err != nil || !got.Equal(want) {
		t.Fatalf("retained until %q, want %v", until, want.UTC())
	}
}

func TestDeleteWALGBackup_LockRefusalWithoutAKnownLockAgeBacksOff(t *testing.T) {
	fakeWalgLocked(t, time.Now().Add(-24*time.Hour))
	for name, locks := range map[string][]*unstructured.Unstructured{
		"no R2BucketLock":      nil,
		"another bucket":       {r2BucketLock("other-bucket", ageRule(28))},
		"not an age condition": {r2BucketLock("test-bucket", ageRule(28), map[string]any{"enabled": true, "condition": map[string]any{"date": "2027-01-01"}})},
	} {
		t.Run(name, func(t *testing.T) {
			dc, c := writableStorageBackup(t, locks...)
			_ = deleteB(t, dc)
			if until, ok := retainedUntil(t, c); ok {
				t.Fatalf("no lock age to count from, got retained until %q", until)
			}
		})
	}
}

func TestWaitForLockExpiry(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	for until, want := range map[string]time.Duration{
		"2026-12-19T08:10:01Z": 25*24*time.Hour + 20*time.Hour + 10*time.Minute + time.Second,
		"2026-11-23T11:00:00Z": 0, // expired: retry now
		"garbage":              0,
	} {
		backup := &cnpgv1.Backup{}
		backup.Annotations = map[string]string{v1beta1.BackupRetainedUntilAnnotationName: until}
		got, pending := waitForLockExpiry(backup, now)
		if pending != (want > 0) || got != want {
			t.Errorf("until %q: got %v, %v; want %v", until, got, pending, want)
		}
	}

	// asked to skip the storage cleanup: released now, not at the lock expiry
	backup := &cnpgv1.Backup{}
	backup.Annotations = map[string]string{
		v1beta1.BackupRetainedUntilAnnotationName: "2026-12-19T08:10:01Z",
		v1beta1.SkipStorageCleanupAnnotationName:  "true",
	}
	if _, pending := waitForLockExpiry(backup, now); pending {
		t.Error("a Backup annotated to skip the storage cleanup must not wait for the lock")
	}
}
