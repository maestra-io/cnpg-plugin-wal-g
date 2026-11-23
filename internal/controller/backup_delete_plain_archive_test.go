package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// Backups written before a BackupConfig switched to libsodium stay in the plain archive
// <bucket>/walg/<name>; the BackupConfig now points at <bucket>/walg-enc/<name>. Retention
// of such a Backup ran `backup-mark -i` and `delete target` against walg-enc, got "not
// found" / "does not exist", removed the finalizer and left the data in walg/ for good
// (issues-maestra#1786, omega llm-internal-proxy 20.11.2026).

const (
	encPrefix   = "s3://test-bucket/walg-enc/db"
	plainPrefix = "s3://test-bucket/walg/db"
	thisBackup  = "base_00000003000000000000002C"
)

// fakeWalgArchives puts a `wal-g` script first on PATH that serves two storages: the
// BackupConfig's walg-enc prefix holds none of the backups asked about, the plain archive
// lists listing (the JSON of `wal-g backup-list --detail --json`) and answers "delete
// target" with deleteAnswer ("<exit code>|<stderr>"). It returns every call as
// "<WALE_S3_PREFIX> <args>".
func fakeWalgArchives(t *testing.T, listing, deleteAnswer string) func() []string {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	listPath := filepath.Join(dir, "list.json")
	if err := os.WriteFile(listPath, []byte(listing), 0644); err != nil {
		t.Fatal(err)
	}
	code, stderr, _ := strings.Cut(deleteAnswer, "|")
	script := "#!/bin/sh\necho \"$WALE_S3_PREFIX $*\" >> " + logPath + "\n" +
		"case \"$WALE_S3_PREFIX\" in\n" +
		"  *walg-enc*) case \"$1\" in\n" +
		"    backup-mark) echo \"ERROR: Failed to get previous backups: object 'walg-enc/db/17/basebackups_005/$3/metadata.json' not found in storage\" >&2; exit 1 ;;\n" +
		"    delete) [ \"$2\" = target ] && { echo \"ERROR: Backup '$3' does not exist.\" >&2; exit 1; } ;;\n" +
		"    backup-list) echo 'INFO: No backups found' >&2; exit 0 ;;\n" +
		"  esac ;;\n" +
		"  *) case \"$1 $2\" in\n" +
		"    \"backup-list --detail\") /bin/cat " + listPath + "; exit 0 ;;\n" +
		"    \"delete target\") echo '" + stderr + "' >&2; exit " + code + " ;;\n" +
		"  esac ;;\n" +
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

// listing renders `wal-g backup-list --detail --json` for name|start day|permanent triples.
func listing(backups ...string) string {
	items := make([]string, 0, len(backups))
	for _, b := range backups {
		parts := strings.Split(b, "|")
		items = append(items, `{"backup_name":"`+parts[0]+`","start_time":"2026-10-`+parts[1]+
			`T02:22:00Z","is_permanent":`+parts[2]+`}`)
	}
	return "[" + strings.Join(items, ",") + "]"
}

// plainArchiveBackup is Backup "b" (thisBackup, PG 17) under deletion, owned by BackupConfig
// "bc" whose prefix is prefix, next to live Backups of the same BackupConfig with the given
// backup IDs.
func plainArchiveBackup(t *testing.T, prefix string, liveBackupIDs ...string) (*BackupDeletionController, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1beta1.AddToScheme, cnpgv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	bc, secret := createTestBackupConfig("bc", "ns")
	bc.Spec.Storage.S3.Prefix = prefix
	owner := []metav1.OwnerReference{{APIVersion: v1beta1.GroupVersion.String(), Kind: "BackupConfig", Name: "bc", UID: bc.UID}}
	now := metav1.Now()
	objs := []client.Object{bc, secret, &cnpgv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "b", Namespace: "ns",
			Finalizers:        []string{v1beta1.BackupFinalizerName},
			DeletionTimestamp: &now,
			Labels:            map[string]string{v1beta1.BackupPgVersionLabelName: "17"},
			Annotations:       map[string]string{"x": "y"},
			OwnerReferences:   owner,
		},
		Status: cnpgv1.BackupStatus{BackupID: thisBackup},
	}}
	for i, id := range liveBackupIDs {
		objs = append(objs, &cnpgv1.Backup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "live-" + string(rune('a'+i)), Namespace: "ns", OwnerReferences: owner,
				Labels: map[string]string{v1beta1.BackupPgVersionLabelName: "17"},
			},
			Status: cnpgv1.BackupStatus{BackupID: id, Phase: cnpgv1.BackupPhaseCompleted},
		})
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return NewBackupDeletionController(c), c
}

func deleteB(t *testing.T, dc *BackupDeletionController) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return dc.deleteWALGBackup(ctx, client.ObjectKey{Namespace: "ns", Name: "b"})
}

// plainCalls returns the calls made against the plain archive, prefix stripped.
func plainCalls(calls []string) []string {
	var out []string
	for _, c := range calls {
		if rest, ok := strings.CutPrefix(c, plainPrefix+"/17 "); ok {
			out = append(out, rest)
		}
	}
	return out
}

func TestDeleteWALGBackup_PreEncryptionBackupIsDeletedFromThePlainArchive(t *testing.T) {
	calls := fakeWalgArchives(t, listing(
		"base_000000030000000000000010|01|false", // its Backup is gone, its data was left here
		"base_000000030000000000000020|02|true",  // permanent: kept
		"base_000000030000000000000025|03|false", // a live Backup still holds it
		thisBackup+"|04|false",
		"base_000000030000000000000030|05|false", // newer than this one
	), "0|")
	dc, c := plainArchiveBackup(t, encPrefix, "base_000000030000000000000025")
	if err := deleteB(t, dc); err != nil {
		t.Fatalf("deleteWALGBackup: %v", err)
	}
	if kept, _ := finalizerKept(t, c); kept {
		t.Fatal("the backup is deleted from the plain archive, the finalizer must go")
	}
	want := []string{
		"backup-list --detail --json",
		"delete target base_000000030000000000000010 --confirm",
		"delete target " + thisBackup + " --confirm",
		"delete garbage --confirm",
	}
	if got := plainCalls(calls()); !slices.Equal(got, want) {
		t.Fatalf("plain archive calls\n%v\nwant\n%v\n(all: %v)", got, want, calls())
	}
}

func TestDeleteWALGBackup_LastPreEncryptionBackupTakesTheArchiveWAL(t *testing.T) {
	calls := fakeWalgArchives(t, listing("base_000000030000000000000010|01|false", thisBackup+"|04|false"), "0|")
	dc, c := plainArchiveBackup(t, encPrefix)
	if err := deleteB(t, dc); err != nil {
		t.Fatalf("deleteWALGBackup: %v", err)
	}
	if kept, _ := finalizerKept(t, c); kept {
		t.Fatal("finalizer must go")
	}
	got := plainCalls(calls())
	if len(got) == 0 || got[len(got)-1] != "delete garbage --without-backup-check --confirm" {
		t.Fatalf("no backup is left in the plain archive: its WAL must go too, got %v", got)
	}
}

func TestDeleteWALGBackup_PlainArchiveLockKeepsTheFinalizer(t *testing.T) {
	fakeWalgArchives(t, listing(thisBackup+"|04|false"),
		"0|WARNING: Objects retained by bucket lock, not deleted: count=3 of 3")
	dc, c := plainArchiveBackup(t, encPrefix)
	_ = deleteB(t, dc)
	kept, annotations := finalizerKept(t, c)
	if !kept || annotations[v1beta1.BackupRetainedByLockAnnotationName] == "" {
		t.Fatalf("a lock refusal keeps the finalizer and marks the Backup, got kept=%v %v", kept, annotations)
	}
}

func TestDeleteWALGBackup_PlainArchiveStorageErrorKeepsTheFinalizer(t *testing.T) {
	fakeWalgArchives(t, listing(thisBackup+"|04|false"), "1|ERROR: operation error S3: DeleteObjects, StatusCode: 503")
	dc, c := plainArchiveBackup(t, encPrefix)
	if err := deleteB(t, dc); err == nil {
		t.Fatal("want an error for a retry")
	}
	kept, annotations := finalizerKept(t, c)
	if !kept || annotations[v1beta1.BackupDeletionFailingAnnotationName] == "" {
		t.Fatalf("a storage error keeps the finalizer and marks the Backup failing, got kept=%v %v", kept, annotations)
	}
}

func TestDeleteWALGBackup_InNeitherStorageReleasesTheBackup(t *testing.T) {
	// Deleted earlier (the finalizer update after it failed): nothing is left anywhere
	calls := fakeWalgArchives(t, listing("base_000000030000000000000030|05|false"), "0|")
	dc, c := plainArchiveBackup(t, encPrefix)
	if err := deleteB(t, dc); err != nil {
		t.Fatalf("deleteWALGBackup: %v", err)
	}
	if kept, _ := finalizerKept(t, c); kept {
		t.Fatal("nothing left to delete: the finalizer must go")
	}
	for _, call := range plainCalls(calls()) {
		if strings.HasPrefix(call, "delete") {
			t.Fatalf("nothing may be deleted in the plain archive, got %q", call)
		}
	}
}

func TestDeleteWALGBackup_NotInStorageWithoutPlainArchiveReleasesTheBackup(t *testing.T) {
	calls := fakeWalgArchives(t, "[]", "0|")
	dc, c := plainArchiveBackup(t, "s3://test-bucket/walg-enc-other/db")
	// the fake treats every prefix containing walg-enc as the encrypted storage
	if err := deleteB(t, dc); err != nil {
		t.Fatalf("deleteWALGBackup: %v", err)
	}
	if kept, _ := finalizerKept(t, c); kept {
		t.Fatal("finalizer must go")
	}
	if slices.ContainsFunc(calls(), func(c string) bool { return strings.Contains(c, "backup-list") }) {
		t.Fatalf("no plain archive to look into, got %v", calls())
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
			if strings.Contains(logs, "rid-first") {
				t.Fatalf("the queue logged with the reconcile that created it:\n%s", logs)
			}
			for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
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
