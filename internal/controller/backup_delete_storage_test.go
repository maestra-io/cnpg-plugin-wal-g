package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

// fakeWalgOnPath puts a `wal-g` script first on PATH that answers "delete target",
// "delete everything" and the storage checks ("st check") with answer ("<exit code>|<stderr>", or "hang" to never return) and
// every other call with exit 0. It returns a function reporting whether a delete was called.
func fakeWalgOnPath(t *testing.T, answer string) func() bool {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "calls.log")
	reply := "exec sleep 600"
	if answer != "hang" {
		code, stderr, _ := strings.Cut(answer, "|")
		reply = "echo '" + stderr + "' >&2; exit " + code
	}
	script := "#!/bin/sh\necho \"$*\" >> " + logPath + "\ncase \"$1 $2\" in\n" +
		"  \"delete target\"*|\"delete everything\"*|\"st check\"*) " + reply + " ;;\nesac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)

	return func() bool {
		data, _ := os.ReadFile(logPath)
		return strings.Contains(string(data), "delete target") || strings.Contains(string(data), "delete everything")
	}
}

// deletingBackup is a Backup under deletion whose BackupConfig reports the storage as not
// writable, as during the R2 incident of 18.11.2026 (issues-maestra#1786).
func deletingBackup(t *testing.T, backupAnnotations, configAnnotations map[string]string, withConfig bool) (*BackupDeletionController, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1beta1.AddToScheme, cnpgv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	bc, secret := createTestBackupConfig("bc", "ns")
	bc.Annotations = configAnnotations
	bc.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.ConditionTypeStorageWritable, Status: metav1.ConditionFalse,
		Reason: "StorageNotWritable", LastTransitionTime: metav1.Now(),
	}}
	now := metav1.Now()
	annotations := map[string]string{"x": "y"}
	for k, v := range backupAnnotations {
		annotations[k] = v
	}
	backup := &cnpgv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "b", Namespace: "ns",
			Finalizers:        []string{v1beta1.BackupFinalizerName},
			DeletionTimestamp: &now,
			Labels:            map[string]string{v1beta1.BackupPgVersionLabelName: "17"},
			Annotations:       annotations,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.GroupVersion.String(), Kind: "BackupConfig", Name: "bc", UID: bc.UID,
			}},
		},
		Status: cnpgv1.BackupStatus{BackupID: "base_00000003000000000000002C"},
	}
	objs := []client.Object{backup, secret}
	if withConfig {
		objs = append(objs, bc)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(bc).Build()
	return NewBackupDeletionController(c), c
}

// finalizerKept reports whether the Backup still exists with the plugin's finalizer, and
// returns its annotations.
func finalizerKept(t *testing.T, c client.Client) (bool, map[string]string) {
	t.Helper()
	b := &cnpgv1.Backup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}, b); err != nil {
		return false, nil
	}
	return containsString(b.Finalizers, v1beta1.BackupFinalizerName), b.Annotations
}

func TestDeleteWALGBackup_StorageFailureKeepsFinalizer(t *testing.T) {
	cases := map[string]string{
		"403 signature": "1|ERROR: operation error S3: ListObjectsV2, https response error StatusCode: 403, " +
			"RequestID: , HostID: , api error SignatureDoesNotMatch: The request signature we calculated does not match",
		"403 access denied": "1|ERROR: operation error S3: ListObjectsV2, StatusCode: 403, api error AccessDenied: Access Denied",
		"5xx":               "1|ERROR: operation error S3: ListObjectsV2, StatusCode: 503, api error ServiceUnavailable: Please reduce your request rate",
		"dial timeout":      "1|ERROR: operation error S3: ListObjectsV2, https response error: dial tcp 10.0.0.1:443: i/o timeout",
		"no such host":      "1|ERROR: operation error S3: ListObjectsV2, dial tcp: lookup wrong.invalid: no such host",
		"hang":              "hang",
	}
	for name, answer := range cases {
		t.Run(name, func(t *testing.T) {
			called := fakeWalgOnPath(t, answer)
			ctrl, c := deletingBackup(t, nil, nil, true)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			if err := ctrl.deleteWALGBackup(ctx, client.ObjectKey{Namespace: "ns", Name: "b"}); err == nil {
				t.Error("a failed deletion must return an error so that it is retried")
			}
			if !called() {
				t.Error("storage reported not writable must not skip the deletion attempt")
			}
			kept, annotations := finalizerKept(t, c)
			if !kept {
				t.Fatal("the finalizer was removed while the backup is still in storage: orphaned backup")
			}
			if _, ok := annotations[v1beta1.BackupDeletionFailingAnnotationName]; !ok {
				t.Errorf("the Backup must be marked %s, got %v", v1beta1.BackupDeletionFailingAnnotationName, annotations)
			}
		})
	}
}

func TestDeleteWALGBackup_LockRefusalKeepsFinalizer(t *testing.T) {
	fakeWalgOnPath(t, "0|WARNING: Objects retained by bucket lock, not deleted: count=3 of 3")
	// The storage was unreachable before and is back: the failing mark gives way to the lock mark.
	ctrl, c := deletingBackup(t, map[string]string{v1beta1.BackupDeletionFailingAnnotationName: "2026-11-18T17:47:40Z"}, nil, true)

	err := ctrl.deleteWALGBackup(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"})
	if err == nil {
		t.Error("a lock refusal must be retried")
	}
	kept, annotations := finalizerKept(t, c)
	if !kept {
		t.Fatal("the finalizer was removed while the lock keeps the backup")
	}
	if _, ok := annotations[v1beta1.BackupRetainedByLockAnnotationName]; !ok {
		t.Errorf("missing %s: %v", v1beta1.BackupRetainedByLockAnnotationName, annotations)
	}
	if _, ok := annotations[v1beta1.BackupDeletionFailingAnnotationName]; ok {
		t.Errorf("%s must be cleared once the storage answers: %v", v1beta1.BackupDeletionFailingAnnotationName, annotations)
	}
}

func TestDeleteWALGBackup_GivesUpOnlyWhenGoneOrAsked(t *testing.T) {
	type tc struct {
		answer            string
		backupAnnotations map[string]string
		configAnnotations map[string]string
		withConfig        bool
		walgCalled        bool
	}
	skip := map[string]string{v1beta1.SkipStorageCleanupAnnotationName: "true"}
	cases := map[string]tc{
		"bucket absent": {answer: "1|ERROR: operation error S3: ListObjectsV2, https response error StatusCode: 404, " +
			"api error NoSuchBucket: The specified bucket does not exist.", withConfig: true, walgCalled: true},
		"backup already deleted (storage answered, stale StorageWritable False)": {
			answer: "0|", withConfig: true, walgCalled: true},
		"BackupConfig gone":               {answer: "1|must not run", withConfig: false},
		"skip annotation on Backup":       {answer: "1|must not run", backupAnnotations: skip, withConfig: true},
		"skip annotation on BackupConfig": {answer: "1|must not run", configAnnotations: skip, withConfig: true},
		"skip annotation not \"true\"": {answer: "0|", withConfig: true, walgCalled: true,
			backupAnnotations: map[string]string{v1beta1.SkipStorageCleanupAnnotationName: "false"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			called := fakeWalgOnPath(t, tc.answer)
			ctrl, c := deletingBackup(t, tc.backupAnnotations, tc.configAnnotations, tc.withConfig)

			if err := ctrl.deleteWALGBackup(context.Background(), client.ObjectKey{Namespace: "ns", Name: "b"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if kept, _ := finalizerKept(t, c); kept {
				t.Error("the finalizer must be removed")
			}
			if called() != tc.walgCalled {
				t.Errorf("wal-g delete target called = %v, want %v", called(), tc.walgCalled)
			}
		})
	}
}

func TestDeletionRetryDelay(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	base := DeletionRequestTimeout + 2*time.Minute
	at := func(annotation string, ago time.Duration) *cnpgv1.Backup {
		b := &cnpgv1.Backup{}
		b.Annotations = map[string]string{annotation: now.Add(-ago).Format(time.RFC3339)}
		return b
	}
	for _, c := range []struct {
		backup *cnpgv1.Backup
		want   time.Duration
	}{
		{&cnpgv1.Backup{}, base},
		{at(v1beta1.BackupDeletionFailingAnnotationName, 10*time.Minute), base},
		{at(v1beta1.BackupDeletionFailingAnnotationName, 2*time.Hour), 30 * time.Minute},
		{at(v1beta1.BackupRetainedByLockAnnotationName, 72*time.Hour), time.Hour},
	} {
		if got := deletionRetryDelay(c.backup, now); got != c.want {
			t.Errorf("annotations %v: got %v, want %v", c.backup.Annotations, got, c.want)
		}
	}
}

func TestDeleteBackupConfig_StorageFailureKeepsFinalizer(t *testing.T) {
	for name, tc := range map[string]struct {
		answer    string
		annotated bool
		kept      bool
	}{
		"403 signature":   {answer: "1|ERROR: StatusCode: 403, api error SignatureDoesNotMatch", kept: true},
		"bucket absent":   {answer: "1|ERROR: StatusCode: 404, api error NoSuchBucket: The specified bucket does not exist."},
		"skip annotation": {answer: "1|must not run", annotated: true},
	} {
		t.Run(name, func(t *testing.T) {
			called := fakeWalgOnPath(t, tc.answer)
			var annotations map[string]string
			if tc.annotated {
				annotations = map[string]string{v1beta1.SkipStorageCleanupAnnotationName: "true"}
			}
			ctrl, c := deletingBackup(t, nil, annotations, true)
			bc := &v1beta1.BackupConfig{}
			key := client.ObjectKey{Namespace: "ns", Name: "bc"}
			if err := c.Get(context.Background(), key, bc); err != nil {
				t.Fatal(err)
			}
			bc.Finalizers = []string{v1beta1.BackupConfigFinalizerName}
			if err := c.Update(context.Background(), bc); err != nil {
				t.Fatal(err)
			}

			err := ctrl.deleteBackupConfig(context.Background(), key)
			if (err != nil) != tc.kept {
				t.Errorf("error = %v, want an error: %v", err, tc.kept)
			}
			if err := c.Get(context.Background(), key, bc); err != nil {
				t.Fatal(err)
			}
			if got := containsString(bc.Finalizers, v1beta1.BackupConfigFinalizerName); got != tc.kept {
				t.Errorf("finalizer kept = %v, want %v", got, tc.kept)
			}
			if called() == tc.annotated {
				t.Errorf("wal-g delete called = %v with the skip annotation = %v", called(), tc.annotated)
			}
		})
	}
}

// The deletion conditions must show while the storage is unreachable: that is when a
// deletion fails. The status reconcile skipped them whenever the read check failed.
func TestStatusReportsFailingDeletionWhileStorageUnreachable(t *testing.T) {
	fakeWalgOnPath(t, "1|ERROR: operation error S3: ListObjectsV2, StatusCode: 403, api error SignatureDoesNotMatch")
	_, c := deletingBackup(t, map[string]string{v1beta1.BackupDeletionFailingAnnotationName: "2026-11-19T12:13:56Z"}, nil, true)
	status := NewBackupConfigStatusController(c, time.Hour, 2*time.Second)
	status.storageCheckRetryBackoff = 0

	key := client.ObjectKey{Namespace: "ns", Name: "bc"}
	if err := status.reconcileStatusByKey(context.Background(), key, logr.Discard()); err != nil {
		t.Fatal(err)
	}
	bc := &v1beta1.BackupConfig{}
	if err := c.Get(context.Background(), key, bc); err != nil {
		t.Fatal(err)
	}
	if meta.IsStatusConditionTrue(bc.Status.Conditions, v1beta1.ConditionTypeStorageReadable) {
		t.Fatal("the storage check must fail in this test")
	}
	if !meta.IsStatusConditionTrue(bc.Status.Conditions, v1beta1.ConditionTypeBackupDeletionFailing) {
		t.Errorf("BackupDeletionFailing must be set while the storage is unreachable, got %+v", bc.Status.Conditions)
	}
}
