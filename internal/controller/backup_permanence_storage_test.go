package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeWalgMark puts a `wal-g` script first on PATH that answers "backup-mark" with answer
// ("<exit code>|<stderr>", single quotes escaped) and every other call with exit 0.
func fakeWalgMark(t *testing.T, answer string) {
	t.Helper()
	dir := t.TempDir()
	code, stderr, _ := strings.Cut(answer, "|")
	script := "#!/bin/sh\ncase \"$1\" in\n  backup-mark) echo '" + stderr + "' >&2; exit " + code + " ;;\nesac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)
}

// manualBackup is a completed manual Backup (only the BackupConfig owns it) of cluster
// "test-cluster" whose sidecar did not push it permanent, so the controller marks it.
func manualBackup(t *testing.T) (*BackupReconciler, *cnpgv1.Backup) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1beta1.AddToScheme, cnpgv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	cluster := createTestCluster("test-cluster", "ns")
	bc, secret := createTestBackupConfig("test-backupconfig", "ns")
	backup := &cnpgv1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "manual", Namespace: "ns",
			Labels: map[string]string{v1beta1.BackupPgVersionLabelName: "18"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.GroupVersion.String(), Kind: "BackupConfig", Name: bc.Name, UID: bc.UID,
			}},
		},
		Spec: cnpgv1.BackupSpec{Cluster: cnpgv1.LocalObjectReference{Name: cluster.Name}},
		Status: cnpgv1.BackupStatus{
			Phase:    cnpgv1.BackupPhaseCompleted,
			BackupID: "base_0000000300000043000000A0",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, bc, secret, backup).Build()
	return &BackupReconciler{Client: c, Scheme: scheme}, backup
}

func TestReconcileBackupPermanence_BackupNotInStorageIsNotRetried(t *testing.T) {
	// A manual backup from before the BackupConfig moved its prefix (issues-maestra#1788)
	fakeWalgMark(t, "1|ERROR: Failed to get previous backups: object '\\''walg-enc/db-x/18/basebackups_005/"+
		"base_0000000300000043000000A0/metadata.json'\\'' not found in storage")
	r, backup := manualBackup(t)
	if err := r.reconcileBackupPermanence(context.Background(), backup); err != nil {
		t.Fatalf("a backup missing from the current storage must not requeue, got %v", err)
	}
}

func TestReconcileBackupPermanence_StorageErrorIsRetried(t *testing.T) {
	fakeWalgMark(t, "1|ERROR: operation error S3: GetObject, StatusCode: 403, api error AccessDenied")
	r, backup := manualBackup(t)
	if err := r.reconcileBackupPermanence(context.Background(), backup); err == nil {
		t.Fatal("a storage error must be returned for a retry")
	}
}
