package controller

import (
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
)

func TestNeedsBackupMark(t *testing.T) {
	manual := func(meta map[string]string) *cnpgv1.Backup {
		b := &cnpgv1.Backup{}
		b.OwnerReferences = []metav1.OwnerReference{{Kind: "BackupConfig"}}
		b.Status.PluginMetadata = meta
		return b
	}
	if !needsBackupMark(manual(nil)) {
		t.Error("a manual backup from an older sidecar (no pluginMetadata) is still marked")
	}
	if needsBackupMark(manual(map[string]string{common.BackupPermanentMetadataKey: "true"})) {
		t.Error("a backup pushed permanent must not be marked again (metadata.json rewrite)")
	}
	scheduled := manual(nil)
	scheduled.OwnerReferences = append(scheduled.OwnerReferences, metav1.OwnerReference{Kind: "ScheduledBackup"})
	if needsBackupMark(scheduled) {
		t.Error("a scheduled backup is never marked")
	}
}

func TestReconcileRetainedDeletions(t *testing.T) {
	now := metav1.Now()
	backup := func(name string, deleting, marked bool) cnpgv1.Backup {
		b := cnpgv1.Backup{}
		b.Name = name
		if deleting {
			b.DeletionTimestamp = &now
		}
		if marked {
			b.Annotations = map[string]string{v1beta1.BackupRetainedByLockAnnotationName: "2026-11-17T20:00:00Z"}
		}
		return b
	}
	bc := &v1beta1.BackupConfig{}

	reconcileRetainedDeletions(bc, []cnpgv1.Backup{backup("b", true, true), backup("a", true, true), backup("c", false, true)})
	if len(bc.Status.Conditions) != 1 || bc.Status.Conditions[0].Status != metav1.ConditionTrue ||
		bc.Status.Conditions[0].Message != "2 Backup(s) kept in storage by the bucket lock, deletion retried: a, b" {
		t.Fatalf("got %+v", bc.Status.Conditions)
	}
	if determinePhase(bc) != v1beta1.BackupConfigPhaseHealthy {
		t.Error("a retained deletion is not a failure: the phase stays healthy")
	}

	reconcileRetainedDeletions(bc, []cnpgv1.Backup{backup("d", true, false)})
	if len(bc.Status.Conditions) != 0 {
		t.Fatalf("condition must go when nothing is retained, got %+v", bc.Status.Conditions)
	}
}
