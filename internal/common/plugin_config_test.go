package common

import (
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func clusterWithParams(params map[string]string) *cnpgv1.Cluster {
	return &cnpgv1.Cluster{Spec: cnpgv1.ClusterSpec{Plugins: []cnpgv1.PluginConfiguration{
		{Name: PluginNameDeprecated, Parameters: params},
	}}}
}

func TestGetSidecarImage(t *testing.T) {
	const def = "registry.example/cnpg-plugin-wal-g:v0.3.0-maestra.7"
	for _, tc := range []struct {
		name    string
		cluster *cnpgv1.Cluster
		want    string
	}{
		{"no plugin", &cnpgv1.Cluster{}, def},
		{"no parameter", clusterWithParams(map[string]string{"backupConfig": "x"}), def},
		{"release tag", clusterWithParams(map[string]string{SidecarImageTagParameter: "v0.3.0-maestra.8"}),
			"registry.example/cnpg-plugin-wal-g:v0.3.0-maestra.8"},
		{"repository cannot change", clusterWithParams(map[string]string{SidecarImageTagParameter: "evil/image:v1.0.0"}), def},
		{"digest-like value refused", clusterWithParams(map[string]string{SidecarImageTagParameter: "sha256:abc"}), def},
		{"empty refused", clusterWithParams(map[string]string{SidecarImageTagParameter: ""}), def},
	} {
		if got := GetSidecarImage(tc.cluster, def); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
	// a registry with a port keeps its port
	if got := GetSidecarImage(clusterWithParams(map[string]string{SidecarImageTagParameter: "v1.2.3"}),
		"reg:5000/p:v0.0.1"); got != "reg:5000/p:v1.2.3" {
		t.Errorf("port: got %q", got)
	}
}

func TestIsManualBackup(t *testing.T) {
	owned := func(kinds ...string) *cnpgv1.Backup {
		b := &cnpgv1.Backup{}
		for _, k := range kinds {
			b.OwnerReferences = append(b.OwnerReferences, metav1.OwnerReference{Kind: k})
		}
		return b
	}
	if !IsManualBackup(owned()) || !IsManualBackup(owned("BackupConfig")) {
		t.Error("a Backup owned by nothing or by its BackupConfig only is manual")
	}
	if IsManualBackup(owned("ScheduledBackup", "BackupConfig")) || IsManualBackup(owned("Cluster")) {
		t.Error("a scheduled or cluster-owned Backup is not manual")
	}
}

func TestIsSidecarReadOnlyRootFilesystem(t *testing.T) {
	on := map[string]string{SidecarReadOnlyRootFilesystemParameter: "true"}
	recovery := func(params map[string]string) *cnpgv1.Cluster {
		return &cnpgv1.Cluster{Spec: cnpgv1.ClusterSpec{
			Bootstrap: &cnpgv1.BootstrapConfiguration{Recovery: &cnpgv1.BootstrapRecovery{Source: "src"}},
			ExternalClusters: []cnpgv1.ExternalCluster{{Name: "src",
				PluginConfiguration: &cnpgv1.PluginConfiguration{Name: PluginNameDeprecated, Parameters: params}}},
		}}
	}
	for _, tc := range []struct {
		name    string
		cluster *cnpgv1.Cluster
		want    bool
	}{
		{"no plugin", &cnpgv1.Cluster{}, false},
		{"no parameter", clusterWithParams(map[string]string{"backupConfig": "x"}), false},
		{"false", clusterWithParams(map[string]string{SidecarReadOnlyRootFilesystemParameter: "false"}), false},
		{"True is not true", clusterWithParams(map[string]string{SidecarReadOnlyRootFilesystemParameter: "True"}), false},
		{"spec.plugins true", clusterWithParams(on), true},
		{"recovery source true", recovery(on), true},
		{"recovery source without it", recovery(map[string]string{"backupConfig": "x"}), false},
	} {
		if got := IsSidecarReadOnlyRootFilesystem(tc.cluster); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
