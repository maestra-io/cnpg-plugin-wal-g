/*
Copyright 2025 YANDEX LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
)

var _ = Describe("pgMajorsToProbe", func() {
	It("probes only the cluster's major in steady state", func() {
		Expect(pgMajorsToProbe([]string{"16", "storage-check"}, true, []int{16})).To(Equal([]int{16}))
	})

	It("probes the new major at once after an upgrade, and the old one while it holds data", func() {
		// The cluster runs 17 and has not written to 17/ yet: 17 comes from the Cluster.
		Expect(pgMajorsToProbe([]string{"16", "storage-check"}, true, []int{17})).To(Equal([]int{16, 17}))
		// Both prefixes hold data.
		Expect(pgMajorsToProbe([]string{"16", "17", "storage-check"}, true, []int{17})).To(Equal([]int{16, 17}))
	})

	It("drops the old major once its prefix is emptied", func() {
		Expect(pgMajorsToProbe([]string{"17", "storage-check"}, true, []int{17})).To(Equal([]int{17}))
	})

	It("probes the majors that hold data when no Cluster archives to the prefix", func() {
		Expect(pgMajorsToProbe([]string{"15", "storage-check"}, true, nil)).To(Equal([]int{15}))
		Expect(pgMajorsToProbe(nil, true, nil)).To(BeEmpty())
	})

	It("probes every known major when the prefix cannot be listed", func() {
		Expect(pgMajorsToProbe(nil, false, []int{17})).To(Equal([]int{11, 12, 13, 14, 15, 16, 17, 18, 19}))
		Expect(pgMajorsToProbe(nil, false, []int{20})).To(ContainElements(11, 19, 20))
	})
})

// fakeWalgStatusPass puts a `wal-g` first on PATH that logs "<storage prefix> <args>" of
// every call, and answers `st ls` with the folders (a failed listing if listFails).
func fakeWalgStatusPass(t *testing.T, folders []string, listFails bool) func() []string {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	listing := "type size last modified                 name"
	for _, f := range folders {
		listing += "\ndir  0    0001-01-01 00:00:00 +0000 UTC " + f + "/"
	}
	script := "#!/bin/sh\necho \"$WALE_S3_PREFIX $*\" >> " + calls + "\ncase \"$*\" in\n" +
		"  'st ls') " + map[bool]string{true: "exit 1", false: "printf '" + strings.ReplaceAll(listing, "\n", "\\n") + "\\n'"}[listFails] + " ;;\n" +
		"  'wal-show --detailed-json') echo '[]' ;;\n  'backup-list --detail --json') echo '[]' ;;\nesac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "wal-g"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	reaperCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = (&cmd.ZombieProcessReaper{}).Start(reaperCtx) }()
	time.Sleep(50 * time.Millisecond)
	return func() []string {
		raw, err := os.ReadFile(calls)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(strings.TrimSpace(string(raw)), "\n")
	}
}

func TestReconcileStatus_ProbesOnlyMajorsWithData(t *testing.T) {
	const root = "s3://test-bucket/test-prefix"
	for name, tc := range map[string]struct {
		image     string
		folders   []string
		listFails bool
		want      []string
	}{
		"steady state":               {"pg:18.0", []string{"18", "storage-check"}, false, []string{"18"}},
		"upgraded, nothing in 18/":   {"pg:18.0", []string{"16", "storage-check"}, false, []string{"16", "18"}},
		"upgraded, both hold data":   {"pg:18.0", []string{"16", "18", "storage-check"}, false, []string{"16", "18"}},
		"old prefix emptied":         {"pg:18.0", []string{"18", "storage-check"}, false, []string{"18"}},
		"listing failed, all majors": {"pg:18.0", nil, true, []string{"11", "12", "13", "14", "15", "16", "17", "18", "19"}},
	} {
		t.Run(name, func(t *testing.T) {
			calls := fakeWalgStatusPass(t, tc.folders, tc.listFails)
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1beta1.AddToScheme, cnpgv1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			cluster := createTestCluster("test-cluster", "ns")
			cluster.Spec.ImageName = tc.image
			bc, secret := createTestBackupConfig("test-backupconfig", "ns")
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, bc, secret).WithStatusSubresource(bc).Build()
			controller := NewBackupConfigStatusController(c, time.Minute, 0)
			if err := controller.reconcileStatusByKey(context.Background(), client.ObjectKeyFromObject(bc), logr.Discard()); err != nil {
				t.Fatal(err)
			}

			all := calls()
			for _, sub := range []string{"wal-show --detailed-json", "backup-list --detail --json"} {
				var got []string
				for _, call := range all {
					if prefix, args, _ := strings.Cut(call, " "); args == sub {
						got = append(got, strings.TrimPrefix(prefix, root+"/"))
					}
				}
				if !slices.Equal(got, tc.want) {
					t.Errorf("%s probed %v, want %v", sub, got, tc.want)
				}
			}
			if n := len(slices.DeleteFunc(slices.Clone(all), func(c string) bool { return c != root+"/ st ls" })); n != 1 {
				t.Errorf("want one listing of the prefix, got %d in %v", n, all)
			}
		})
	}
}
