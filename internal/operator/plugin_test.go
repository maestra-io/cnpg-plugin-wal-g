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

package operator

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	cnpgi "github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/controller"
)

// A roll as the new and the old Pod see it: the lease belongs to another Pod, and SIGTERM.
var _ = Describe("CNPGI", Ordered, func() {
	const (
		namespace = "default"
		leaseName = "plugin-test"
	)

	var (
		testEnv   *envtest.Environment
		k8sClient client.Client
		plugin    *CNPGI
		mgr       ctrl.Manager
		sigterm   context.CancelFunc
		mgrDone   chan error
		cluster   *cnpgv1.Cluster
	)

	dial := func() *grpc.ClientConn {
		conn, err := grpc.NewClient(plugin.ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(conn.Close)
		return conn
	}
	probe := func(conn *grpc.ClientConn) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := identity.NewIdentityClient(conn).Probe(ctx, &identity.ProbeRequest{})
		return err
	}
	pre := func() (*cnpgi.ReconcilerHooksResult, error) {
		clusterJSON, err := json.Marshal(cluster)
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return cnpgi.NewReconcilerHooksClient(dial()).Pre(ctx, &cnpgi.ReconcilerHooksRequest{
			ClusterDefinition: clusterJSON, ResourceDefinition: clusterJSON,
		})
	}

	BeforeAll(func() {
		testScheme := runtime.NewScheme()
		Expect(clientscheme.AddToScheme(testScheme)).To(Succeed())
		Expect(cnpgv1.AddToScheme(testScheme)).To(Succeed())
		Expect(v1beta1.AddToScheme(testScheme)).To(Succeed())

		testEnv = &envtest.Environment{
			CRDDirectoryPaths: []string{
				filepath.Join("..", "..", "config", "crd", "bases"),
				filepath.Join("..", "..", "config", "crd", "cnpg"),
			},
			ErrorIfCRDPathMissing: true,
		}
		cfg, err := testEnv.Start()
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(testEnv.Stop)

		k8sClient, err = client.New(cfg, client.Options{Scheme: testScheme})
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       ptr.To("the-old-pod"),
				LeaseDurationSeconds: ptr.To(int32(600)),
				AcquireTime:          ptr.To(metav1.NowMicro()),
				RenewTime:            ptr.To(metav1.NowMicro()),
			},
		})).To(Succeed())
		backupConfig := &v1beta1.BackupConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "backup-config", Namespace: namespace},
		}
		backupConfig.Spec.Encryption.Method = "libsodium"
		Expect(k8sClient.Create(ctx, backupConfig)).To(Succeed())
		cluster = &cnpgv1.Cluster{
			TypeMeta:   metav1.TypeMeta{APIVersion: cnpgv1.SchemeGroupVersion.String(), Kind: cnpgv1.ClusterKind},
			ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: namespace, UID: "cluster-uid"},
			Spec: cnpgv1.ClusterSpec{Plugins: []cnpgv1.PluginConfiguration{{
				Name:       "cnpg-extensions.yandex.cloud",
				Parameters: map[string]string{"backupConfig": backupConfig.Name},
			}}},
		}

		// A free port: bound here, released for the plugin
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		address := listener.Addr().String()
		Expect(listener.Close()).To(Succeed())

		mgr, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                  testScheme,
			Metrics:                 server.Options{BindAddress: "0"},
			LeaderElection:          true,
			LeaderElectionID:        leaseName,
			LeaderElectionNamespace: namespace,
			LeaseDuration:           ptr.To(600 * time.Second),
			RenewDeadline:           ptr.To(400 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		plugin = &CNPGI{Client: mgr.GetClient(), Cache: mgr.GetCache(), ServerAddress: address}
		Expect(mgr.Add(plugin)).To(Succeed())

		Expect(plugin.ListenerCheck(nil)).NotTo(Succeed(), "ready before the listener serves")

		var mgrCtx context.Context
		mgrCtx, sigterm = context.WithCancel(ctx)
		DeferCleanup(func() { sigterm() })
		mgrDone = make(chan error, 1)
		go func() { mgrDone <- mgr.Start(mgrCtx) }()
	})

	It("serves and is ready while another Pod holds the lease", func() {
		Eventually(func() error { return plugin.ListenerCheck(nil) }, 10*time.Second, 50*time.Millisecond).Should(Succeed())
		Expect(probe(dial())).To(Succeed())
		Consistently(mgr.Elected(), time.Second).ShouldNot(BeClosed())
	})

	It("reconciles a Cluster while another Pod holds the lease, without touching the encryption key twice", func() {
		result, err := pre()
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Behavior).To(Equal(cnpgi.ReconcilerHooksResult_BEHAVIOR_CONTINUE))

		name := controller.GetRoleNameForBackupConfig(cluster.Name)
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rbacv1.Role{})).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rbacv1.RoleBinding{})).To(Succeed())
		secretKey := client.ObjectKey{Namespace: namespace, Name: "backup-config-encryption"}
		var first, second corev1.Secret
		Expect(k8sClient.Get(ctx, secretKey, &first)).To(Succeed())
		Expect(first.Data["libsodiumKey"]).NotTo(BeEmpty())

		// The first answers of a Pod may come from a cache that does not have the objects yet
		Eventually(func(g Gomega) {
			result, err := pre()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.Behavior).To(Equal(cnpgi.ReconcilerHooksResult_BEHAVIOR_CONTINUE))
		}, 5*time.Second, 50*time.Millisecond).Should(Succeed())
		Expect(k8sClient.Get(ctx, secretKey, &second)).To(Succeed())
		Expect(second.Data).To(Equal(first.Data))
		Expect(second.ResourceVersion).To(Equal(first.ResourceVersion))
	})

	It("keeps serving after SIGTERM until the endpoint removal has propagated, then exits in the grace period", func() {
		open := dial()
		Expect(probe(open)).To(Succeed())

		stopped := time.Now()
		sigterm()
		Consistently(func(g Gomega) {
			g.Expect(probe(open)).To(Succeed())
			g.Expect(probe(dial())).To(Succeed(), "a new connection")
		}, listenerDrain-time.Second, 100*time.Millisecond).Should(Succeed())

		Eventually(mgrDone, 10*time.Second).Should(Receive(BeNil()))
		Expect(time.Since(stopped)).To(BeNumerically("<", 10*time.Second), "the chart's terminationGracePeriodSeconds")
		Expect(plugin.ListenerCheck(nil)).NotTo(Succeed())
	})
})
