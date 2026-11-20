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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("BackupConfigReconciler cluster Roles", func() {
	ctx := context.Background()
	roleKey := types.NamespacedName{Namespace: "default", Name: GetRoleNameForBackupConfig("test-cluster")}
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-backupconfig"}}

	secretRule := func(role *rbacv1.Role) []string {
		for _, rule := range role.Rules {
			if len(rule.Resources) == 1 && rule.Resources[0] == "secrets" {
				return rule.ResourceNames
			}
		}
		return nil
	}

	It("grants the encryption Secret as soon as the BackupConfig enables encryption", func() {
		cluster := createTestCluster("test-cluster", "default")
		backupConfig, s3Secret := createTestBackupConfig("test-backupconfig", "default")

		// The Role as the Cluster reconciler hook left it before encryption was turned on
		role := &rbacv1.Role{}
		role.Namespace, role.Name = roleKey.Namespace, roleKey.Name
		BuildRoleForBackupConfigs(role, cluster, []v1beta1.BackupConfig{*backupConfig})
		Expect(secretRule(role)).NotTo(ContainElement("test-backupconfig-encryption"))

		backupConfig.Spec.Encryption.Method = "libsodium"
		c := setupFakeBackupClient(cluster, backupConfig, s3Secret, role)
		r := &BackupConfigReconciler{Client: c, Scheme: c.Scheme(), BackupDeletionController: NewBackupDeletionController(c)}

		_, err := r.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		updated := &rbacv1.Role{}
		Expect(c.Get(ctx, roleKey, updated)).To(Succeed())
		Expect(secretRule(updated)).To(ContainElements("test-backupconfig-encryption", "test-backupconfig-s3-credentials"))
	})

	It("leaves a missing Role to the Cluster reconciler hook", func() {
		cluster := createTestCluster("test-cluster", "default")
		backupConfig, s3Secret := createTestBackupConfig("test-backupconfig", "default")
		c := setupFakeBackupClient(cluster, backupConfig, s3Secret)
		r := &BackupConfigReconciler{Client: c, Scheme: c.Scheme(), BackupDeletionController: NewBackupDeletionController(c)}

		_, err := r.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierrs.IsNotFound(c.Get(ctx, roleKey, &rbacv1.Role{}))).To(BeTrue())
	})

	It("does not touch the Role of a cluster using another BackupConfig", func() {
		cluster := createTestCluster("test-cluster", "default")
		cluster.Spec.Plugins[0].Parameters["backupConfig"] = "other-backupconfig"
		backupConfig, s3Secret := createTestBackupConfig("test-backupconfig", "default")
		backupConfig.Spec.Encryption.Method = "libsodium"
		role := &rbacv1.Role{}
		role.Namespace, role.Name = roleKey.Namespace, roleKey.Name
		c := setupFakeBackupClient(cluster, backupConfig, s3Secret, role)
		r := &BackupConfigReconciler{Client: c, Scheme: c.Scheme(), BackupDeletionController: NewBackupDeletionController(c)}

		_, err := r.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		unchanged := &rbacv1.Role{}
		Expect(c.Get(ctx, roleKey, unchanged)).To(Succeed())
		Expect(unchanged.Rules).To(BeEmpty())
	})
})
