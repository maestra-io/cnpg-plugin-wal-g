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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("getSecretReferencesFromBackupConfig", func() {
	newS3BackupConfig := func() *v1beta1.BackupConfig {
		return &v1beta1.BackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-backup-config",
				Namespace: "test-namespace",
			},
			Spec: v1beta1.BackupConfigSpec{
				Storage: v1beta1.StorageConfig{
					StorageType: v1beta1.StorageTypeS3,
					S3: &v1beta1.S3StorageConfig{
						Prefix: "s3://test-bucket/prefix",
						AccessKeyIDRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "s3-credentials"},
							Key:                  "access-key",
						},
						AccessKeySecretRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "s3-credentials"},
							Key:                  "secret-key",
						},
					},
				},
			},
		}
	}

	It("should not reference a session token Secret when sessionToken is not set", func() {
		refs := getSecretReferencesFromBackupConfig(newS3BackupConfig())
		Expect(refs).To(ConsistOf(
			types.NamespacedName{Namespace: "test-namespace", Name: "s3-credentials"},
			types.NamespacedName{Namespace: "test-namespace", Name: "s3-credentials"},
		))
	})

	It("should reference the sessionToken Secret so it is protected by the finalizer", func() {
		backupConfig := newS3BackupConfig()
		backupConfig.Spec.Storage.S3.SessionTokenRef = &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "s3-session-token"},
			Key:                  "session_token",
		}

		refs := getSecretReferencesFromBackupConfig(backupConfig)
		Expect(refs).To(ContainElement(types.NamespacedName{Namespace: "test-namespace", Name: "s3-session-token"}))
		Expect(refs).To(HaveLen(3))
	})
})
