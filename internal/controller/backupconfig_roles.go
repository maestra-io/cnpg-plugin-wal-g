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
	"fmt"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/machinery/pkg/stringset"
	"github.com/go-logr/logr"
	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BackupConfigsForCluster returns the BackupConfigs the cluster uses for archiving and for recovery
func BackupConfigsForCluster(
	ctx context.Context,
	c client.Client,
	cluster *cnpgv1.Cluster,
) ([]v1beta1.BackupConfig, error) {
	backupConfigs := make([]v1beta1.BackupConfig, 0, 2)

	archiveBackupConfig, err := v1beta1.GetBackupConfigForCluster(ctx, c, cluster)
	if err != nil {
		return nil, fmt.Errorf("while fetching backup config of cluster %s: %w", cluster.Name, err)
	}
	if archiveBackupConfig != nil {
		backupConfigs = append(backupConfigs, *archiveBackupConfig)
	}

	recoveryBackupConfig, err := v1beta1.GetBackupConfigForClusterRecovery(ctx, c, cluster)
	if err != nil {
		return nil, fmt.Errorf("while fetching recovery backup config of cluster %s: %w", cluster.Name, err)
	}
	if recoveryBackupConfig != nil {
		backupConfigs = append(backupConfigs, *recoveryBackupConfig)
	}

	return backupConfigs, nil
}

// EnsureRoleForCluster makes the Role of the cluster grant access to exactly what backupConfigs reference.
// A missing Role is created only when create is true.
func EnsureRoleForCluster(
	ctx context.Context,
	c client.Client,
	cluster *cnpgv1.Cluster,
	backupConfigs []v1beta1.BackupConfig,
	create bool,
) error {
	logger := logr.FromContextOrDiscard(ctx)

	var role rbacv1.Role
	if err := c.Get(ctx, client.ObjectKey{
		Namespace: cluster.Namespace,
		Name:      GetRoleNameForBackupConfig(cluster.Name),
	}, &role); err != nil {
		if !apierrs.IsNotFound(err) || !create {
			return client.IgnoreNotFound(err)
		}

		newRole := rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: cluster.Namespace,
				Name:      GetRoleNameForBackupConfig(cluster.Name),
			},

			Rules: []rbacv1.PolicyRule{},
		}

		BuildRoleForBackupConfigs(&newRole, cluster, backupConfigs)
		logger.Info("Creating role", "role", newRole.TypeMeta)

		return c.Create(ctx, &newRole)
	}

	// Role already exists, need to calculate changes and update
	oldRole := role.DeepCopy()

	BuildRoleForBackupConfigs(&role, cluster, backupConfigs)
	if equality.Semantic.DeepEqual(&role, oldRole) {
		// There's no need to hit the API server again
		return nil
	}

	patchData, _ := client.MergeFrom(oldRole).Data(&role)
	logger.Info("Patching role", "role", role.TypeMeta, "patch", string(patchData))

	return c.Patch(ctx, &role, client.MergeFrom(oldRole))
}

// BuildRole builds the Role object for this cluster
// It collects all BackupConfig objects used by cluster and grants read/watch permissions on them only
// It also finds all secrets referenced by these BackupConfig and grants read permission on them
func BuildRoleForBackupConfigs(
	role *rbacv1.Role,
	cluster *cnpgv1.Cluster,
	backupConfigs []v1beta1.BackupConfig,
) {
	backupConfigNames := stringset.New()
	secretsNames := stringset.New()
	configMapsNames := stringset.New()

	for i := range backupConfigs {
		backupConfigNames.Put(backupConfigs[i].Name)
		backupConfigSecrets, backupConfigCMs := collectSecretsAndConfigMapsNamesFromBackupConfig(&backupConfigs[i])
		for _, secretName := range backupConfigSecrets {
			secretsNames.Put(secretName)
		}
		for _, configMapName := range backupConfigCMs {
			configMapsNames.Put(configMapName)
		}
	}

	role.SetOwnerReferences([]metav1.OwnerReference{
		{
			APIVersion:         cnpgv1.SchemeGroupVersion.String(),
			Kind:               cnpgv1.ClusterKind,
			Name:               cluster.Name,
			UID:                cluster.UID,
			BlockOwnerDeletion: ptr.To(true),
		},
	})

	role.Rules = []rbacv1.PolicyRule{
		// It is safe to grant list permission restricted by resourceNames
		// (https://kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources)
		{
			APIGroups:     []string{"cnpg-extensions.yandex.cloud"},
			Verbs:         []string{"watch", "get", "list"},
			Resources:     []string{"backupconfigs"},
			ResourceNames: backupConfigNames.ToSortedList(),
		},
		{
			APIGroups:     []string{"cnpg-extensions.yandex.cloud"},
			Verbs:         []string{"update"},
			Resources:     []string{"backupconfigs/status"},
			ResourceNames: backupConfigNames.ToSortedList(),
		},
		{
			APIGroups:     []string{""},
			Verbs:         []string{"watch", "get", "list"},
			Resources:     []string{"secrets"},
			ResourceNames: secretsNames.ToSortedList(),
		},
		{
			APIGroups:     []string{""},
			Verbs:         []string{"watch", "get", "list"},
			Resources:     []string{"configmaps"},
			ResourceNames: configMapsNames.ToSortedList(),
		},
	}
}

// nolint:gocyclo
func collectSecretsAndConfigMapsNamesFromBackupConfig(config *v1beta1.BackupConfig) ([]string, []string) {
	usedConfigMapNames := make([]string, 0)
	usedSecretNames := make([]string, 0)

	if config.Spec.Storage.StorageType == v1beta1.StorageTypeS3 {
		s3Config := config.Spec.Storage.S3

		// Add AccessKeyID and AccessKeySecret secrets names
		usedSecretNames = append(
			usedSecretNames,
			s3Config.AccessKeyIDRef.Name,
			s3Config.AccessKeySecretRef.Name,
		)

		if s3Config.SessionTokenRef != nil {
			usedSecretNames = append(usedSecretNames, s3Config.SessionTokenRef.Name)
		}

		if s3Config.PrefixFrom != nil {
			if s3Config.PrefixFrom.SecretKeyRef != nil {
				usedSecretNames = append(usedSecretNames, s3Config.PrefixFrom.SecretKeyRef.Name)
			}
			if s3Config.PrefixFrom.ConfigMapKeyRef != nil {
				usedConfigMapNames = append(usedConfigMapNames, s3Config.PrefixFrom.ConfigMapKeyRef.Name)
			}
		}

		if s3Config.RegionFrom != nil {
			if s3Config.RegionFrom.SecretKeyRef != nil {
				usedSecretNames = append(usedSecretNames, s3Config.RegionFrom.SecretKeyRef.Name)
			}
			if s3Config.RegionFrom.ConfigMapKeyRef != nil {
				usedConfigMapNames = append(usedConfigMapNames, s3Config.RegionFrom.ConfigMapKeyRef.Name)
			}
		}

		if s3Config.EndpointURLFrom != nil {
			if s3Config.EndpointURLFrom.SecretKeyRef != nil {
				usedSecretNames = append(usedSecretNames, s3Config.EndpointURLFrom.SecretKeyRef.Name)
			}
			if s3Config.EndpointURLFrom.ConfigMapKeyRef != nil {
				usedConfigMapNames = append(usedConfigMapNames, s3Config.EndpointURLFrom.ConfigMapKeyRef.Name)
			}
		}

		if s3Config.CustomCA != nil {
			if s3Config.CustomCA.Kind == "Secret" {
				usedSecretNames = append(usedSecretNames, s3Config.CustomCA.Name)
			} else {
				usedConfigMapNames = append(usedConfigMapNames, s3Config.CustomCA.Name)
			}
		}
	}

	if config.Spec.Storage.StorageType == v1beta1.StorageTypeGCS {
		gcsConfig := config.Spec.Storage.GCS

		if gcsConfig.CredentialsSecretRef != nil {
			usedSecretNames = append(usedSecretNames, gcsConfig.CredentialsSecretRef.Name)
		}

		if gcsConfig.PrefixFrom != nil {
			if gcsConfig.PrefixFrom.SecretKeyRef != nil {
				usedSecretNames = append(usedSecretNames, gcsConfig.PrefixFrom.SecretKeyRef.Name)
			}
			if gcsConfig.PrefixFrom.ConfigMapKeyRef != nil {
				usedConfigMapNames = append(usedConfigMapNames, gcsConfig.PrefixFrom.ConfigMapKeyRef.Name)
			}
		}
	}

	// Add encryption secrets to the list of secrets that need permissions
	if config.Spec.Encryption.Method != "" && config.Spec.Encryption.Method != "none" {
		encryptionSecretName := v1beta1.GetBackupConfigEncryptionSecretName(config)
		if encryptionSecretName != "" {
			usedSecretNames = append(usedSecretNames, encryptionSecretName)
		}
	}

	return usedSecretNames, usedConfigMapNames
}

// BuildRoleBinding builds the role binding object for this cluster
func BuildRoleBindingForBackupConfig(
	cluster *cnpgv1.Cluster,
) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      GetRoleNameForBackupConfig(cluster.Name),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         cnpgv1.SchemeGroupVersion.String(),
					Kind:               cnpgv1.ClusterKind,
					Name:               cluster.Name,
					UID:                cluster.UID,
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				APIGroup:  "",
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     GetRoleNameForBackupConfig(cluster.Name),
		},
	}
}

// GetRBACName returns the name of the RBAC entities for the wal-g backup configs
func GetRoleNameForBackupConfig(clusterName string) string {
	return fmt.Sprintf("%s-backups", clusterName)
}
