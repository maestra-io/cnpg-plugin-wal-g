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

package common

import (
	"regexp"
	"strings"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
)

func GetRecoveryPluginConfigFromCluster(cluster *cnpgv1.Cluster) *cnpgv1.PluginConfiguration {
	if cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.Recovery == nil {
		return nil
	}

	clusterRecoverySourceName := cluster.Spec.Bootstrap.Recovery.Source
	if clusterRecoverySourceName == "" {
		return nil
	}

	clusterRecoverySource, ok := lo.Find(cluster.Spec.ExternalClusters, func(c cnpgv1.ExternalCluster) bool {
		return c.Name == clusterRecoverySourceName
	})
	if !ok {
		return nil
	}

	return clusterRecoverySource.PluginConfiguration
}

func GetPluginConfigFromCluster(cluster *cnpgv1.Cluster) *cnpgv1.PluginConfiguration {
	if cluster.Spec.Plugins == nil {
		return nil
	}

	pluginConfig, ok := lo.Find(cluster.Spec.Plugins, func(c cnpgv1.PluginConfiguration) bool {
		return c.Name == PluginNameDeprecated
	})

	if !ok {
		return nil
	}

	return &pluginConfig
}

// GetInitContainerRestartPolicy returns the restart policy configuration for init containers.
// Returns the policy pointer, or nil if no restart policy should be set.
// Supports "Always" (default) and "unset" values.
func GetInitContainerRestartPolicy(cluster *cnpgv1.Cluster) *corev1.ContainerRestartPolicy {
	pluginConfig := GetPluginConfigFromCluster(cluster)
	if pluginConfig == nil || pluginConfig.Parameters == nil {
		// Default: Always
		policy := corev1.ContainerRestartPolicyAlways
		return &policy
	}

	restartPolicyParam, exists := pluginConfig.Parameters["sidecarRestartPolicy"]
	if !exists {
		// Default: Always
		policy := corev1.ContainerRestartPolicyAlways
		return &policy
	}

	switch restartPolicyParam {
	case "Always":
		policy := corev1.ContainerRestartPolicyAlways
		return &policy
	case "unset":
		// Don't set restart policy (traditional init container behavior)
		return nil
	default:
		// Invalid value, use default (Always)
		policy := corev1.ContainerRestartPolicyAlways
		return &policy
	}
}

// SidecarReadOnlyRootFilesystemParameter: "true" runs this Cluster's sidecar
// with a read-only root filesystem and an emptyDir on /tmp (custom CA bundles
// and GCS credentials are written there). Any other value or no parameter
// keeps the writable root, so existing pods do not roll.
const SidecarReadOnlyRootFilesystemParameter = "sidecarReadOnlyRootFilesystem"

// IsSidecarReadOnlyRootFilesystem reports whether cluster asks for a read-only
// sidecar root filesystem: SidecarReadOnlyRootFilesystemParameter is "true" in
// its spec.plugins entry or in the plugin of its recovery source (a Cluster that
// only restores, such as a backup-verification one, has no spec.plugins entry,
// and its full-recovery Job still gets the sidecar).
func IsSidecarReadOnlyRootFilesystem(cluster *cnpgv1.Cluster) bool {
	for _, pluginConfig := range []*cnpgv1.PluginConfiguration{
		GetPluginConfigFromCluster(cluster), GetRecoveryPluginConfigFromCluster(cluster),
	} {
		if pluginConfig != nil && pluginConfig.Parameters[SidecarReadOnlyRootFilesystemParameter] == "true" {
			return true
		}
	}
	return false
}

// SidecarImageTagParameter selects, for one Cluster, another tag of the sidecar
// image than --cnpg-i-pg-sidecar-image carries (a canary of a new sidecar on one
// Cluster, without rolling every instance pod of the Kubernetes cluster). Only
// the tag can change: the repository stays the operator's.
const SidecarImageTagParameter = "sidecarImageTag"

var sidecarImageTagPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$`)

// GetSidecarImage returns the sidecar image for cluster: defaultImage, or its
// repository with the tag from the sidecarImageTag parameter when that is a
// valid release tag (vX.Y.Z[-suffix]). An invalid tag falls back to defaultImage.
func GetSidecarImage(cluster *cnpgv1.Cluster, defaultImage string) string {
	pluginConfig := GetPluginConfigFromCluster(cluster)
	if pluginConfig == nil {
		return defaultImage
	}
	tag, ok := pluginConfig.Parameters[SidecarImageTagParameter]
	if !ok || !sidecarImageTagPattern.MatchString(tag) {
		return defaultImage
	}
	repository := strings.SplitN(defaultImage, "@", 2)[0]
	if i := strings.LastIndex(repository, ":"); i > strings.LastIndex(repository, "/") {
		repository = repository[:i]
	}
	return repository + ":" + tag
}
