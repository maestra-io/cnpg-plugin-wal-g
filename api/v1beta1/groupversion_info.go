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

// Package v1 contains API Schema definitions for the  v1 API group.
// +kubebuilder:object:generate=true
// +groupName=cnpg-extensions.yandex.cloud
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "cnpg-extensions.yandex.cloud", Version: "v1beta1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	// apimachinery's builder: controller-runtime's scheme.Builder is
	// deprecated since controller-runtime v0.24.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme

	BackupConfigKind = "BackupConfig"
)

// addKnownTypes registers this group-version's types, as controller-runtime's
// scheme.Builder.Register did.
func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &BackupConfig{}, &BackupConfigList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
