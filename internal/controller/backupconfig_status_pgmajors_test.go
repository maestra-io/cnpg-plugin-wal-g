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
