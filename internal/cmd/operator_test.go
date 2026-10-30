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

package cmd

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/operator"
)

var _ = Describe("operator command", func() {
	AfterEach(func() { viper.Reset() })

	It("has --storage-check-timeout, default 60s, bound to viper", func() {
		command := NewOperatorCmd()
		flag := command.Flags().Lookup(operator.StorageCheckTimeoutKey)
		Expect(flag).NotTo(BeNil())
		Expect(flag.DefValue).To(Equal("1m0s"))
		Expect(viper.GetDuration(operator.StorageCheckTimeoutKey)).To(Equal(60 * time.Second))

		Expect(command.Flags().Set(operator.StorageCheckTimeoutKey, "90s")).To(Succeed())
		Expect(viper.GetDuration(operator.StorageCheckTimeoutKey)).To(Equal(90 * time.Second))
	})
})
