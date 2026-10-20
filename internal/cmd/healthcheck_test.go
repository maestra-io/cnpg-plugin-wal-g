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
	"context"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/instance"
)

type fakeIdentity struct {
	identity.UnimplementedIdentityServer
	ready bool
}

func (f fakeIdentity) Probe(context.Context, *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	return &identity.ProbeResponse{Ready: f.ready}, nil
}

var _ = Describe("healthcheck", func() {
	var socketPath string

	BeforeEach(func() {
		// unix socket paths are limited to ~104 bytes; keep the directory short.
		dir, err := os.MkdirTemp("", "hc")
		Expect(err).NotTo(HaveOccurred())
		if len(dir) > 80 {
			Expect(os.RemoveAll(dir)).To(Succeed())
			dir, err = os.MkdirTemp("/tmp", "hc")
			Expect(err).NotTo(HaveOccurred())
		}
		DeferCleanup(os.RemoveAll, dir)
		socketPath = filepath.Join(dir, "plugin.sock")
	})

	serve := func(ready bool) {
		lis, err := net.Listen("unix", socketPath)
		Expect(err).NotTo(HaveOccurred())
		srv := grpc.NewServer()
		identity.RegisterIdentityServer(srv, fakeIdentity{ready: ready})
		go func() { _ = srv.Serve(lis) }()
		DeferCleanup(srv.Stop)
	}

	probe := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return probeInstancePlugin(ctx, socketPath)
	}

	It("succeeds when the plugin answers Identity.Probe", func() {
		serve(true)
		Expect(probe()).To(Succeed())
	})

	It("fails when the plugin reports not ready", func() {
		serve(false)
		Expect(probe()).To(MatchError(ContainSubstring("reports not ready")))
	})

	It("fails when there is no socket yet", func() {
		Expect(probe()).To(MatchError(ContainSubstring("does not answer")))
	})

	It("fails on a stale socket file nobody listens on (emptyDir after a node reboot)", func() {
		lis, err := net.Listen("unix", socketPath)
		Expect(err).NotTo(HaveOccurred())
		lis.(*net.UnixListener).SetUnlinkOnClose(false)
		Expect(lis.Close()).To(Succeed())
		Expect(socketPath).To(BeAnExistingFile())

		Expect(probe()).To(MatchError(ContainSubstring("does not answer")))
	})

	It("is wired as a subcommand with the instance plugin's socket as default", func() {
		cmd := NewHealthcheckCmd()
		Expect(cmd.Use).To(Equal("healthcheck"))
		Expect(cmd.Flags().Lookup("socket").DefValue).To(Equal("/plugins/cnpg-extensions.yandex.cloud"))

		serve(true)
		cmd.SetArgs([]string{"--socket", socketPath})
		Expect(cmd.ExecuteContext(context.Background())).To(Succeed())
	})

	It("probes the socket the instance plugin's CNPG-I server actually creates", func() {
		Expect(common.InstancePluginSocketPath).To(Equal(path.Join("/plugins", common.PluginMetadata.Name)))

		dir := filepath.Dir(socketPath)
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		go func() { _ = (&instance.CNPGI{PluginPath: dir}).Start(ctx) }()

		Eventually(func() error {
			pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
			defer pcancel()
			return probeInstancePlugin(pctx, filepath.Join(dir, common.PluginMetadata.Name))
		}).WithTimeout(10 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
	})
})
