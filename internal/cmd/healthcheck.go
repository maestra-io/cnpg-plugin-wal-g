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
	"errors"
	"fmt"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
)

// NewHealthcheckCmd creates the command the plugin-wal-g sidecar's
// startupProbe runs: it succeeds only once the instance plugin answers the
// CNPG-I Identity Probe on its unix socket. A socket FILE is not enough:
// /plugins is an emptyDir, so after a node reboot the previous run's socket
// is still there until the new process replaces it.
func NewHealthcheckCmd() *cobra.Command {
	var (
		socketPath string
		timeout    time.Duration
	)

	cmd := &cobra.Command{
		Use:           "healthcheck",
		Short:         "Checks that the instance plugin serves CNPG-I on its unix socket",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the error

		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			return probeInstancePlugin(ctx, socketPath)
		},
	}

	cmd.Flags().StringVar(&socketPath, "socket", common.InstancePluginSocketPath,
		"CNPG-I unix socket of the instance plugin")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "overall timeout of the check")

	return cmd
}

// probeInstancePlugin calls Identity.Probe over the unix socket at socketPath.
func probeInstancePlugin(ctx context.Context, socketPath string) error {
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("cannot create gRPC client for %s: %w", socketPath, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := identity.NewIdentityClient(conn).Probe(ctx, &identity.ProbeRequest{})
	if err != nil {
		return fmt.Errorf("instance plugin on %s does not answer: %w", socketPath, err)
	}
	if !resp.GetReady() {
		return errors.New("instance plugin on " + socketPath + " reports not ready")
	}

	return nil
}
