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
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/instance"
)

func NewInstanceSubcommand() *cobra.Command {
	// logFlags := &log.Flags{}
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Starts the CNPG Yandex Extensions instance-plugin acting as a sidecar for PostgreSQL instances",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return instance.Start(cmd.Context())
		},
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			mode := viper.GetString("mode")
			if !slices.Contains(common.AllowedPluginModes, mode) {
				return fmt.Errorf("incorrect mode parameter: %s, should be one of 'normal', 'recovery'", mode)
			}
			return nil
		},
	}

	modeParamUsage := "instance plugin mode: " +
		"'normal' (use standard settings for backups and WAL fetch/store) or " +
		"'recovery' (use recovery settings for backups and WAL fetching)`)"
	cmd.Flags().String("mode", "normal", modeParamUsage)
	_ = viper.BindPFlag("mode", cmd.Flags().Lookup("mode"))

	cmd.Flags().String("cnpg-i-socket-path", "/plugins/cnpg-extensions.yandex.cloud",
		"CNPG-I GRPC server socket path (default /plugins/cnpg-extensions.yandex.cloud)")
	_ = viper.BindPFlag("cnpg-i-socket-path", cmd.Flags().Lookup("cnpg-i-socket-path"))

	// Switchover WAL guard, target primary side (internal/instance/switchover_guard.go).
	// Each flag can also be set through the environment variable named after it,
	// e.g. via the Cluster's spec.env, which the sidecar inherits.
	cmd.Flags().Duration(instance.SwitchoverTargetRetryTimeoutKey, instance.DefaultSwitchoverTargetRetryTimeout,
		"during a switchover, how long in total (from the first miss) the target primary keeps polling the archive "+
			"for missing WAL segments of the current timeline before answering 'not found' (0 disables the retry) "+
			"[env SWITCHOVER_TARGET_RETRY_TIMEOUT]")
	_ = viper.BindPFlag(instance.SwitchoverTargetRetryTimeoutKey, cmd.Flags().Lookup(instance.SwitchoverTargetRetryTimeoutKey))
	_ = viper.BindEnv(instance.SwitchoverTargetRetryTimeoutKey, "SWITCHOVER_TARGET_RETRY_TIMEOUT")

	cmd.Flags().Duration(instance.SwitchoverTargetRetryIntervalKey, instance.DefaultSwitchoverTargetRetryInterval,
		"pause between two archive polls of the switchover target primary [env SWITCHOVER_TARGET_RETRY_INTERVAL]")
	_ = viper.BindPFlag(instance.SwitchoverTargetRetryIntervalKey, cmd.Flags().Lookup(instance.SwitchoverTargetRetryIntervalKey))
	_ = viper.BindEnv(instance.SwitchoverTargetRetryIntervalKey, "SWITCHOVER_TARGET_RETRY_INTERVAL")

	return cmd
}
