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

package operator

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	// +kubebuilder:scaffold:imports

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/certs"
	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/controller"
	"github.com/wal-g/cnpg-plugin-wal-g/internal/util/cmd"
	webhookv1 "github.com/wal-g/cnpg-plugin-wal-g/internal/webhook/v1beta1"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/version"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

const (
	// WebhookSecretName is the name of the secret where the certificates
	// for the webhook server are stored
	WebhookSecretName = "cnpg-plugin-wal-g-webhook-cert" // #nosec

	// WebhookServiceName is the name of the service where the webhook server
	// is reachable
	WebhookServiceName = "cnpg-plugin-wal-g-webhook-service" // #nosec

	// MutatingWebhookConfigurationName is the name of the mutating webhook configuration
	MutatingWebhookConfigurationName = "cnpg-plugin-wal-g-mutating-webhook-configuration"

	// ValidatingWebhookConfigurationName is the name of the validating webhook configuration
	ValidatingWebhookConfigurationName = "cnpg-plugin-wal-g-validating-webhook-configuration"

	// The name of the directory containing the TLS certificates
	defaultWebhookCertDir = "/run/secrets/cnpg-plugin-wal-g/webhook"

	// CaSecretName is the name of the secret which is hosting the Operator CA
	CaSecretName = "cnpg-plugin-wal-g-ca-secret" // #nosec

	// OperatorDeploymentLabelSelector is the labelSelector to be used to get the operators deployment
	OperatorDeploymentLabelSelector = "app.kubernetes.io/name=cnpg-plugin-wal-g"

	// StorageCheckTimeoutKey is the viper key (flag --storage-check-timeout,
	// env STORAGE_CHECK_TIMEOUT) of the budget of one `wal-g st check` call of
	// the BackupConfig status controller.
	StorageCheckTimeoutKey = "storage-check-timeout"

	// BackupConfigStatusIntervalKey is the viper key (flag --backup-config-status-interval,
	// env BACKUP_CONFIG_STATUS_INTERVAL) of the period of the BackupConfig status pass.
	BackupConfigStatusIntervalKey = "backup-config-status-interval"

	// DefaultBackupConfigStatusInterval is the default period of the status pass. Its
	// consumers need no more: CnpgBackupDeletionFailing fires after 1h,
	// PostgresqlNoRecentBackup after 36h, and WalgS3StorageUnavailable on a 15m window
	// plus for: 20m. Each pass lists every BackupConfig's storage (~40 R2 Class A
	// operations per BackupConfig, omega 22.11.2026).
	DefaultBackupConfigStatusInterval = 10 * time.Minute
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cnpgv1.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo,funlen
func Start(ctx context.Context) error {
	var tlsOpts []func(*tls.Config)

	opts := zap.Options{}
	if version.IsDevelopment() {
		opts.Development = true
	} else {
		opts.Development = false
		opts.StacktraceLevel = zapcore.DPanicLevel
	}

	opts.BindFlags(flag.CommandLine)

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	ctx = logr.NewContext(ctx, ctrl.Log)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !viper.GetBool("enable-http2") {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var webhookCertWatcher *certwatcher.CertWatcher

	webhookCertPath := viper.GetString("webhook-cert-path")
	webhookCertName := viper.GetString("webhook-cert-name")
	webhookCertKey := viper.GetString("webhook-cert-key")

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
		CertDir: defaultWebhookCertDir,
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: "0", // Disabling metrics server by default
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: viper.GetString("health-probe-bind-address"),
		LeaderElection:         viper.GetBool("leader-elect"),
		LeaderElectionID:       "f522231e.cnpg-extensions.yandex.cloud",
		// The defaults (15s lease, 10s renew deadline) lost the lease on every etcd
		// defrag pass, which stalls the apiserver's etcd member for 9-11s, and restarted
		// the whole operator daily (omega 21-23.11.2026, issues-maestra#1786).
		LeaseDuration: ptr.To(60 * time.Second),
		RenewDeadline: ptr.To(40 * time.Second),
		// LeaderElectionReleaseOnCancel stays off. It is safe only if nothing runs after
		// the Manager stops, and BackupDeletionController does not wait for a `wal-g
		// delete` in flight. Until the lease expires (a roll) only background work waits:
		// the CNPG-I listener and the webhooks do not need the lease.
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	if err := mgr.Add(&cmd.ZombieProcessReaper{}); err != nil {
		setupLog.Error(err, "unable to create zombie process reaper runnable")
		return err
	}

	backupDeletionController := controller.NewBackupDeletionController(mgr.GetClient())
	// uncached: R2BucketLocks are read once per lock refusal, no informer for them
	backupDeletionController.APIReader = mgr.GetAPIReader()
	if err := mgr.Add(backupDeletionController); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BackupDeletionController")
		return err
	}

	if err = (&controller.BackupConfigReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr, backupDeletionController); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BackupConfigReconciler")
		return err
	}

	// Create and add the RetentionController
	retentionController := controller.NewRetentionController(
		mgr.GetClient(),
		10*time.Minute, // Run retention check each 10 minutes
	)
	if err := mgr.Add(retentionController); err != nil {
		setupLog.Error(err, "unable to add controller", "controller", "RetentionController")
		return err
	}

	// Create and add the BackupConfigStatusController
	statusInterval := viper.GetDuration(BackupConfigStatusIntervalKey)
	if statusInterval <= 0 {
		err := fmt.Errorf("--%s must be positive, got %s", BackupConfigStatusIntervalKey, statusInterval)
		setupLog.Error(err, "invalid flag")
		return err
	}
	backupConfigStatusController := controller.NewBackupConfigStatusController(
		mgr.GetClient(),
		statusInterval,
		viper.GetDuration(StorageCheckTimeoutKey), // Budget of one 'wal-g st check' call
	)
	if err := mgr.Add(backupConfigStatusController); err != nil {
		setupLog.Error(err, "unable to add controller", "controller", "BackupConfigStatusController")
		return err
	}

	// Register the BackupReconciler
	if err = (&controller.BackupReconciler{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		DeletionController: backupDeletionController,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BackupReconciler")
		return err
	}

	// Register the SecretReconciler
	if err = (&controller.SecretReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SecretReconciler")
		return err
	}

	// Register the ConfigMapReconciler
	if err = (&controller.ConfigMapReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ConfigMapReconciler")
		return err
	}

	// nolint:goconst
	kubeClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create Kubernetes client")
		os.Exit(1)
	}
	if err := ensurePKI(ctx, kubeClient); err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	if err = webhookv1.SetupBackupConfigWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "PostgresqlCluster")
		os.Exit(1)
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			return err
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		return err
	}

	cnpgiConfig := CNPGI{
		Client:         mgr.GetClient(),
		Cache:          mgr.GetCache(),
		ServerAddress:  viper.GetString("cnpg-i-bind-address"),
		PluginPath:     viper.GetString("cnpg-i-socket-path"),
		ServerCertPath: viper.GetString("cnpg-i-cert-path"),
		ServerCertName: viper.GetString("cnpg-i-cert-name"),
		ServerCertKey:  viper.GetString("cnpg-i-cert-key"),
		ClientCertPath: viper.GetString("cnpg-i-client-cert-path"),
		ClientCertName: viper.GetString("cnpg-i-client-cert-name"),
	}

	if err := mgr.Add(&cnpgiConfig); err != nil {
		setupLog.Error(err, "unable to create CNPGI runnable")
		return err
	}
	if err := mgr.AddReadyzCheck("cnpg-i", cnpgiConfig.ListenerCheck); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		return err
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		return err
	}

	return nil
}

// ensurePKI sets up the necessary Public Key Infrastructure (PKI) for the operator,
// including generating and installing certificates into the webhook configuration.
func ensurePKI(
	ctx context.Context,
	kubeClient client.Client,
) error {
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)
	operatorNamespace, _, err := kubeConfig.Namespace()
	if err != nil {
		return err
	}
	// We need to self-manage required PKI infrastructure and install the certificates into
	// the webhooks configuration
	pkiConfig := certs.PublicKeyInfrastructure{
		CaSecretName:                       CaSecretName,
		CertDir:                            defaultWebhookCertDir,
		SecretName:                         WebhookSecretName,
		ServiceName:                        WebhookServiceName,
		OperatorNamespace:                  operatorNamespace,
		MutatingWebhookConfigurationName:   MutatingWebhookConfigurationName,
		ValidatingWebhookConfigurationName: ValidatingWebhookConfigurationName,
		OperatorDeploymentLabelSelector:    OperatorDeploymentLabelSelector,
	}
	err = pkiConfig.Setup(ctx, kubeClient)
	if err != nil {
		setupLog.Error(err, "unable to setup PKI infrastructure")
	}
	return err
}
