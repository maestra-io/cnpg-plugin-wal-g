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
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	cnpgihttp "github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/http"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
)

const (
	// listenerDrain is how long the listener keeps serving after SIGTERM: CNPG dials the
	// Service until the endpoint removal reaches its node, and reconciles every cluster of
	// the plugin at once when the EndpointSlice changes.
	listenerDrain = 3 * time.Second

	// listenerGracefulStop is the time in-flight calls then get to finish. Together with
	// listenerDrain it stays under the Pod's terminationGracePeriodSeconds (10): the
	// leader-elected controllers stop only after the listener.
	listenerGracefulStop = 2 * time.Second

	// listenerDialTimeout is under the 1s default timeout of the kubelet's readiness probe.
	listenerDialTimeout = 500 * time.Millisecond
)

// CNPGI is the implementation of the CNPG-i server for operator
type CNPGI struct {
	Client client.Client
	// Cache is the cache behind Client
	Cache          cache.Cache
	ServerAddress  string
	PluginPath     string
	ServerCertPath string
	ServerCertName string
	ServerCertKey  string
	ClientCertPath string
	ClientCertName string
}

// NeedLeaderElection makes every Pod serve CNPG-I, leader or not. The calls come from the one
// CNPG leader, one at a time per Cluster, and what the hooks write (Role, RoleBinding, encryption
// Secret) is create-if-missing or a patch computed from the Cluster and its BackupConfigs.
// Behind the lease, a roll left CNPG without the plugin until the old Pod's lease expired
// (60s+, every Cluster in an error phase; issues-maestra#1786).
func (c *CNPGI) NeedLeaderElection() bool {
	return false
}

// Start starts the GRPC server
// of the operator plugin
func (c *CNPGI) Start(ctx context.Context) error {
	if err := c.syncHooksCache(ctx); err != nil {
		return err
	}

	serveCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	defer stop()

	enrich := func(server *grpc.Server) error {
		lifecycle.RegisterOperatorLifecycleServer(server, LifecycleImplementation{
			Client: c.Client,
		})
		reconciler.RegisterReconcilerHooksServer(server, ReconcilerImplementation{
			Client: c.Client,
		})
		go func() {
			<-ctx.Done()
			time.Sleep(listenerDrain)
			// Canceling serveCtx makes cnpgihttp.Server drop the calls still running
			defer time.AfterFunc(listenerGracefulStop, stop).Stop()
			server.GracefulStop()
		}()
		return nil
	}

	srv := cnpgihttp.Server{
		IdentityImpl:  IdentityImplementation{},
		Enrichers:     []cnpgihttp.ServerEnricher{enrich},
		PluginPath:    c.PluginPath,
		ServerAddress: c.ServerAddress,
	}

	if len(c.ServerCertPath) > 0 {
		srv.ServerCertPath = filepath.Join(c.ServerCertPath, c.ServerCertName)
		srv.ServerKeyPath = filepath.Join(c.ServerCertPath, c.ServerCertKey)
	}

	if len(c.ClientCertPath) > 0 {
		srv.ClientCertPath = filepath.Join(c.ClientCertPath, c.ClientCertName)
	}

	if err := srv.Start(serveCtx); err != nil {
		return err
	}
	if ctx.Err() == nil {
		// cnpgihttp.Server returns no error when Serve fails
		return errors.New("the CNPG-I server stopped unexpectedly")
	}
	return nil
}

// syncHooksCache waits for the informers of everything the hooks read. Without it the first
// calls to a Pod that is not the leader (no controller started, so no informer) would wait for
// the initial lists, the Secrets of the whole cluster among them.
func (c *CNPGI) syncHooksCache(ctx context.Context) error {
	for _, obj := range []client.Object{
		&v1beta1.BackupConfig{}, &corev1.Secret{}, &rbacv1.Role{}, &rbacv1.RoleBinding{},
	} {
		if _, err := c.Cache.GetInformer(ctx, obj); err != nil {
			return fmt.Errorf("while syncing the %T cache: %w", obj, err)
		}
	}
	return nil
}

// ListenerCheck is the readiness check of the CNPG-I listener: the Service must not route to a
// Pod that refuses connections, and a roll must not stop the old Pod before the new one serves.
func (c *CNPGI) ListenerCheck(_ *http.Request) error {
	conn, err := net.DialTimeout("tcp", c.ServerAddress, listenerDialTimeout)
	if err != nil {
		return fmt.Errorf("the CNPG-I listener is not serving: %w", err)
	}
	return conn.Close()
}
