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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1beta1 "github.com/wal-g/cnpg-plugin-wal-g/api/v1beta1"
)

// drainQueue empties the status queue without running any reconcile.
func drainQueue(c *BackupConfigStatusController) []string {
	var keys []string
	for len(c.queue) > 0 {
		keys = append(keys, (<-c.queue).String())
	}
	return keys
}

var _ = Describe("BackupConfigStatusController queue", func() {
	var (
		controller *BackupConfigStatusController
		keys       []types.NamespacedName
	)

	BeforeEach(func() {
		var objs []client.Object
		keys = nil
		for i := 0; i < 3; i++ {
			name := fmt.Sprintf("bc-%d", i)
			objs = append(objs, &v1beta1.BackupConfig{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}})
			keys = append(keys, types.NamespacedName{Namespace: "ns", Name: name})
		}
		controller = NewBackupConfigStatusController(setupFakeBackupClient(objs...), 2*time.Minute, 0)
		controller.logger, _ = newRecordingLogger()
		// No worker goroutines: the pool counts as full, so the queue stays observable.
		controller.workers = maxStatusReconcileWorkers
	})

	It("queues the keys missing from the queue when the previous pass is still running", func() {
		// The previous pass left bc-0 queued (it outlasted checkInterval).
		Expect(controller.EnqueueStatusUpdate(keys[0])).To(BeTrue())

		controller.enqueueAllStatuses(context.Background())
		controller.enqueueAllStatuses(context.Background())

		// Up to v0.3.0-maestra.15 the pass logged "Cannot enqueue backupconfigs for
		// status update" and queued nothing, so bc-1 and bc-2 waited a whole interval.
		Expect(drainQueue(controller)).To(ConsistOf("ns/bc-0", "ns/bc-1", "ns/bc-2"))
	})

	It("queues a key again once a worker has taken it", func() {
		Expect(controller.EnqueueStatusUpdate(keys[0])).To(BeTrue())
		Expect(controller.EnqueueStatusUpdate(keys[0])).To(BeFalse())

		Expect(controller.tryAcquire((<-controller.queue).String())).To(BeTrue())
		Expect(controller.EnqueueStatusUpdate(keys[0])).To(BeTrue())
		Expect(drainQueue(controller)).To(Equal([]string{"ns/bc-0"}))
	})

	It("sizes the pool to the number of BackupConfigs", func() {
		Expect(statusWorkersFor(0, 2*time.Minute)).To(Equal(minStatusReconcileWorkers))
		Expect(statusWorkersFor(7, 2*time.Minute)).To(Equal(3))   // omicron, iota
		Expect(statusWorkersFor(79, 2*time.Minute)).To(Equal(14)) // omega: 79 x 15s / 90s
		Expect(statusWorkersFor(1000, 2*time.Minute)).To(Equal(maxStatusReconcileWorkers))

		controller.enqueueAllStatuses(context.Background())
		Expect(controller.wantWorkers).To(Equal(minStatusReconcileWorkers))
	})

	It("retires surplus workers down to the wanted size", func() {
		controller.workers, controller.wantWorkers = 5, 2
		retired := 0
		for controller.retireWorker() {
			retired++
		}
		Expect(retired).To(Equal(3))
		Expect(controller.workers).To(Equal(2))
	})
})
