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
	"sync"
	"syscall"
)

// Exit statuses are delivered per pid. The old broadcast (every exit sent to every
// subscriber over a buffered channel, under the lock) blocked the reaper for good
// once a subscriber that had stopped reading had 8 undelivered exits, and with it
// every later wal-g call; more concurrent reconciles made that likelier.
var (
	waiters   = map[int]chan syscall.WaitStatus{}
	waitersMx sync.Mutex
)

// startAndAwait runs start (which starts the process and returns its pid) under the
// lock, so the reaper cannot deliver the pid's exit before its waiter is registered.
func startAndAwait(start func() (int, error)) (int, <-chan syscall.WaitStatus, error) {
	waitersMx.Lock()
	defer waitersMx.Unlock()
	pid, err := start()
	if err != nil {
		return 0, nil, err
	}
	ch := make(chan syscall.WaitStatus, 1)
	waiters[pid] = ch
	return pid, ch, nil
}

// stopAwaiting drops the waiter of pid (its caller gave up waiting).
func stopAwaiting(pid int) {
	waitersMx.Lock()
	defer waitersMx.Unlock()
	delete(waiters, pid)
}

// notifyAllSubscribers hands the exit status of pid to its waiter, if any. Never blocks.
func notifyAllSubscribers(pid int, wstatus syscall.WaitStatus) {
	waitersMx.Lock()
	defer waitersMx.Unlock()
	if ch, ok := waiters[pid]; ok {
		delete(waiters, pid)
		ch <- wstatus
	}
}
