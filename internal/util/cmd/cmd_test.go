package cmd

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Many concurrent subprocesses, some of them abandoned at their deadline, all
// get their own exit status and none blocks the reaper.
func TestRunConcurrentWithReaper(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = (&ZombieProcessReaper{}).Start(ctx) }()
	time.Sleep(50 * time.Millisecond) // the reaper subscribes to SIGCHLD

	// abandoned waiters: their deadline passes before the process exits
	for range 20 {
		short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
		_, err := New("sh", "-c", "sleep 0.2").WithContext(short).Run()
		stop()
		if err == nil {
			t.Fatal("a run past its deadline must fail")
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code := "0"
			if i%2 == 1 {
				code = "3"
			}
			runCtx, stop := context.WithTimeout(ctx, 20*time.Second)
			defer stop()
			res, err := New("sh", "-c", "exit "+code).WithContext(runCtx).Run()
			if res.State() == nil {
				errs <- err
				return
			}
			if got := res.State().ExitCode(); (code == "0") != (got == 0) || (err == nil) != (got == 0) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("wrong result: %v", err)
	}
}
