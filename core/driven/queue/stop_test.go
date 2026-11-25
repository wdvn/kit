package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Stop must be idempotent: the app now calls it when the process is asked to
// exit, while callers commonly also `defer qm.Stop()`. Before this the second
// call closed an already-closed channel and panicked.
func TestStopIsIdempotent(t *testing.T) {
	qm, _ := setupTestQueue(t)

	qm.RegisterWorker("idem", func(ctx context.Context, job *Job) (any, error) {
		return "ok", nil
	}, WorkerOptions{Concurrency: 2, PollInterval: 10 * time.Millisecond})

	qm.Stop()
	qm.Stop()
	qm.Stop()
}

// Concurrent Stop calls must all return and none may panic.
func TestStopIsSafeUnderConcurrency(t *testing.T) {
	qm, _ := setupTestQueue(t)
	qm.RegisterWorker("conc", func(ctx context.Context, job *Job) (any, error) {
		return "ok", nil
	}, WorkerOptions{Concurrency: 4, PollInterval: 10 * time.Millisecond})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qm.Stop()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop calls did not return")
	}
}
