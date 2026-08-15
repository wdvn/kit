package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wdvn/kit/core/driven/sqlite"
)

func setupTestQueue(t *testing.T, opts ...Config) (*QueueManager, func()) {
	db, ttl := sqlite.MustOpen(sqlite.Config{
		Path:        ":memory:",
		AutoMigrate: []any{},
	})
	_ = ttl

	qm, err := New(db, opts...)
	if err != nil {
		t.Fatalf("failed to create queue manager: %v", err)
	}

	cleanup := func() {
		qm.Stop()
	}

	return qm, cleanup
}

func TestQueue_EnqueueAndProcess(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	var processed uint32
	doneCh := make(chan string, 1)

	handler := func(ctx context.Context, job *Job) (any, error) {
		atomic.AddUint32(&processed, 1)
		doneCh <- job.Payload
		return "success", nil
	}

	err := qm.RegisterWorker("emails", handler, WorkerOptions{
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	ctx := context.Background()
	job, err := qm.Enqueue(ctx, "emails", "send_welcome", map[string]string{"email": "test@example.com"})
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	select {
	case payload := <-doneCh:
		if atomic.LoadUint32(&processed) != 1 {
			t.Errorf("expected 1 processed job, got %d", processed)
		}
		t.Logf("job payload processed: %s", payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to process")
	}

	// Verify job state in database
	processedJob, err := qm.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if processedJob == nil {
		t.Fatal("expected job record, got nil")
	}
	if processedJob.State != StateCompleted {
		t.Errorf("expected state %s, got %s", StateCompleted, processedJob.State)
	}
	if processedJob.Progress != 100 {
		t.Errorf("expected progress 100, got %d", processedJob.Progress)
	}
	if processedJob.Result != "success" {
		t.Errorf("expected result \"success\", got %s", processedJob.Result)
	}
}

func TestQueue_DelayedJob(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	var executedAt time.Time
	doneCh := make(chan time.Time, 1)

	handler := func(ctx context.Context, job *Job) (any, error) {
		doneCh <- time.Now()
		return nil, nil
	}

	_ = qm.RegisterWorker("delayed_queue", handler, WorkerOptions{
		PollInterval: 50 * time.Millisecond,
	})

	startTime := time.Now()
	delay := 300 * time.Millisecond

	_, err := qm.Enqueue(context.Background(), "delayed_queue", "delayed_task", "data", JobOptions{
		Delay: delay,
	})
	if err != nil {
		t.Fatalf("failed to enqueue delayed job: %v", err)
	}

	select {
	case executedAt = <-doneCh:
		elapsed := executedAt.Sub(startTime)
		if elapsed < delay-50*time.Millisecond {
			t.Errorf("job executed too early: elapsed %v, expected at least %v", elapsed, delay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delayed job")
	}
}

func TestQueue_RetryWithBackoff(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	var attempts uint32
	doneCh := make(chan struct{}, 1)

	handler := func(ctx context.Context, job *Job) (any, error) {
		cnt := atomic.AddUint32(&attempts, 1)
		if cnt < 3 {
			return nil, errors.New("temporary error")
		}
		doneCh <- struct{}{}
		return "ok", nil
	}

	_ = qm.RegisterWorker("retry_queue", handler, WorkerOptions{
		PollInterval: 50 * time.Millisecond,
	})

	_, err := qm.Enqueue(context.Background(), "retry_queue", "retry_task", nil, JobOptions{
		MaxAttempts: 3,
		Backoff: BackoffConfig{
			Type:  BackoffFixed,
			Delay: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("failed to enqueue retry job: %v", err)
	}

	select {
	case <-doneCh:
		if atomic.LoadUint32(&attempts) != 3 {
			t.Errorf("expected 3 attempts, got %d", attempts)
		}
	case <-time.After(4 * time.Second):
		t.Fatalf("timed out waiting for retry job, attempts: %d", atomic.LoadUint32(&attempts))
	}
}

func TestQueue_PriorityOrdering(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	ctx := context.Background()
	queueName := "priority_queue"

	// Enqueue jobs with different priorities before starting worker
	_, _ = qm.Enqueue(ctx, queueName, "low_prio", "low", JobOptions{Priority: 1})
	_, _ = qm.Enqueue(ctx, queueName, "high_prio", "high", JobOptions{Priority: 10})
	_, _ = qm.Enqueue(ctx, queueName, "mid_prio", "mid", JobOptions{Priority: 5})

	var mu sync.Mutex
	var order []string
	doneCh := make(chan struct{}, 3)

	handler := func(ctx context.Context, job *Job) (any, error) {
		mu.Lock()
		order = append(order, job.Name)
		mu.Unlock()
		doneCh <- struct{}{}
		return nil, nil
	}

	_ = qm.RegisterWorker(queueName, handler, WorkerOptions{
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
	})

	for i := 0; i < 3; i++ {
		select {
		case <-doneCh:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for priority jobs")
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(order))
	}
	if order[0] != "high_prio" || order[1] != "mid_prio" || order[2] != "low_prio" {
		t.Errorf("unexpected priority execution order: %v", order)
	}
}

func TestQueue_ConcurrencyWorkerPool(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	const jobCount = 20
	var count uint32

	handler := func(ctx context.Context, job *Job) (any, error) {
		time.Sleep(20 * time.Millisecond)
		atomic.AddUint32(&count, 1)
		return nil, nil
	}

	_ = qm.RegisterWorker("concurrent_queue", handler, WorkerOptions{
		Concurrency:  5,
		PollInterval: 20 * time.Millisecond,
	})

	ctx := context.Background()
	for i := 0; i < jobCount; i++ {
		_, err := qm.Enqueue(ctx, "concurrent_queue", "task", i)
		if err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	start := time.Now()
	for atomic.LoadUint32(&count) < jobCount {
		if time.Since(start) > 5*time.Second {
			t.Fatalf("timed out waiting for concurrent jobs, completed: %d/%d", count, jobCount)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if atomic.LoadUint32(&count) != jobCount {
		t.Errorf("expected %d jobs completed, got %d", jobCount, count)
	}
}

func TestQueue_PauseResume(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	queueName := "paused_queue"
	qm.Pause(queueName)

	if !qm.IsPaused(queueName) {
		t.Error("expected queue to be paused")
	}

	var processed uint32
	handler := func(ctx context.Context, job *Job) (any, error) {
		atomic.AddUint32(&processed, 1)
		return nil, nil
	}

	_ = qm.RegisterWorker(queueName, handler, WorkerOptions{
		PollInterval: 50 * time.Millisecond,
	})

	_, _ = qm.Enqueue(context.Background(), queueName, "paused_task", "test")

	time.Sleep(300 * time.Millisecond)
	if atomic.LoadUint32(&processed) != 0 {
		t.Errorf("expected 0 processed while paused, got %d", processed)
	}

	qm.Resume(queueName)
	if qm.IsPaused(queueName) {
		t.Error("expected queue to be resumed")
	}

	time.Sleep(500 * time.Millisecond)
	if atomic.LoadUint32(&processed) != 1 {
		t.Errorf("expected 1 processed after resume, got %d", processed)
	}
}

func TestQueue_ProgressAndClean(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	ctx := context.Background()
	job, err := qm.Enqueue(ctx, "clean_queue", "clean_task", "data")
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	err = qm.UpdateProgress(ctx, job.ID, 50)
	if err != nil {
		t.Fatalf("failed to update progress: %v", err)
	}

	updatedJob, _ := qm.GetJob(ctx, job.ID)
	if updatedJob.Progress != 50 {
		t.Errorf("expected progress 50, got %d", updatedJob.Progress)
	}

	// Manually set state to completed and update time to past
	qm.db.Model(&Job{}).Where("id = ?", job.ID).Updates(map[string]any{
		"state":      StateCompleted,
		"updated_at": time.Now().Add(-2 * time.Hour),
	})

	cleaned, err := qm.Clean(ctx, "clean_queue", StateCompleted, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to clean: %v", err)
	}
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned job, got %d", cleaned)
	}

	cleanedJob, _ := qm.GetJob(ctx, job.ID)
	if cleanedJob != nil {
		t.Error("expected job to be cleaned/deleted")
	}
}

func TestQueue_RetryJob(t *testing.T) {
	qm, cleanup := setupTestQueue(t)
	defer cleanup()

	ctx := context.Background()
	job, _ := qm.Enqueue(ctx, "retry_manual", "task", "data")

	// Set job state to failed
	qm.db.Model(&Job{}).Where("id = ?", job.ID).Updates(map[string]any{
		"state":         StateFailed,
		"failed_reason": "some error",
		"attempts":      3,
	})

	err := qm.RetryJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to retry job: %v", err)
	}

	retriedJob, _ := qm.GetJob(ctx, job.ID)
	if retriedJob.State != StateWaiting {
		t.Errorf("expected state %s, got %s", StateWaiting, retriedJob.State)
	}
	if retriedJob.Attempts != 0 {
		t.Errorf("expected 0 attempts, got %d", retriedJob.Attempts)
	}
}
