package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wdvn/kit/core/driven/sqlite"
)

// Mutex-based queue manager for benchmark comparison
type mutexQueueManager struct {
	paused map[string]bool
	mu     sync.RWMutex
}

func (m *mutexQueueManager) IsPaused(queueName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.paused[queueName]
}

// BenchmarkLockless_IsPaused measures lockless atomic Load-Acquire performance under high concurrency.
func BenchmarkLockless_IsPaused(b *testing.B) {
	qm, cleanup := setupTestBenchQueue(b)
	defer cleanup()

	queueName := "bench_queue"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = qm.IsPaused(queueName)
		}
	})
}

// BenchmarkMutex_IsPaused measures traditional RWMutex performance under high concurrency.
func BenchmarkMutex_IsPaused(b *testing.B) {
	mqm := &mutexQueueManager{
		paused: map[string]bool{"bench_queue": false},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = mqm.IsPaused("bench_queue")
		}
	})
}

// BenchmarkQueue_Enqueue measures job enqueue throughput.
func BenchmarkQueue_Enqueue(b *testing.B) {
	db, ttl := sqlite.MustOpen(sqlite.Config{Path: ":memory:"})
	_ = ttl
	qm, err := New(db)
	if err != nil {
		b.Fatalf("failed to create queue: %v", err)
	}
	defer qm.Stop()

	ctx := context.Background()
	payload := map[string]string{"event": "user_signup", "email": "bench@example.com"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := qm.Enqueue(ctx, "bench_enqueue", "task", payload)
		if err != nil {
			b.Fatalf("enqueue error: %v", err)
		}
	}
}

// BenchmarkQueue_WorkerPop measures atomic SQLite job pop throughput across workers.
func BenchmarkQueue_WorkerPop(b *testing.B) {
	db, ttl := sqlite.MustOpen(sqlite.Config{Path: ":memory:"})
	_ = ttl
	qm, err := New(db)
	if err != nil {
		b.Fatalf("failed to create queue: %v", err)
	}
	defer qm.Stop()

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = qm.Enqueue(ctx, "pop_queue", "task", "payload")
			_, _ = qm.popNextJob(ctx, "pop_queue", 30*time.Second)
		}
	})
}

func setupTestBenchQueue(b *testing.B, opts ...Config) (*QueueManager, func()) {
	db, ttl := sqlite.MustOpen(sqlite.Config{
		Path:        ":memory:",
		AutoMigrate: []any{},
	})
	_ = ttl

	qm, err := New(db, opts...)
	if err != nil {
		b.Fatalf("failed to create queue manager: %v", err)
	}

	cleanup := func() {
		qm.Stop()
	}

	return qm, cleanup
}
