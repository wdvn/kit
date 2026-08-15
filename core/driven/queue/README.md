# Driven Queue Module (`core/driven/queue`)

A BullMQ-inspired background job queue system for `github.com/wdvn/kit` built on top of GORM and SQLite.

## Features

- **Job States**: `waiting`, `delayed`, `active`, `completed`, `failed`.
- **Delayed Jobs**: Schedule jobs to run at a future timestamp or after a delay.
- **Retries & Backoff**: Retries failed jobs with `Fixed` or `Exponential` backoff strategies.
- **Priority Queues**: Jobs with higher `Priority` values are processed first.
- **Worker Pools & Concurrency**: Configurable concurrent worker routines.
- **Lock Protection & Housekeeping**: Automatic recovery of stalled jobs if a worker crashes.
- **Queue Controls**: Pause, resume, retry, progress tracking (`UpdateProgress`), and job cleaning.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/wdvn/kit/core/driven/queue"
	"github.com/wdvn/kit/core/driven/sqlite"
)

func main() {
	db, _ := sqlite.MustOpen(sqlite.Config{Path: "app.db"})

	// Initialize queue manager
	qm, err := queue.New(db)
	if err != nil {
		log.Fatal(err)
	}
	defer qm.Stop()

	// Register worker for "email" queue
	qm.RegisterWorker("email", func(ctx context.Context, job *queue.Job) (any, error) {
		fmt.Printf("Processing email job payload: %s\n", job.Payload)
		return "sent", nil
	}, queue.WorkerOptions{Concurrency: 3})

	// Enqueue a job with delay and retries
	qm.Enqueue(context.Background(), "email", "send_welcome", map[string]string{"to": "user@example.com"}, queue.JobOptions{
		Delay:       2 * time.Second,
		MaxAttempts: 5,
		Priority:    10,
		Backoff: queue.BackoffConfig{
			Type:  queue.BackoffExponential,
			Delay: 1 * time.Second,
		},
	})
}
```
