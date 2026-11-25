package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// JobState represents the current execution state of a Job.
type JobState string

const (
	StateWaiting   JobState = "waiting"
	StateDelayed   JobState = "delayed"
	StateActive    JobState = "active"
	StateCompleted JobState = "completed"
	StateFailed    JobState = "failed"
)

// BackoffType represents retry backoff calculation strategies.
type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

// Job represents a background task queued and processed via SQLite.
type Job struct {
	ID           string      `gorm:"primaryKey;size:64" json:"id"`
	Queue        string      `gorm:"size:64;index:idx_queue_state" json:"queue"`
	Name         string      `gorm:"size:128;index" json:"name"`
	Payload      string      `gorm:"type:text" json:"payload"`
	State        JobState    `gorm:"size:20;index:idx_queue_state" json:"state"`
	Priority     int         `gorm:"default:0;index" json:"priority"`
	Attempts     int         `gorm:"default:0" json:"attempts"`
	MaxAttempts  int         `gorm:"default:3" json:"max_attempts"`
	BackoffType  BackoffType `gorm:"size:20;default:'fixed'" json:"backoff_type"`
	BackoffDelay int64       `gorm:"default:1000" json:"backoff_delay"` // milliseconds
	RunAt        time.Time   `gorm:"index" json:"run_at"`
	LockedAt     *time.Time  `json:"locked_at,omitempty"`
	LockToken    string      `gorm:"size:64" json:"lock_token,omitempty"`
	Progress     int         `gorm:"default:0" json:"progress"`
	Result       string      `gorm:"type:text" json:"result,omitempty"`
	FailedReason string      `gorm:"type:text" json:"failed_reason,omitempty"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
	CompletedAt  *time.Time  `json:"completed_at,omitempty"`
	FailedAt     *time.Time  `json:"failed_at,omitempty"`

	// Memory-only options for job behavior
	removeOnComplete bool `gorm:"-"`
	removeOnFail     bool `gorm:"-"`
}

// TableName returns the GORM table name for jobs.
func (Job) TableName() string {
	return "kit_jobs"
}

// BackoffConfig specifies how a job retries when failing.
type BackoffConfig struct {
	Type  BackoffType
	Delay time.Duration
}

// JobOptions configures enqueue parameters for a Job.
type JobOptions struct {
	JobID            string
	Delay            time.Duration
	RunAt            time.Time
	Priority         int
	MaxAttempts      int
	Backoff          BackoffConfig
	RemoveOnComplete bool
	RemoveOnFail     bool
}

// WorkerOptions configures background worker behavior.
type WorkerOptions struct {
	Concurrency     int           // Number of concurrent goroutines (default: 1)
	LockDuration    time.Duration // Stalled lock timeout (default: 30s)
	PollInterval    time.Duration // Polling interval when queue is idle (default: 500ms)
	StalledInterval time.Duration // Interval to check for stalled jobs (default: 10s)
}

// JobHandler is the function signature for executing a job.
type JobHandler func(ctx context.Context, job *Job) (any, error)

// Config holds settings for QueueManager.
type Config struct {
	AutoMigrate     bool
	DefaultLock     time.Duration
	StalledInterval time.Duration
}

// QueueManager controls job enqueueing, worker processing, and queue state.
type QueueManager struct {
	db     *gorm.DB
	config Config

	workers sync.Map // map[string]*workerPool
	paused  sync.Map // map[string]*atomic.Bool

	stopCh chan struct{}
	wg     sync.WaitGroup

	stopOnce sync.Once
	stopDone sync.WaitGroup
}

type workerPool struct {
	queueName string
	handler   JobHandler
	opts      WorkerOptions
	stopCh    chan struct{}
	wg        sync.WaitGroup
	manager   *QueueManager
}

// New creates a new SQLite-driven QueueManager.
func New(db *gorm.DB, cfg ...Config) (*QueueManager, error) {
	c := Config{
		AutoMigrate:     true,
		DefaultLock:     30 * time.Second,
		StalledInterval: 10 * time.Second,
	}
	if len(cfg) > 0 {
		if cfg[0].DefaultLock > 0 {
			c.DefaultLock = cfg[0].DefaultLock
		}
		if cfg[0].StalledInterval > 0 {
			c.StalledInterval = cfg[0].StalledInterval
		}
		c.AutoMigrate = cfg[0].AutoMigrate
	}

	if c.AutoMigrate {
		if err := db.AutoMigrate(&Job{}); err != nil {
			return nil, fmt.Errorf("queue: failed to auto-migrate job table: %w", err)
		}
	}

	qm := &QueueManager{
		db:     db,
		config: c,
		stopCh: make(chan struct{}),
	}
	qm.stopDone.Add(1)

	// Start background stalled job housekeeper
	qm.wg.Add(1)
	go qm.stalledHousekeeper()

	return qm, nil
}

// Enqueue adds a job to a named queue.
func (q *QueueManager) Enqueue(ctx context.Context, queueName, jobName string, payload any, opts ...JobOptions) (*Job, error) {
	var opt JobOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	jobID := opt.JobID
	if jobID == "" {
		jobID = uuid.New().String()
	}

	payloadStr := ""
	if payload != nil {
		switch v := payload.(type) {
		case string:
			payloadStr = v
		case []byte:
			payloadStr = string(v)
		default:
			b, err := json.Marshal(payload)
			if err != nil {
				return nil, fmt.Errorf("queue: failed to marshal payload: %w", err)
			}
			payloadStr = string(b)
		}
	}

	now := time.Now()
	runAt := now
	if !opt.RunAt.IsZero() {
		runAt = opt.RunAt
	} else if opt.Delay > 0 {
		runAt = now.Add(opt.Delay)
	}

	state := StateWaiting
	if runAt.After(now) {
		state = StateDelayed
	}

	maxAttempts := opt.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	backoffType := opt.Backoff.Type
	if backoffType == "" {
		backoffType = BackoffFixed
	}

	backoffDelay := opt.Backoff.Delay.Milliseconds()
	if backoffDelay <= 0 {
		backoffDelay = 1000
	}

	job := &Job{
		ID:               jobID,
		Queue:            queueName,
		Name:             jobName,
		Payload:          payloadStr,
		State:            state,
		Priority:         opt.Priority,
		Attempts:         0,
		MaxAttempts:      maxAttempts,
		BackoffType:      backoffType,
		BackoffDelay:     backoffDelay,
		RunAt:            runAt,
		CreatedAt:        now,
		UpdatedAt:        now,
		removeOnComplete: opt.RemoveOnComplete,
		removeOnFail:     opt.RemoveOnFail,
	}

	if err := q.db.WithContext(ctx).Create(job).Error; err != nil {
		return nil, fmt.Errorf("queue: failed to enqueue job: %w", err)
	}

	slog.Debug("queue: job enqueued", "id", job.ID, "queue", queueName, "name", jobName, "state", state)
	return job, nil
}

// RegisterWorker starts workers for a queue.
func (q *QueueManager) RegisterWorker(queueName string, handler JobHandler, opts ...WorkerOptions) error {
	var wOpt WorkerOptions
	if len(opts) > 0 {
		wOpt = opts[0]
	}

	if wOpt.Concurrency <= 0 {
		wOpt.Concurrency = 1
	}
	if wOpt.LockDuration <= 0 {
		wOpt.LockDuration = q.config.DefaultLock
	}
	if wOpt.PollInterval <= 0 {
		wOpt.PollInterval = 500 * time.Millisecond
	}

	pool := &workerPool{
		queueName: queueName,
		handler:   handler,
		opts:      wOpt,
		stopCh:    make(chan struct{}),
		manager:   q,
	}

	if _, loaded := q.workers.LoadOrStore(queueName, pool); loaded {
		return fmt.Errorf("queue: worker for queue %q already registered", queueName)
	}

	for i := 0; i < wOpt.Concurrency; i++ {
		pool.wg.Add(1)
		go pool.workerLoop(i)
	}

	slog.Info("queue: registered worker", "queue", queueName, "concurrency", wOpt.Concurrency)
	return nil
}

func (q *QueueManager) getPauseFlag(queueName string) *atomic.Bool {
	val, _ := q.paused.LoadOrStore(queueName, &atomic.Bool{})
	return val.(*atomic.Bool)
}

// Pause pauses processing jobs for a specific queue (lockless store-release).
func (q *QueueManager) Pause(queueName string) {
	q.getPauseFlag(queueName).Store(true)
	slog.Info("queue: queue paused", "queue", queueName)
}

// Resume resumes processing jobs for a specific queue (lockless store-release).
func (q *QueueManager) Resume(queueName string) {
	q.getPauseFlag(queueName).Store(false)
	slog.Info("queue: queue resumed", "queue", queueName)
}

// IsPaused returns true if the queue is currently paused (lockless load-acquire).
func (q *QueueManager) IsPaused(queueName string) bool {
	val, ok := q.paused.Load(queueName)
	if !ok {
		return false
	}
	return val.(*atomic.Bool).Load()
}

// GetJob fetches a job by ID.
func (q *QueueManager) GetJob(ctx context.Context, id string) (*Job, error) {
	var job Job
	if err := q.db.WithContext(ctx).First(&job, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

// ListJobs retrieves jobs matching criteria.
func (q *QueueManager) ListJobs(ctx context.Context, queueName string, state JobState, limit, offset int) ([]Job, error) {
	var jobs []Job
	tx := q.db.WithContext(ctx).Where("queue = ?", queueName)
	if state != "" {
		tx = tx.Where("state = ?", state)
	}
	if limit <= 0 {
		limit = 50
	}
	if err := tx.Order("created_at DESC").Limit(limit).Offset(offset).Find(&jobs).Error; err != nil {
		return nil, err
	}
	return jobs, nil
}

// RetryJob resets a failed job to waiting state for re-execution.
func (q *QueueManager) RetryJob(ctx context.Context, id string) error {
	now := time.Now()
	res := q.db.WithContext(ctx).Model(&Job{}).
		Where("id = ? AND state = ?", id, StateFailed).
		Updates(map[string]any{
			"state":         StateWaiting,
			"attempts":      0,
			"run_at":        now,
			"failed_reason": "",
			"failed_at":     nil,
			"locked_at":     nil,
			"lock_token":    "",
			"updated_at":    now,
		})

	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("queue: job %q not found or not in failed state", id)
	}
	return nil
}

// RemoveJob deletes a job record from the database.
func (q *QueueManager) RemoveJob(ctx context.Context, id string) error {
	return q.db.WithContext(ctx).Delete(&Job{}, "id = ?", id).Error
}

// Clean deletes jobs matching state older than specified duration.
func (q *QueueManager) Clean(ctx context.Context, queueName string, state JobState, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	res := q.db.WithContext(ctx).
		Where("queue = ? AND state = ? AND updated_at <= ?", queueName, state, cutoff).
		Delete(&Job{})
	return res.RowsAffected, res.Error
}

// UpdateProgress updates the progress percentage of a job.
func (q *QueueManager) UpdateProgress(ctx context.Context, id string, progress int) error {
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	return q.db.WithContext(ctx).Model(&Job{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"progress":   progress,
			"updated_at": time.Now(),
		}).Error
}

// Stop gracefully stops all workers and background housekeeping routines.
//
// It is idempotent: the app calls it during shutdown, and callers commonly also
// have `defer qm.Stop()`. A second call returns immediately instead of closing
// an already-closed channel (which panics).
func (q *QueueManager) Stop() {
	q.stopOnce.Do(q.stop)

	// Wait outside the Once so concurrent callers all block until the workers
	// have actually finished, while the teardown itself runs exactly once.
	q.stopDone.Wait()
}

func (q *QueueManager) stop() {
	defer q.stopDone.Done()

	// Never nil out stopCh: the housekeeper re-reads this field on every loop
	// iteration, so setting it to nil lets it evaluate `case <-nil`, which
	// blocks forever and makes Stop hang on q.wg.Wait(). Closing once (guarded
	// by stopOnce) is enough for a closed channel to stay permanently ready.
	close(q.stopCh)

	q.workers.Range(func(key, value any) bool {
		pool := value.(*workerPool)
		close(pool.stopCh)
		pool.wg.Wait()
		q.workers.Delete(key)
		return true
	})

	q.wg.Wait()
	slog.Info("queue: stopped all workers and housekeeper")
}

// workerLoop executes worker goroutines.
func (w *workerPool) workerLoop(workerID int) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		default:
			job, err := w.manager.popNextJob(context.Background(), w.queueName, w.opts.LockDuration)
			if err != nil {
				slog.Error("queue: worker pop error", "queue", w.queueName, "worker", workerID, "error", err)
				time.Sleep(w.opts.PollInterval)
				continue
			}

			if job == nil {
				select {
				case <-w.stopCh:
					return
				case <-ticker.C:
					continue
				}
			}

			w.processJob(job)
		}
	}
}

// processJob executes job handler and manages result/retry lifecycle.
func (w *workerPool) processJob(job *Job) {
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.LockDuration)
	defer cancel()

	job.Attempts++
	w.manager.db.Model(&Job{}).Where("id = ?", job.ID).Update("attempts", job.Attempts)

	slog.Debug("queue: processing job", "id", job.ID, "queue", job.Queue, "attempt", job.Attempts)

	res, err := w.handler(ctx, job)
	completedTime := time.Now()

	if err == nil {
		// Job completed successfully
		resStr := ""
		if res != nil {
			switch v := res.(type) {
			case string:
				resStr = v
			case []byte:
				resStr = string(v)
			default:
				if b, marshalErr := json.Marshal(res); marshalErr == nil {
					resStr = string(b)
				}
			}
		}

		if job.removeOnComplete {
			w.manager.db.Delete(&Job{}, "id = ?", job.ID)
		} else {
			w.manager.db.Model(&Job{}).
				Where("id = ? AND lock_token = ?", job.ID, job.LockToken).
				Updates(map[string]any{
					"state":        StateCompleted,
					"result":       resStr,
					"progress":     100,
					"completed_at": completedTime,
					"locked_at":    nil,
					"lock_token":   "",
					"updated_at":   completedTime,
				})
		}
		slog.Debug("queue: job completed", "id", job.ID)
		return
	}

	// Job execution failed
	failedErrStr := err.Error()
	if job.Attempts >= job.MaxAttempts {
		// Terminal failure
		if job.removeOnFail {
			w.manager.db.Delete(&Job{}, "id = ?", job.ID)
		} else {
			w.manager.db.Model(&Job{}).
				Where("id = ? AND lock_token = ?", job.ID, job.LockToken).
				Updates(map[string]any{
					"state":         StateFailed,
					"failed_reason": failedErrStr,
					"failed_at":     completedTime,
					"locked_at":     nil,
					"lock_token":    "",
					"updated_at":    completedTime,
				})
		}
		slog.Warn("queue: job failed permanently", "id", job.ID, "attempts", job.Attempts, "error", failedErrStr)
		return
	}

	// Schedule retry backoff
	var backoffDur time.Duration
	if job.BackoffType == BackoffExponential {
		mult := math.Pow(2, float64(job.Attempts-1))
		backoffDur = time.Duration(float64(job.BackoffDelay)*mult) * time.Millisecond
	} else {
		backoffDur = time.Duration(job.BackoffDelay) * time.Millisecond
	}

	nextRunAt := completedTime.Add(backoffDur)
	w.manager.db.Model(&Job{}).
		Where("id = ? AND lock_token = ?", job.ID, job.LockToken).
		Updates(map[string]any{
			"state":         StateDelayed,
			"run_at":        nextRunAt,
			"failed_reason": failedErrStr,
			"locked_at":     nil,
			"lock_token":    "",
			"updated_at":    completedTime,
		})

	slog.Info("queue: job scheduled for retry", "id", job.ID, "attempt", job.Attempts, "next_run", nextRunAt)
}

// popNextJob retrieves the next available job for processing atomically.
func (q *QueueManager) popNextJob(ctx context.Context, queueName string, lockDuration time.Duration) (*Job, error) {
	if q.IsPaused(queueName) {
		return nil, nil
	}

	now := time.Now()
	lockToken := uuid.New().String()

	var job Job
	err := q.db.WithContext(ctx).Raw(`
		UPDATE kit_jobs 
		SET state = ?, locked_at = ?, lock_token = ?, updated_at = ? 
		WHERE id = (
			SELECT id FROM kit_jobs 
			WHERE queue = ? AND state IN (?, ?) AND run_at <= ? 
			ORDER BY priority DESC, run_at ASC, id ASC 
			LIMIT 1
		) 
		RETURNING *
	`, StateActive, now, lockToken, now, queueName, StateWaiting, StateDelayed, now).Scan(&job).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	if job.ID == "" {
		return nil, nil
	}

	return &job, nil
}

// stalledHousekeeper recovers stalled active jobs whose locks have expired.
func (q *QueueManager) stalledHousekeeper() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.config.StalledInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			return
		case <-ticker.C:
			q.recoverStalledJobs()
		}
	}
}

func (q *QueueManager) recoverStalledJobs() {
	now := time.Now()
	lockTimeoutCutoff := now.Add(-q.config.DefaultLock)

	var stalled []Job
	if err := q.db.Where("state = ? AND locked_at <= ?", StateActive, lockTimeoutCutoff).Find(&stalled).Error; err != nil {
		slog.Warn("queue: stalled housekeeper error", "error", err)
		return
	}

	for _, job := range stalled {
		if job.Attempts >= job.MaxAttempts {
			q.db.Model(&Job{}).Where("id = ? AND lock_token = ?", job.ID, job.LockToken).Updates(map[string]any{
				"state":         StateFailed,
				"failed_reason": "job stalled and lock expired",
				"failed_at":     now,
				"locked_at":     nil,
				"lock_token":    "",
				"updated_at":    now,
			})
			slog.Warn("queue: stalled job marked failed", "id", job.ID)
		} else {
			q.db.Model(&Job{}).Where("id = ? AND lock_token = ?", job.ID, job.LockToken).Updates(map[string]any{
				"state":      StateWaiting,
				"locked_at":  nil,
				"lock_token": "",
				"updated_at": now,
			})
			slog.Info("queue: stalled job recovered to waiting", "id", job.ID)
		}
	}
}
