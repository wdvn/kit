package sqlite_test

import (
	"context"
	"os"
	"testing"
	"time"

	driven "github.com/wdvn/kit/core/driven/sqlite"
)

// Session is a model with a CreatedAt field for TTL testing.
type Session struct {
	ID        uint      `gorm:"primaryKey"`
	Token     string    `gorm:"size:200;not null"`
	UserID    uint      `gorm:"not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// tempDB returns a temporary file path for testing.
// Using a file-based DB avoids the :memory: connection-pooling issue
// where each connection sees a separate in-memory database.
func tempDB(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "kit-ttl-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := f.Name()
	_ = f.Close()
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

// TestTTLPluginBasic verifies that the TTL plugin expires and deletes documents.
func TestTTLPluginBasic(t *testing.T) {
	db, ttl := driven.MustOpen(driven.Config{
		Path:        tempDB(t),
		AutoMigrate: []any{&Session{}},
		LogLevel:    0,
		TTL: &driven.TTLConfig{
			Indexes: []driven.TTLIndex{
				{Model: &Session{}, Field: "CreatedAt", ExpireAfterSeconds: 1},
			},
			CleanupInterval: 1 * time.Second,
		},
	})
	defer ttl.Stop()

	if ttl == nil {
		t.Fatal("expected TTL plugin to be non-nil")
	}

	// Insert a session
	ctx := context.Background()
	createSession := driven.Handler[CreateSessionParams, Session](db, driven.OpCreate)

	sess, err := createSession(ctx, &CreateSessionParams{
		Data: &Session{Token: "abc123", UserID: 1},
	})
	if err != nil {
		t.Fatalf("create session failed: %v", err)
	}
	t.Logf("Created session: ID=%d, CreatedAt=%v", sess.ID, sess.CreatedAt)

	// Wait for TTL to expire and cleanup (1s expire + 100ms interval + buffer)
	time.Sleep(2 * time.Second)

	// Session should be deleted by now
	findSession := driven.Handler[FindSessionParams, Session](db, driven.OpFind)
	_, err = findSession(ctx, &FindSessionParams{ID: sess.ID})
	if err == nil {
		t.Error("expected session to be deleted by TTL, but it still exists")
	}
	t.Logf("TTL cleanup result: %v", err)
}

// TestTTLPluginNoExpire verifies that non-expired documents are not deleted.
func TestTTLPluginNoExpire(t *testing.T) {
	db, ttl := driven.MustOpen(driven.Config{
		Path:        tempDB(t),
		AutoMigrate: []any{&Session{}},
		LogLevel:    0,
		TTL: &driven.TTLConfig{
			Indexes: []driven.TTLIndex{
				{Model: &Session{}, Field: "CreatedAt", ExpireAfterSeconds: 3600}, // 1 hour
			},
			CleanupInterval: 100 * time.Millisecond,
		},
	})
	defer ttl.Stop()

	ctx := context.Background()
	createSession := driven.Handler[CreateSessionParams, Session](db, driven.OpCreate)

	sess, err := createSession(ctx, &CreateSessionParams{
		Data: &Session{Token: "no-expire", UserID: 2},
	})
	if err != nil {
		t.Fatalf("create session failed: %v", err)
	}

	// Wait a bit — should NOT be deleted since expireAfterSeconds is 3600
	time.Sleep(500 * time.Millisecond)

	findSession := driven.Handler[FindSessionParams, Session](db, driven.OpFind)
	found, err := findSession(ctx, &FindSessionParams{ID: sess.ID})
	if err != nil {
		t.Fatalf("expected session to still exist: %v", err)
	}
	if found.Token != "no-expire" {
		t.Errorf("expected token 'no-expire', got %q", found.Token)
	}
}

// TestTTLPluginPurgeNow verifies that PurgeNow immediately deletes expired docs.
func TestTTLPluginPurgeNow(t *testing.T) {
	db, ttl := driven.MustOpen(driven.Config{
		Path:        tempDB(t),
		AutoMigrate: []any{&Session{}},
		LogLevel:    0,
		TTL: &driven.TTLConfig{
			Indexes: []driven.TTLIndex{
				{Model: &Session{}, Field: "CreatedAt", ExpireAfterSeconds: 0}, // expire immediately
			},
			// Long interval so background worker doesn't interfere
			CleanupInterval: 1 * time.Hour,
		},
	})
	defer ttl.Stop()

	ctx := context.Background()
	createSession := driven.Handler[CreateSessionParams, Session](db, driven.OpCreate)

	// Create 3 sessions
	for i := 0; i < 3; i++ {
		_, err := createSession(ctx, &CreateSessionParams{
			Data: &Session{Token: "purge-test", UserID: uint(i)},
		})
		if err != nil {
			t.Fatalf("create session %d failed: %v", i, err)
		}
	}

	// Manually purge
	deleted := ttl.PurgeNow()
	if deleted != 3 {
		t.Errorf("expected 3 deleted, got %d", deleted)
	}

	// Verify all gone
	countSessions := driven.Handler[CountSessionsParams, CountResult](db, driven.OpCount)
	count, err := countSessions(ctx, &CountSessionsParams{})
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count.Count != 0 {
		t.Errorf("expected 0 sessions, got %d", count.Count)
	}
}

// TestTTLPluginMultipleIndexes verifies multiple TTL indexes work together.
func TestTTLPluginMultipleIndexes(t *testing.T) {
	// Use a model with two time fields
	type Event struct {
		ID        uint   `gorm:"primaryKey"`
		Name      string `gorm:"size:100"`
		StartTime time.Time
		EndTime   time.Time
	}

	db, ttl := driven.MustOpen(driven.Config{
		Path:        tempDB(t),
		AutoMigrate: []any{&Event{}},
		LogLevel:    0,
		TTL: &driven.TTLConfig{
			Indexes: []driven.TTLIndex{
				{Model: &Event{}, Field: "StartTime", ExpireAfterSeconds: 0},  // expire immediately based on StartTime
				{Model: &Event{}, Field: "EndTime", ExpireAfterSeconds: 3600}, // 1 hour after EndTime
			},
			CleanupInterval: 1 * time.Hour,
		},
	})
	defer ttl.Stop()

	// Insert events directly
	db.Exec("INSERT INTO events (name, start_time, end_time) VALUES (?, ?, ?)",
		"event-1", time.Now(), time.Now().Add(2*time.Hour))
	db.Exec("INSERT INTO events (name, start_time, end_time) VALUES (?, ?, ?)",
		"event-2", time.Now(), time.Now().Add(-30*time.Minute)) // end_time is in the past

	// PurgeNow: both events expire via StartTime (0s expireAfterSeconds)
	deleted := ttl.PurgeNow()
	if deleted < 2 {
		t.Errorf("expected at least 2 deleted, got %d", deleted)
	}
}

// TestTTLPluginWithoutTTL verifies that Open works without TTL config.
func TestTTLPluginWithoutTTL(t *testing.T) {
	db, ttl := driven.MustOpen(driven.Config{
		Path:        ":memory:",
		AutoMigrate: []any{&Session{}},
		LogLevel:    0,
	})

	if ttl != nil {
		t.Error("expected TTL plugin to be nil when no TTL config is provided")
	}

	// Basic operation should still work
	ctx := context.Background()
	createSession := driven.Handler[CreateSessionParams, Session](db, driven.OpCreate)
	_, err := createSession(ctx, &CreateSessionParams{
		Data: &Session{Token: "no-ttl", UserID: 99},
	})
	if err != nil {
		t.Fatalf("create session failed: %v", err)
	}
}

// TestTTLPluginStopIdempotent verifies Stop can be called multiple times safely.
func TestTTLPluginStopIdempotent(t *testing.T) {
	db, ttl := driven.MustOpen(driven.Config{
		Path:        tempDB(t),
		AutoMigrate: []any{&Session{}},
		TTL: &driven.TTLConfig{
			Indexes: []driven.TTLIndex{
				{Model: &Session{}, Field: "CreatedAt", ExpireAfterSeconds: 1},
			},
		},
	})

	// Verify the sessions table exists
	var count int64
	if err := db.Raw("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='sessions'").Scan(&count).Error; err != nil {
		t.Fatalf("failed to check table existence: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected sessions table to exist, got count=%d", count)
	}
	t.Logf("sessions table exists: verified")

	ttl.Stop()
	ttl.Stop() // should not panic
	ttl.Stop() // should not panic
}

// --- Params for Session operations ---

type CreateSessionParams struct {
	Data *Session
}

func (p *CreateSessionParams) Table() string { return "sessions" }

type FindSessionParams struct {
	ID uint `where:"id"`
}

func (p *FindSessionParams) Table() string { return "sessions" }

type CountSessionsParams struct{}

func (p *CountSessionsParams) Table() string { return "sessions" }
