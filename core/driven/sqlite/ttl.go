package sqlite

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gorm.io/gorm"
)

// TTLIndex defines a TTL index on a model field, similar to MongoDB's
// db.collection.createIndex({ field: 1 }, { expireAfterSeconds: N }).
//
// The background worker periodically deletes records where:
//
//	field_value + ExpireAfterSeconds <= now
//
// Example:
//
//	sqlite.TTLIndex{
//	    Model:              &Session{},
//	    Field:              "CreatedAt",
//	    ExpireAfterSeconds: 3600, // delete 1 hour after CreatedAt
//	}
type TTLIndex struct {
	// Model is the GORM model struct (used to determine the table name).
	Model any

	// Field is the name of the time.Time field to index on.
	// This field must be of type time.Time or *time.Time.
	Field string

	// ExpireAfterSeconds is the number of seconds after the field value
	// at which the document expires and becomes eligible for deletion.
	ExpireAfterSeconds int64
}

// TTLConfig holds configuration for MongoDB-style TTL indexes.
type TTLConfig struct {
	// Indexes is the list of TTL indexes to register.
	Indexes []TTLIndex

	// CleanupInterval is how often the background worker checks for
	// expired documents. Defaults to 60 seconds if not set.
	CleanupInterval time.Duration

	// BatchSize controls how many documents are deleted per cleanup
	// batch per index. Defaults to 100 if not set.
	BatchSize int
}

// TTLPlugin is a GORM plugin that provides MongoDB-style TTL index support.
// It runs a background worker that periodically deletes expired documents
// based on registered TTL indexes.
//
// Usage:
//
//	db, ttl := sqlite.MustOpen(sqlite.Config{
//	    Path: "app.db",
//	    AutoMigrate: []any{&Session{}},
//	    TTL: &sqlite.TTLConfig{
//	        Indexes: []sqlite.TTLIndex{
//	            {Model: &Session{}, Field: "CreatedAt", ExpireAfterSeconds: 3600},
//	        },
//	        CleanupInterval: 60 * time.Second,
//	    },
//	})
//	defer ttl.Stop()
type TTLPlugin struct {
	config TTLConfig
	db     *gorm.DB

	indexes []ttlIndexEntry

	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
}

type ttlIndexEntry struct {
	tableName          string
	columnName         string
	expireAfterSeconds int64
}

// Name returns the plugin name for GORM registration.
func (p *TTLPlugin) Name() string {
	return "sqlite:ttl"
}

// Initialize registers TTL indexes and starts the background cleanup worker.
func (p *TTLPlugin) Initialize(db *gorm.DB) error {
	p.db = db

	if p.config.BatchSize <= 0 {
		p.config.BatchSize = 100
	}
	if p.config.CleanupInterval <= 0 {
		p.config.CleanupInterval = 60 * time.Second
	}

	// Resolve table/column names from the registered indexes
	for _, idx := range p.config.Indexes {
		entry, err := p.resolveIndex(idx)
		if err != nil {
			return fmt.Errorf("ttl: failed to resolve index for field %q: %w", idx.Field, err)
		}
		p.indexes = append(p.indexes, entry)
		slog.Info("sqlite: ttl index registered",
			"table", entry.tableName,
			"field", entry.columnName,
			"expire_after_seconds", entry.expireAfterSeconds,
		)
	}

	// Start background cleanup worker
	p.stopCh = make(chan struct{})
	p.wg.Add(1)
	go p.cleanupLoop()

	slog.Info("sqlite: ttl plugin initialized",
		"indexes", len(p.indexes),
		"cleanup_interval", p.config.CleanupInterval,
	)

	return nil
}

// resolveIndex uses GORM's statement to resolve the table and column names
// from a TTLIndex definition.
func (p *TTLPlugin) resolveIndex(idx TTLIndex) (ttlIndexEntry, error) {
	stmt := &gorm.Statement{DB: p.db}
	if err := stmt.Parse(idx.Model); err != nil {
		return ttlIndexEntry{}, fmt.Errorf("parse model: %w", err)
	}

	tableName := stmt.Schema.Table
	field := stmt.Schema.LookUpField(idx.Field)
	if field == nil {
		return ttlIndexEntry{}, fmt.Errorf("field %q not found on model %T", idx.Field, idx.Model)
	}

	return ttlIndexEntry{
		tableName:          tableName,
		columnName:         field.DBName,
		expireAfterSeconds: idx.ExpireAfterSeconds,
	}, nil
}

// Stop gracefully shuts down the background cleanup goroutine.
func (p *TTLPlugin) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopCh != nil {
		close(p.stopCh)
		p.wg.Wait()
		p.stopCh = nil
	}
}

// cleanupLoop periodically deletes expired documents.
func (p *TTLPlugin) cleanupLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.CleanupInterval)
	defer ticker.Stop()

	// Run immediately on start
	p.purgeExpired()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.purgeExpired()
		}
	}
}

// purgeExpired deletes documents where field_value + expireAfterSeconds <= now
// across all registered TTL indexes.
func (p *TTLPlugin) purgeExpired() {
	if p.db == nil {
		return
	}

	now := time.Now()
	var totalDeleted int64

	for _, idx := range p.indexes {
		// Calculate the cutoff: documents with field value <= cutoff are expired
		cutoff := now.Add(-time.Duration(idx.expireAfterSeconds) * time.Second)

		result := p.db.
			Table(idx.tableName).
			Where(fmt.Sprintf("%s <= ?", idx.columnName), cutoff).
			Limit(p.config.BatchSize).
			Delete(nil)

		if result.Error != nil {
			slog.Warn("sqlite: ttl cleanup error",
				"table", idx.tableName,
				"field", idx.columnName,
				"error", result.Error,
			)
			continue
		}

		totalDeleted += result.RowsAffected
	}

	if totalDeleted > 0 {
		slog.Info("sqlite: ttl purged expired documents",
			"count", totalDeleted,
		)
	}
}

// PurgeNow immediately deletes all expired documents across all registered
// TTL indexes. Returns the total number of deleted documents.
func (p *TTLPlugin) PurgeNow() int64 {
	if p.db == nil {
		return 0
	}

	now := time.Now()
	var totalDeleted int64

	for _, idx := range p.indexes {
		cutoff := now.Add(-time.Duration(idx.expireAfterSeconds) * time.Second)

		result := p.db.
			Table(idx.tableName).
			Where(fmt.Sprintf("%s <= ?", idx.columnName), cutoff).
			Delete(nil)

		if result.Error != nil {
			slog.Warn("sqlite: ttl manual purge error",
				"table", idx.tableName,
				"field", idx.columnName,
				"error", result.Error,
			)
			continue
		}

		totalDeleted += result.RowsAffected
	}

	return totalDeleted
}
