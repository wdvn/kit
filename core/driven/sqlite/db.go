package sqlite

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config holds the SQLite connection configuration.
type Config struct {
	// Path is the file path for the SQLite database.
	// Use ":memory:" for in-memory databases, or "" for a temp file.
	Path string

	// AutoMigrate is a list of model structs to auto-migrate on connection.
	AutoMigrate []any

	// Logger sets the GORM logger level.
	// 0 = Silent, 1 = Error, 2 = Warn, 3 = Info
	LogLevel int

	// WAL enables Write-Ahead Logging for better concurrent read performance.
	WAL bool

	// JournalMode overrides the default journal mode.
	JournalMode string

	// BusyTimeout sets the busy timeout in milliseconds (default: 5000).
	BusyTimeout int

	// TTL configures MongoDB-style TTL indexes for automatic document expiration.
	// When set, the TTL plugin is registered and a background worker is started.
	// The plugin instance is returned via the second return value of Open/MustOpen.
	TTL *TTLConfig
}

// Open creates a new GORM DB connection to an SQLite database using pure Go driver.
// If cfg.TTL is set, the TTL plugin is registered and returned as the second value.
func Open(cfg Config) (*gorm.DB, *TTLPlugin, error) {
	dsn := buildDSN(cfg)

	gormCfg := &gorm.Config{}

	switch cfg.LogLevel {
	case 0:
		gormCfg.Logger = logger.Default.LogMode(logger.Silent)
	case 1:
		gormCfg.Logger = logger.Default.LogMode(logger.Error)
	case 2:
		gormCfg.Logger = logger.Default.LogMode(logger.Warn)
	case 3:
		gormCfg.Logger = logger.Default.LogMode(logger.Info)
	default:
		gormCfg.Logger = logger.Default.LogMode(logger.Warn)
	}

	db, err := gorm.Open(sqlite.Open(dsn), gormCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}

	// Apply pragmas
	if cfg.WAL || cfg.JournalMode == "WAL" {
		db.Exec("PRAGMA journal_mode=WAL")
	} else if cfg.JournalMode != "" {
		db.Exec(fmt.Sprintf("PRAGMA journal_mode=%s", cfg.JournalMode))
	}

	busyTimeout := cfg.BusyTimeout
	if busyTimeout == 0 {
		busyTimeout = 5000
	}
	db.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeout))

	// Run auto-migration
	if len(cfg.AutoMigrate) > 0 {
		if err := db.AutoMigrate(cfg.AutoMigrate...); err != nil {
			return nil, nil, fmt.Errorf("failed to auto-migrate: %w", err)
		}
	}

	// Register TTL plugin if configured
	var ttlPlugin *TTLPlugin
	if cfg.TTL != nil {
		ttlPlugin = &TTLPlugin{config: *cfg.TTL}
		if err := db.Use(ttlPlugin); err != nil {
			return nil, nil, fmt.Errorf("failed to register ttl plugin: %w", err)
		}
	}

	slog.Info("sqlite: connected", "path", cfg.Path)
	return db, ttlPlugin, nil
}

// MustOpen is like Open but panics on error.
// If cfg.TTL is set, the TTL plugin is returned as the second value.
func MustOpen(cfg Config) (*gorm.DB, *TTLPlugin) {
	db, ttl, err := Open(cfg)
	if err != nil {
		panic(err)
	}
	return db, ttl
}

// buildDSN constructs the SQLite connection string.
func buildDSN(cfg Config) string {
	if cfg.Path == ":memory:" {
		return ":memory:?_pragma=foreign_keys(1)"
	}

	if cfg.Path == "" {
		cfg.Path = "app.db"
	}

	// Ensure directory exists
	dir := filepath.Dir(cfg.Path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			slog.Warn("sqlite: failed to create db directory", "dir", dir, "error", err)
		}
	}

	return fmt.Sprintf("%s?_pragma=foreign_keys(1)", cfg.Path)
}
