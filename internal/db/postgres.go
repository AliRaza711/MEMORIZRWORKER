// Package db provides a thread-safe PostgreSQL connection pool using pgxpool.
//
// Unlike a "new connection per call" pattern, this package initialises a
// single *pgxpool.Pool at startup and shares it across all goroutines.
// pgxpool handles connection lifecycle, health checks, and idle reaping
// internally.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates and validates a pgxpool.Pool from environment variables.
// It blocks until the connection is confirmed healthy or the context expires.
func NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}

	// ── Pool sizing ──────────────────────────────────────
	if v := os.Getenv("DB_POOL_MIN_CONNS"); v != "" {
		n, _ := strconv.Atoi(v)
		cfg.MinConns = int32(n)
	}
	if v := os.Getenv("DB_POOL_MAX_CONNS"); v != "" {
		n, _ := strconv.Atoi(v)
		cfg.MaxConns = int32(n)
	}

	// ── Health & timeout defaults ────────────────────────
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating pgx pool: %w", err)
	}

	// Verify connectivity before returning.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	stat := pool.Stat()
	slog.Info("database pool established",
		"min_conns", cfg.MinConns,
		"max_conns", cfg.MaxConns,
		"idle_conns", stat.IdleConns(),
	)

	return pool, nil
}
