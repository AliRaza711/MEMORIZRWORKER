// Package main is the entrypoint for the Memorizr background worker.
//
// Responsibilities:
//   - Load environment configuration
//   - Establish the PostgreSQL connection pool
//   - Wire up the notifier client
//   - Start the worker pool
//   - Capture SIGINT/SIGTERM for graceful shutdown
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/memorizr-worker/internal/db"
	"github.com/memorizr-worker/internal/notifier"
	"github.com/memorizr-worker/internal/worker"
)

func main() {
	// ── Structured logging ───────────────────────────────
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("memorizr worker starting")

	// ── Load .env (non-fatal if missing; Docker sets env directly) ──
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, relying on system environment")
	}

	// ── Root context with cancellation on OS signal ──────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Database pool ────────────────────────────────────
	pool, err := db.NewPool(ctx)
	if err != nil {
		slog.Error("failed to create database pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// ── Notifier client ──────────────────────────────────
	notify := notifier.NewClient()

	// ── Worker pool ──────────────────────────────────────
	cfg := worker.DefaultConfig()
	wp := worker.NewPool(pool, notify, cfg)

	// Run the worker in a goroutine so we can wait for shutdown below.
	done := make(chan struct{})
	go func() {
		wp.Run(ctx)
		close(done)
	}()

	// ── Wait for shutdown signal ─────────────────────────
	// signal.NotifyContext already hooked SIGINT/SIGTERM to ctx.
	// When ctx is cancelled, wp.Run() will finish its current batch
	// (via WaitGroup) and return, closing the `done` channel.
	<-done

	slog.Info("memorizr worker shut down gracefully")
}
