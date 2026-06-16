// Package worker implements a high-performance, bounded-concurrency
// background worker that polls PostgreSQL for due reminders and dispatches
// them through the notifier layer.
//
// Design principles:
//   - SELECT … FOR UPDATE SKIP LOCKED for safe multi-instance polling
//   - sync.WaitGroup to settle each batch before polling again
//   - Context-based 3-minute timeout to prevent goroutine leaks
//   - Graceful shutdown: context cancellation drains the current batch
//   - Stuck job recovery: periodic sweep resets abandoned "processing" rows
package worker

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "strconv"
    "sync"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/memorizr-worker/internal/domain"
    "github.com/memorizr-worker/internal/notifier"
)

// ─── Configuration ───────────────────────────────────────────────────────────

// PoolConfig holds the tunable knobs for the worker pool.
type PoolConfig struct {
    PollInterval      time.Duration // How often to check for due reminders.
    BatchSize         int           // Max reminders to claim per poll cycle.
    ProcessTimeout    time.Duration // Per-batch context deadline (SLA ceiling).
    StuckJobThreshold time.Duration // How long before a "processing" row is considered stuck.
}

// DefaultConfig returns sensible defaults, overridden by environment variables
// when present.
func DefaultConfig() PoolConfig {
    cfg := PoolConfig{
        PollInterval:      5 * time.Second,
        BatchSize:         20,
        ProcessTimeout:    3 * time.Minute,
        StuckJobThreshold: 5 * time.Minute,
    }

    if v := os.Getenv("WORKER_POLL_INTERVAL"); v != "" {
        if d, err := time.ParseDuration(v); err == nil {
            cfg.PollInterval = d
        }
    }
    if v := os.Getenv("WORKER_BATCH_SIZE"); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            cfg.BatchSize = n
        }
    }
    if v := os.Getenv("WORKER_PROCESS_TIMEOUT"); v != "" {
        if d, err := time.ParseDuration(v); err == nil {
            cfg.ProcessTimeout = d
        }
    }
    if v := os.Getenv("WORKER_STUCK_JOB_THRESHOLD"); v != "" {
        if d, err := time.ParseDuration(v); err == nil {
            cfg.StuckJobThreshold = d
        }
    }

    return cfg
}

// ─── Worker Pool ─────────────────────────────────────────────────────────────

// Pool is the main coordinator. It owns the poll loop, spawns bounded
// goroutines per batch, and handles lifecycle signals.
type Pool struct {
    db       *pgxpool.Pool
    notifier *notifier.Client
    cfg      PoolConfig
}

// NewPool creates a worker pool wired to the given database and notifier.
func NewPool(db *pgxpool.Pool, n *notifier.Client, cfg PoolConfig) *Pool {
    return &Pool{
        db:       db,
        notifier: n,
        cfg:      cfg,
    }
}

// Run starts the main poll loop. It blocks until ctx is cancelled.
// On cancellation it finishes processing the current in-flight batch
// before returning.
func (p *Pool) Run(ctx context.Context) {
    slog.Info("worker pool starting",
        "poll_interval", p.cfg.PollInterval,
        "batch_size", p.cfg.BatchSize,
        "process_timeout", p.cfg.ProcessTimeout,
    )

    ticker := time.NewTicker(p.cfg.PollInterval)
    defer ticker.Stop()

    // Run stuck-job recovery on a slower cadence (2× the threshold).
    stuckTicker := time.NewTicker(p.cfg.StuckJobThreshold * 2)
    defer stuckTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            slog.Info("worker pool received shutdown signal")
            return

        case <-stuckTicker.C:
            p.recoverStuckJobs(ctx)

        case <-ticker.C:
            p.pollAndProcess(ctx)
        }
    }
}

// ─── Poll Cycle ──────────────────────────────────────────────────────────────

// pollAndProcess fetches a batch of due reminders, spawns a goroutine for
// each, and blocks until the entire batch settles via sync.WaitGroup.
func (p *Pool) pollAndProcess(ctx context.Context) {
    // Create a child context with a strict timeout so no goroutine
    // in this batch can run longer than ProcessTimeout.
    batchCtx, cancel := context.WithTimeout(ctx, p.cfg.ProcessTimeout)
    defer cancel()

    reminders, err := p.fetchBatch(batchCtx)
    if err != nil {
        slog.Error("failed to fetch batch", "error", err)
        return
    }
    if len(reminders) == 0 {
        return
    }

    slog.Info("processing batch", "count", len(reminders))

    var wg sync.WaitGroup
    wg.Add(len(reminders))

    for i := range reminders {
        r := reminders[i] // capture for goroutine
        go func() {
            defer wg.Done()
            p.processReminder(batchCtx, &r)
        }()
    }

    // Block until every goroutine in this batch has finished.
    // This ensures we don't overlap batches and provides backpressure
    // when downstream systems are slow.
    wg.Wait()
}

// ─── Database Operations ─────────────────────────────────────────────────────

// fetchBatch claims up to BatchSize pending reminders using
// SELECT … FOR UPDATE SKIP LOCKED. This is safe for concurrent workers:
// each instance sees only unclaimed rows.
func (p *Pool) fetchBatch(ctx context.Context) ([]domain.Reminder, error) {
    query := `
        UPDATE "Reminder"
        SET    status     = 'Processing',
               retry_count   = retry_count + 1
        WHERE  id IN (
            SELECT id
            FROM   "Reminder"
            WHERE  status = 'Pending'
              AND  target_datetime <= NOW()
            ORDER  BY target_datetime ASC
            FOR UPDATE SKIP LOCKED
            LIMIT  $1
        )
        RETURNING id, user_id, title, description, target_datetime,
                  status, retry_count, failure_reason,
                  created_at, updated_at
    `

    rows, err := p.db.Query(ctx, query, p.cfg.BatchSize)
    if err != nil {
        return nil, fmt.Errorf("querying due reminders: %w", err)
    }
    defer rows.Close()

    var batch []domain.Reminder
    for rows.Next() {
        var r domain.Reminder
        // Note: Make sure your internal/domain/reminder.go struct 
        // matches these exported fields (ID, UserID, Title, etc.)
        if err := rows.Scan(
            &r.ID, &r.UserID, &r.Title, &r.Description, &r.TargetDatetime,
            &r.Status, &r.RetryCount, &r.FailureReason,
            &r.CreatedAt, &r.UpdatedAt,
        ); err != nil {
            return nil, fmt.Errorf("scanning reminder row: %w", err)
        }
        
        // ADD THIS LINE to default the routing to WhatsApp
        r.Channel = domain.ChannelWhatsApp 
        
        batch = append(batch, r)
    }

    return batch, rows.Err()
}

// markSent transitions a reminder to "Completed" after successful delivery.
func (p *Pool) markSent(ctx context.Context, id string) error {
    _, err := p.db.Exec(ctx,
        `UPDATE "Reminder" SET status = 'Completed' WHERE id = $1`, id,
    )
    return err
}

// markFailed records the error message and either requeues the reminder
// as "Pending" (if retries remain) or moves it to "Failed".
func (p *Pool) markFailed(ctx context.Context, r *domain.Reminder, sendErr error) error {
    errMsg := sendErr.Error()
    newStatus := domain.StatusPending // requeue for retry
    if !r.CanRetry() {
        newStatus = domain.StatusFailed // exhausted
    }

    _, err := p.db.Exec(ctx,
        `UPDATE "Reminder" SET status = $1, failure_reason = $2 WHERE id = $3`,
        string(newStatus), errMsg, r.ID,
    )
    return err
}

// ─── Per-Reminder Processing ─────────────────────────────────────────────────

// processReminder dispatches a single reminder through the notifier and
// updates its database status based on the outcome.
func (p *Pool) processReminder(ctx context.Context, r *domain.Reminder) {
    credit := r.Credit()
    slog.Info("processing reminder",
        "id", r.ID,
        "attempt", r.RetryCount,
        "credits_remaining", credit.Remaining,
    )

    err := p.notifier.Dispatch(ctx, r)

    if err != nil {
        slog.Warn("delivery failed",
            "id", r.ID,
            "error", err,
            "will_retry", r.CanRetry(),
        )
        if dbErr := p.markFailed(ctx, r, err); dbErr != nil {
            slog.Error("failed to mark reminder as failed",
                "id", r.ID,
                "db_error", dbErr,
            )
        }
        return
    }

    if dbErr := p.markSent(ctx, r.ID); dbErr != nil {
        slog.Error("failed to mark reminder as sent",
            "id", r.ID,
            "db_error", dbErr,
        )
        return
    }

    slog.Info("reminder delivered",
        "id", r.ID,
    )
}

// ─── Stuck Job Recovery ──────────────────────────────────────────────────────

// recoverStuckJobs resets reminders that have been in "Processing" state
// longer than StuckJobThreshold back to "Pending" so they can be retried.
func (p *Pool) recoverStuckJobs(ctx context.Context) {
    query := `
        UPDATE "Reminder"
        SET    status = 'Pending'
        WHERE  status = 'Processing'
          AND  updated_at < NOW() - $1::INTERVAL
    `

    tag, err := p.db.Exec(ctx, query, p.cfg.StuckJobThreshold.String())
    if err != nil {
        slog.Error("stuck job recovery failed", "error", err)
        return
    }

    if tag.RowsAffected() > 0 {
        slog.Warn("recovered stuck jobs",
            "count", tag.RowsAffected(),
            "threshold", p.cfg.StuckJobThreshold,
        )
    }
}

// ─── Healthcheck ─────────────────────────────────────────────────────────────

// Healthy reports whether the worker pool can reach the database.
func (p *Pool) Healthy(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()

    var n int
    return p.db.QueryRow(ctx, "SELECT 1").Scan(&n)
}

// ─── Stats ───────────────────────────────────────────────────────────────────

// Stats holds a point-in-time snapshot of queue depths, useful for dashboards.
type Stats struct {
    Pending    int64 `json:"pending"`
    Processing int64 `json:"processing"`
    Sent       int64 `json:"sent"`
    Failed     int64 `json:"failed"`
}

// GetStats queries the current distribution of reminder statuses.
func (p *Pool) GetStats(ctx context.Context) (*Stats, error) {
    query := `
        SELECT
            COALESCE(SUM(CASE WHEN status = 'Pending'    THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN status = 'Processing' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN status = 'Completed'  THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN status = 'Failed'     THEN 1 ELSE 0 END), 0)
        FROM "Reminder"
    `

    var s Stats
    err := p.db.QueryRow(ctx, query).Scan(&s.Pending, &s.Processing, &s.Sent, &s.Failed)
    if err != nil {
        return nil, fmt.Errorf("querying stats: %w", err)
    }
    return &s, nil
}

// ensure Pool implements a compile-time interface check if needed
var _ interface {
    Run(ctx context.Context)
    Healthy(ctx context.Context) error
} = (*Pool)(nil)