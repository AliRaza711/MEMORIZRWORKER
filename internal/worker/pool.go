// Package worker implements a high-performance, bounded-concurrency
// background worker that polls PostgreSQL for due reminders and dispatches
// them through the notifier layer.
package worker

import (
    "context"
    "fmt"
    "log/slog"
    "os"
    "runtime"
    "strconv"
    "sync"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/memorizr-worker/internal/domain"
    "github.com/memorizr-worker/internal/notifier"
)

type PoolConfig struct {
    PollInterval      time.Duration
    BatchSize         int
    ProcessTimeout    time.Duration
    StuckJobThreshold time.Duration
}

func DefaultConfig() PoolConfig {
    cfg := PoolConfig{
        PollInterval:      5 * time.Second,
        BatchSize:         100,
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

type Pool struct {
    db       *pgxpool.Pool
    notifier *notifier.Client
    cfg      PoolConfig
    
    // FILE WRITING LOGIC FOR LOCAL BENCHMARK
    outFile   *os.File
    fileMutex sync.Mutex
}

func NewPool(db *pgxpool.Pool, n *notifier.Client, cfg PoolConfig) *Pool {
    // Open or create the text file to log finished reminders
    f, err := os.OpenFile("reminders_done.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        panic("failed to open output file for reminders: " + err.Error())
    }

    return &Pool{
        db:      db,
        notifier: n,
        cfg:     cfg,
        outFile: f,
    }
}

func (p *Pool) Run(ctx context.Context) {
    slog.Info("worker pool starting",
        "poll_interval", p.cfg.PollInterval,
        "batch_size", p.cfg.BatchSize,
        "process_timeout", p.cfg.ProcessTimeout,
    )

    stuckTicker := time.NewTicker(p.cfg.StuckJobThreshold * 2)
    defer stuckTicker.Stop()

    for {
        // Check for shutdown signals
        if ctx.Err() != nil {
            slog.Info("worker pool received shutdown signal")
            return
        }

        select {
        case <-stuckTicker.C:
            p.recoverStuckJobs(ctx)
        default:
            // Non-blocking default case allows the loop to run immediately
        }

        // Process a batch and find out how many were processed
        processedCount := p.pollAndProcess(ctx)

        // SMART POLLING:
        // If we processed LESS than a full batch, the queue is empty. We sleep.
        // If we processed a FULL batch, we skip the sleep and fetch the next batch instantly.
        if processedCount < p.cfg.BatchSize {
            select {
            case <-ctx.Done():
                slog.Info("worker pool received shutdown signal")
                return
            case <-time.After(p.cfg.PollInterval):
                // Woke up after sleeping, loop will restart
            }
        }
    }
}

func (p *Pool) pollAndProcess(ctx context.Context) int {
    batchCtx, cancel := context.WithTimeout(ctx, p.cfg.ProcessTimeout)
    defer cancel()

    // 1. TIMING: Searching / DB Fetch Time
    fetchStart := time.Now()
    reminders, err := p.fetchBatch(batchCtx)
    fetchDuration := time.Since(fetchStart)

    if err != nil {
        slog.Error("failed to fetch batch", "error", err)
        return 0
    }
    if len(reminders) == 0 {
        return 0
    }

    slog.Info("--------------------------------------------------")
    slog.Info("📦 NEW BATCH STARTED",
        "count", len(reminders),
        "db_search_duration", fetchDuration,
    )

    batchStart := time.Now()
    var wg sync.WaitGroup
    wg.Add(len(reminders))

    for i := range reminders {
        r := reminders[i] // capture for goroutine
        go func(rem domain.Reminder) {
            defer wg.Done()
            p.processReminder(batchCtx, &rem)
        }(r)
    }

    wg.Wait()

    batchDuration := time.Since(batchStart)

    // MEMORY AND THREAD PROFILING
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    activeGoroutines := runtime.NumGoroutine()

    slog.Info("✅ BATCH COMPLETELY PROCESSED",
        "count", len(reminders),
        "total_batch_duration", batchDuration,
        "active_threads_goroutines", activeGoroutines,
        "memory_alloc_mb", float64(m.Alloc)/1024/1024,
        "memory_sys_mb", float64(m.Sys)/1024/1024,
    )
    slog.Info("--------------------------------------------------")

    return len(reminders)
}

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
        if err := rows.Scan(
            &r.ID, &r.UserID, &r.Title, &r.Description, &r.TargetDatetime,
            &r.Status, &r.RetryCount, &r.FailureReason,
            &r.CreatedAt, &r.UpdatedAt,
        ); err != nil {
            return nil, fmt.Errorf("scanning reminder row: %w", err)
        }
        r.Channel = domain.ChannelWhatsApp
        batch = append(batch, r)
    }

    return batch, rows.Err()
}

func (p *Pool) markSent(ctx context.Context, id string) error {
    _, err := p.db.Exec(ctx,
        `UPDATE "Reminder" SET status = 'Completed' WHERE id = $1`, id,
    )
    return err
}

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

func (p *Pool) processReminder(ctx context.Context, r *domain.Reminder) {
    processStart := time.Now()

    // 2. TIMING: File Write Time (Simulating WhatsApp Dispatch latency)
    dispatchStart := time.Now()
    
    // Artificial 20ms network simulation latency 
    time.Sleep(20 * time.Millisecond)
    
    // Mutex Lock to prevent multi-threaded file write race conditions
    p.fileMutex.Lock()
    _, writeErr := p.outFile.WriteString(fmt.Sprintf("Reminder %s done!\n", r.ID))
    p.fileMutex.Unlock()
    
    dispatchDuration := time.Since(dispatchStart)

    if writeErr != nil {
        updateStart := time.Now()
        dbErr := p.markFailed(ctx, r, writeErr)
        updateDuration := time.Since(updateStart)

        slog.Warn("delivery failed (file write)",
            "id", r.ID,
            "error", writeErr,
            "dispatch_duration", dispatchDuration,
            "db_update_duration", updateDuration,
        )
        if dbErr != nil {
            slog.Error("failed to mark reminder as failed", "id", r.ID, "db_error", dbErr)
        }
        return
    }

    // 3. TIMING: DB Update Time (PostgreSQL)
    updateStart := time.Now()
    dbErr := p.markSent(ctx, r.ID)
    updateDuration := time.Since(updateStart)

    if dbErr != nil {
        slog.Error("failed to mark reminder as sent", "id", r.ID, "db_error", dbErr)
        return
    }

    // Final Micro-Timing Log per reminder
    slog.Info("🚀 reminder delivered",
        "id", r.ID,
        "dispatch_duration", dispatchDuration,
        "db_update_duration", updateDuration,
        "total_item_duration", time.Since(processStart),
    )
}

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

func (p *Pool) Healthy(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    var n int
    return p.db.QueryRow(ctx, "SELECT 1").Scan(&n)
}

type Stats struct {
    Pending    int64 `json:"pending"`
    Processing int64 `json:"processing"`
    Sent       int64 `json:"sent"`
    Failed     int64 `json:"failed"`
}

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

var _ interface {
    Run(ctx context.Context)
    Healthy(ctx context.Context) error
} = (*Pool)(nil)