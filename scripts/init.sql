-- ──────────────────────────────────────────────────────────
--  Memorizr — Database Schema
-- ──────────────────────────────────────────────────────────

-- Enum-like status via CHECK constraint for clarity and safety.
CREATE TYPE reminder_status AS ENUM ('pending', 'processing', 'sent', 'failed');
CREATE TYPE channel_type    AS ENUM ('email', 'sms', 'whatsapp');

CREATE TABLE reminders (
    id          BIGSERIAL       PRIMARY KEY,
    recipient   VARCHAR(255)    NOT NULL,
    channel     channel_type    NOT NULL DEFAULT 'email',
    subject     VARCHAR(500)    NOT NULL,
    body        TEXT            NOT NULL,
    due_at      TIMESTAMPTZ     NOT NULL,
    status      reminder_status NOT NULL DEFAULT 'pending',
    attempts    SMALLINT        NOT NULL DEFAULT 0,
    max_retries SMALLINT        NOT NULL DEFAULT 3,
    last_error  TEXT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Partial Index 1: Polling for due reminders ───────────
-- Used by the worker's SELECT … FOR UPDATE SKIP LOCKED query.
-- Only indexes rows that are pending AND due now-or-earlier,
-- keeping the index tiny even on tables with millions of sent rows.
CREATE INDEX idx_reminders_pending_due
    ON reminders (due_at)
    WHERE status = 'pending';

-- ── Partial Index 2: Stuck job recovery ──────────────────
-- Detects rows that moved to 'processing' but were never
-- completed (e.g., worker crash). A separate sweep query
-- resets these back to 'pending'.
CREATE INDEX idx_reminders_stuck_processing
    ON reminders (updated_at)
    WHERE status = 'processing';

-- ── Trigger: auto-update `updated_at` on every row change ─
CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON reminders
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_updated_at();

-- ── Seed data for smoke-testing ──────────────────────────
INSERT INTO reminders (recipient, channel, subject, body, due_at) VALUES
    ('user@example.com',  'email',    'Stand-up in 15 min',       'Daily stand-up starts at 09:15 UTC.',  NOW() - INTERVAL '1 minute'),
    ('+14155550100',       'sms',      'Take medication',          'Time to take your evening vitamins.',  NOW() - INTERVAL '30 seconds'),
    ('+442071234567',      'whatsapp', 'Flight check-in opens',    'Your BA283 check-in window is open.',  NOW() + INTERVAL '1 hour');
