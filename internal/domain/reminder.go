package domain

import "time"

// ReminderStatus represents the lifecycle state of a reminder.
type ReminderStatus string

const (
    StatusPending    ReminderStatus = "Pending"
    StatusProcessing ReminderStatus = "Processing"
    StatusCompleted  ReminderStatus = "Completed"
    StatusFailed     ReminderStatus = "Failed"
)

// Channel represents the delivery mechanism for a reminder.
type Channel string

const (
    ChannelEmail    Channel = "Email"
    ChannelSMS      Channel = "SMS"
    ChannelWhatsApp Channel = "WhatsApp"
)

// Reminder is the primary domain entity mapping to Prisma
type Reminder struct {
    ID             string         `json:"id"`
    UserID         string         `json:"user_id"`
    Title          string         `json:"title"`
    Description    *string        `json:"description,omitempty"`
    TargetDatetime time.Time      `json:"target_datetime"`
    Status         ReminderStatus `json:"status"`
    RetryCount     int            `json:"retry_count"`
    FailureReason  *string        `json:"failure_reason,omitempty"`
    CreatedAt      time.Time      `json:"created_at"`
    UpdatedAt      time.Time      `json:"updated_at"`

    // Temporary field to route to the mock notifier 
    // (Until we implement the Prisma string[] array scanning)
    Channel        Channel        `json:"-"`
}

// CanRetry reports whether this reminder has remaining retry budget.
func (r *Reminder) CanRetry() bool {
    return r.RetryCount < 3 // Enforce max 3 retries
}

// CreditSummary provides a human-readable snapshot of the retry budget.
type CreditSummary struct {
    ReminderID string
    Used       int
    Remaining  int
}

// Credit returns a CreditSummary for this reminder.
func (r *Reminder) Credit() CreditSummary {
    return CreditSummary{
        ReminderID: r.ID,
        Used:       r.RetryCount,
        Remaining:  3 - r.RetryCount,
    }
}