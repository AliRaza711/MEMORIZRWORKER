// Package notifier provides an abstraction layer for delivering reminders
// through multiple channels (Email, SMS, WhatsApp).
//
// In production, each sender would call an external API (SendGrid, Twilio,
// WhatsApp Business API). This implementation uses mocked HTTP calls with
// realistic latency simulation so the worker pool can be stress-tested
// without external dependencies.
package notifier

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/memorizr-worker/internal/domain"
)

// Sender defines the contract for delivering a reminder. Each channel
// (email, sms, whatsapp) implements this interface.
type Sender interface {
	Send(ctx context.Context, r *domain.Reminder) error
}

// Client routes a reminder to the correct Sender based on r.Channel.
type Client struct {
	senders map[domain.Channel]Sender
}

// NewClient wires up the mock senders for all supported channels.
func NewClient() *Client {
	return &Client{
		senders: map[domain.Channel]Sender{
			domain.ChannelEmail:    &emailSender{},
			domain.ChannelSMS:     &smsSender{},
			domain.ChannelWhatsApp: &whatsAppSender{},
		},
	}
}

// Dispatch routes the reminder to the appropriate channel sender.
// Returns an error if the channel is unsupported or the send fails.
func (c *Client) Dispatch(ctx context.Context, r *domain.Reminder) error {
	sender, ok := c.senders[r.Channel]
	if !ok {
		return fmt.Errorf("unsupported channel: %s", r.Channel)
	}
	return sender.Send(ctx, r)
}

// ─── Mock Senders ────────────────────────────────────────────────────────────

// emailSender simulates an email delivery (e.g., SendGrid, SES).
type emailSender struct{}

func (s *emailSender) Send(ctx context.Context, r *domain.Reminder) error {
	return simulateHTTPCall(ctx, "email", r)
}

// smsSender simulates an SMS delivery (e.g., Twilio).
type smsSender struct{}

func (s *smsSender) Send(ctx context.Context, r *domain.Reminder) error {
	return simulateHTTPCall(ctx, "sms", r)
}

// whatsAppSender simulates a WhatsApp delivery (e.g., WhatsApp Business API).
type whatsAppSender struct{}

func (s *whatsAppSender) Send(ctx context.Context, r *domain.Reminder) error {
	return simulateHTTPCall(ctx, "whatsapp", r)
}

// simulateHTTPCall introduces realistic latency (50–300ms) and a configurable
// failure rate (~5%) to exercise the worker's retry logic.
func simulateHTTPCall(ctx context.Context, channel string, r *domain.Reminder) error {
    latency := time.Duration(50+rand.Intn(250)) * time.Millisecond

    select {
    case <-time.After(latency):
        if rand.Float64() < 0.05 {
            // CHANGED %d to %s for string UUIDs
            return fmt.Errorf("%s provider returned HTTP 503 for reminder %s", channel, r.ID) 
        }
        return nil
    case <-ctx.Done():
        return fmt.Errorf("notification cancelled: %w", ctx.Err())
    }
}
