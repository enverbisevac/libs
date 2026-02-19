package outbox

import (
	"context"
	"time"
)

// MessageStatus represents the status of an outbox message.
type MessageStatus string

const (
	StatusPending    MessageStatus = "pending"
	StatusProcessing MessageStatus = "processing"
	StatusProcessed  MessageStatus = "processed"
	StatusFailed     MessageStatus = "failed"
)

// Message represents an outbox message.
type Message struct {
	ID          string
	Topic       string
	Key         string
	Payload     []byte
	Headers     map[string]string
	Status      MessageStatus
	Retries     int
	LastError   string
	CreatedAt   time.Time
	ProcessedAt time.Time
}

// Store is the interface for outbox message persistence.
type Store interface {
	// Save persists messages within the given transaction.
	// tx can be pgx.Tx, *sql.Tx, or nil (Store creates its own transaction).
	Save(ctx context.Context, tx any, msgs ...Message) error

	// FetchPending atomically claims up to limit pending messages for processing.
	FetchPending(ctx context.Context, limit int) ([]Message, error)

	// MarkProcessed marks messages as successfully processed.
	MarkProcessed(ctx context.Context, ids ...string) error

	// MarkFailed increments retries and records the error.
	// Sets status to failed when max retries reached, else back to pending.
	MarkFailed(ctx context.Context, id string, err error) error
}

// Publisher is the interface for publishing outbox messages.
type Publisher interface {
	Publish(ctx context.Context, msgs ...Message) error
}
