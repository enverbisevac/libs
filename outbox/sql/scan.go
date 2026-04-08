package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/enverbisevac/libs/outbox"
)

// ScanMessages scans rows produced by a fetch query into outbox.Message values.
//
// The expected column order is:
//
//	id, topic, key, payload, headers, status, retries, last_error, created_at, processed_at
//
// processed_at is treated as nullable.
func ScanMessages(rows *sql.Rows) ([]outbox.Message, error) {
	var msgs []outbox.Message
	for rows.Next() {
		var msg outbox.Message
		var headers []byte
		var processedAt sql.NullTime
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Key, &msg.Payload,
			&headers, &msg.Status, &msg.Retries, &msg.LastError,
			&msg.CreatedAt, &processedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if processedAt.Valid {
			msg.ProcessedAt = processedAt.Time
		}
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &msg.Headers); err != nil {
				return nil, fmt.Errorf("outbox: unmarshal headers: %w", err)
			}
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: rows: %w", err)
	}
	return msgs, nil
}