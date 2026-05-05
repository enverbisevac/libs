// stream/errors.go
package stream

import (
	"errors"
	"fmt"
)

// ErrSkipRetry when returned (or wrapped) from a Handler signals that the
// message is permanently failed and should be routed to the dead letter
// stream immediately, regardless of remaining retry budget.
var ErrSkipRetry = errors.New("stream: skip retry")

// SkipRetry wraps err so it satisfies errors.Is(err, ErrSkipRetry) while
// still preserving the wrapped error chain. Returning stream.SkipRetry(myErr)
// from a handler routes the message to the DLQ on this attempt.
func SkipRetry(err error) error {
	if err == nil {
		return ErrSkipRetry
	}
	return fmt.Errorf("%w: %w", ErrSkipRetry, err)
}
