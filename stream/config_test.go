package stream_test

import (
	"testing"

	"github.com/enverbisevac/libs/stream"
	"github.com/stretchr/testify/require"
)

func TestFormatStream(t *testing.T) {
	require.Equal(t, "app.default.orders", stream.FormatStream("app", "default", "orders"))
	require.Equal(t, "myapp.tenant1.events", stream.FormatStream("myapp", "tenant1", "events"))
}

func TestDeadLetterStream(t *testing.T) {
	require.Equal(t, "app.default.orders.dead",
		stream.DeadLetterStream(stream.FormatStream("app", "default", "orders")))
}

func TestStartPosition_Default(t *testing.T) {
	var p stream.StartPosition
	require.Equal(t, stream.StartLatest, p, "zero value of StartPosition must be StartLatest")
}
