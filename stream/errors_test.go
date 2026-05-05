// stream/errors_test.go
package stream_test

import (
	"errors"
	"testing"

	"github.com/enverbisevac/libs/stream"
	"github.com/stretchr/testify/require"
)

func TestSkipRetry_NilWraps(t *testing.T) {
	err := stream.SkipRetry(nil)
	require.True(t, errors.Is(err, stream.ErrSkipRetry))
}

func TestSkipRetry_PreservesWrapped(t *testing.T) {
	inner := errors.New("boom")
	err := stream.SkipRetry(inner)
	require.True(t, errors.Is(err, stream.ErrSkipRetry))
	require.True(t, errors.Is(err, inner))
}
