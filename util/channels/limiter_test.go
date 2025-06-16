package channels

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimiter(t *testing.T) {
	var (
		limiter = NewConcurrencyLimiter(2)
	)

	require.True(t, limiter.Acquire(context.Background()))
	require.True(t, limiter.Acquire(context.Background()))

	timeoutCtx, done := context.WithTimeout(context.Background(), time.Millisecond)
	defer done()

	require.False(t, limiter.Acquire(timeoutCtx))
}
