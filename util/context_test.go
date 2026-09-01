package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsContextLive(t *testing.T) {
	ctx := context.Background()
	actual := IsContextLive(ctx)
	assert.Equal(t, true, actual)
}

func TestIsContextLiveCanceled(t *testing.T) {
	// create a child context from background and cancel to force an error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	actual := IsContextLive(ctx)
	assert.Equal(t, false, actual)
}
