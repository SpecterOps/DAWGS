package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Compile-time assertion that *Driver implements graph.Optimizer.
var _ graph.Optimizer = (*Driver)(nil)

func TestDriver_Optimize_NoopReturnsNil(t *testing.T) {
	d := &Driver{}
	require.NoError(t, d.Optimize(context.Background()))
}
