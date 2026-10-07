package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderOverviewCommandRequiresOutputPath(t *testing.T) {
	ctx := NewCommandContext(context.Background(), nil, NewScope(RunModeREPL), t.TempDir())

	err := renderOverviewCmd().Fn(ctx, []string{"local"})

	require.EqualError(t, err, "invalid usage: render-overview -out <output.html> <connection>")
}
