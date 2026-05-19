package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommandOutputWriteHighlightedHonorsStyledOutput(t *testing.T) {
	styledOutput := NewCommandOutput(true)
	styledOutput.WriteHighlightedWithStyle(`{"key":"value"}`, "json", "monokai")
	require.Contains(t, styledOutput.outputBuilder.String(), "\x1b[")

	plainOutput := NewCommandOutput(false)
	plainOutput.WriteHighlightedWithStyle(`{"key":"value"}`, "json", "monokai")
	require.Equal(t, `{"key":"value"}`, plainOutput.outputBuilder.String())
}

func TestCommandContextWarningStyleHonorsStyledOutput(t *testing.T) {
	ctx := NewCommandContext(context.Background(), nil, NewScope(), t.TempDir())
	ctx.SetStyledOutputEnabled(false)
	ctx.output.Warn("watch out")
	require.Equal(t, " * watch out\n\n", ctx.OutputString())
}
