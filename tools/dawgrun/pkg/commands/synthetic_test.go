package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadSyntheticCommandIsRegistered(t *testing.T) {
	command, found := Registry()["load-synthetic"]
	require.True(t, found)
	require.NotNil(t, command.Fn)
	require.Equal(t, []string{"[flags]", "<connection>"}, command.args)
	require.Equal(t, "Loads a deterministic synthetic Active Directory graph", command.help)
	require.Contains(t, SortedCommandNames(), "load-synthetic")
}

func TestLoadSyntheticCommandRequiresConnection(t *testing.T) {
	ctx := newPlainCommandContext(t)

	err := loadSyntheticCmd().Fn(ctx, nil)
	require.ErrorContains(t, err, "invalid usage: load-synthetic")
	require.Empty(t, ctx.OutputString())
}

func TestFormatSyntheticCountsIsSorted(t *testing.T) {
	require.Equal(t, "Computer=2, User=10", formatSyntheticCounts(map[string]int64{
		"User":     10,
		"Computer": 2,
	}))
}
