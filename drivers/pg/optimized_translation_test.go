package pg

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func setOptimizedTranslationForTest(t testing.TB, enabled bool) {
	t.Helper()
	previous := SetOptimizedTranslation(enabled)
	t.Cleanup(func() {
		SetOptimizedTranslation(previous)
	})
}

func TestOptimizedTranslationSwitch(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	require.True(t, OptimizedTranslationEnabled())

	previous := SetOptimizedTranslation(false)
	require.True(t, previous)
	require.False(t, OptimizedTranslationEnabled())

	previous = SetOptimizedTranslation(true)
	require.False(t, previous)
	require.True(t, OptimizedTranslationEnabled())
}

func TestOptimizedTranslationSwitchConcurrentAccess(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	var group sync.WaitGroup
	for index := range 16 {
		group.Add(1)
		go func(enabled bool) {
			defer group.Done()
			for range 100 {
				SetOptimizedTranslation(enabled)
				_ = OptimizedTranslationEnabled()
			}
		}(index%2 == 0)
	}
	group.Wait()

	SetOptimizedTranslation(true)
	require.True(t, OptimizedTranslationEnabled())
}
