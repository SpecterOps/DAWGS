package pg

import (
	"context"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestKindCacheRefreshWaitRespectsCancellation(t *testing.T) {
	cache := newKindCache()
	require.True(t, cache.tryLockRefresh())
	require.False(t, cache.tryLockRefresh())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cache.lockRefresh(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled metadata waiter stayed blocked")
	}
	cache.unlockRefresh()
	require.ErrorIs(t, cache.lockRefresh(ctx), context.Canceled)
	require.True(t, cache.tryLockRefresh(), "cancellation must not retain the refresh gate")
	cache.unlockRefresh()
	require.NoError(t, cache.lockRefresh(context.Background()))
	cache.unlockRefresh()
}

func TestKindCacheRequiresCompleteInitialCatalog(t *testing.T) {
	cache := newKindCache()
	require.True(t, cache.pending())
	require.False(t, cache.current.Load().loaded)
	kind := graph.StringKind("Learned")
	require.True(t, cache.merge(map[graph.Kind]int16{kind: 1}, 0))
	require.False(t, cache.current.Load().loaded, "a partial lookup does not initialize the catalog")
	require.True(t, cache.pending())

	require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{}, false, func() {}))
	require.True(t, cache.current.Load().loaded, "an empty full catalog is initialized")
	require.False(t, cache.pending())
	require.True(t, cache.merge(map[graph.Kind]int16{kind: 1}, cache.current.Load().epoch))
	require.True(t, cache.current.Load().loaded)
	cache.fence()
	require.True(t, cache.current.Load().loaded, "refreshes retain a usable initialized view")
}
