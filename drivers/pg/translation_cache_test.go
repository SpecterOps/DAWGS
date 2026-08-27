package pg

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func cacheableBuild(sql string, parameters map[string]any, sources map[string]string) func() (string, translationCacheBuildResult, error) {
	return func() (string, translationCacheBuildResult, error) {
		return sql, translationCacheBuildResult{
			parameters:       parameters,
			parameterSources: sources,
		}, nil
	}
}

func TestTranslationCacheRebindsCurrentValues(t *testing.T) {
	translationCache := newTranslationCache(2)
	firstParameters := map[string]any{"needle": graph.ID(1)}
	key := translationCache.Key("RETURN $needle", 7, firstParameters)

	_, bindings, err := translationCache.GetOrBuild(key, firstParameters, cacheableBuild("select @p0", map[string]any{"p0": uint64(1)}, map[string]string{"p0": "needle"}))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": uint64(1)}, bindings)

	secondParameters := map[string]any{"needle": graph.ID(2)}
	builds := 0
	_, bindings, err = translationCache.GetOrBuild(translationCache.Key("RETURN $needle", 7, secondParameters), secondParameters, func() (string, translationCacheBuildResult, error) {
		builds++
		return "", translationCacheBuildResult{}, nil
	})
	require.NoError(t, err)
	require.Zero(t, builds)
	require.Equal(t, map[string]any{"p0": uint64(2)}, bindings)
	require.Equal(t, int64(1), translationCache.Stats().Hits)
}

func TestTranslationCachePartitionsKeysAndBypassesUnsafeResults(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)

	require.NotEqual(t, key, translationCache.Key("RETURN $other", 1, parameters))
	require.NotEqual(t, key, translationCache.Key("RETURN $value", 2, parameters))
	require.NotEqual(t, key, translationCache.Key("RETURN $value", 1, map[string]any{"other": int64(1)}))
	require.NotEqual(t, key, translationCache.Key("RETURN $value", 1, map[string]any{"value": "1"}))
	translationCache.Invalidate()
	require.NotEqual(t, key, translationCache.Key("RETURN $value", 1, parameters))
	key = translationCache.Key("RETURN $value", 1, parameters)

	builds := 0
	for range 2 {
		_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			builds++
			return "select @p0", translationCacheBuildResult{
				parameters: map[string]any{"p0": int64(1)},
			}, nil
		})
		require.NoError(t, err)
	}
	require.Equal(t, 2, builds, "a parameter without provenance must not be retained")
}

func TestTranslationCacheKeyUsesDigestWithoutSourceText(t *testing.T) {
	translationCache := newTranslationCache(1)
	key := translationCache.Key("RETURN $secret // customer-value", 1, map[string]any{"secret": "value"})
	other := translationCache.Key("RETURN $other", 1, map[string]any{"secret": "value"})

	require.Equal(t, len("RETURN $secret // customer-value"), key.querySize)
	require.NotEqual(t, key.queryDigest, other.queryDigest)
}

func TestTranslationCacheBypassesOversizedSource(t *testing.T) {
	translationCache := newTranslationCache(1)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key(strings.Repeat("x", maxCachedCypherBytes+1), 1, parameters)
	builds := 0

	for range 2 {
		_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			builds++
			return "select @p0", translationCacheBuildResult{
				parameters:       map[string]any{"p0": int64(1)},
				parameterSources: map[string]string{"p0": "value"},
			}, nil
		})
		require.NoError(t, err)
	}

	require.Equal(t, 2, builds)
	require.Equal(t, int64(2), translationCache.Stats().Bypasses)
}

func TestCacheableTranslationRequiresExactParameterProvenance(t *testing.T) {
	_, cacheable := cacheableTranslation("select @p0", translationCacheBuildResult{
		parameters:       map[string]any{"p0": int64(1)},
		parameterSources: map[string]string{"p1": "value"},
	}, map[string]any{"value": int64(1)}, nil)

	require.False(t, cacheable)
}

func TestTranslationCacheReportsEviction(t *testing.T) {
	translationCache := newTranslationCache(1)
	parameters := map[string]any{"value": int64(1)}

	_, _, err := translationCache.GetOrBuild(translationCache.Key("RETURN $value", 1, parameters), parameters, cacheableBuild("select @p0", map[string]any{"p0": int64(1)}, map[string]string{"p0": "value"}))
	require.NoError(t, err)
	_, _, err = translationCache.GetOrBuild(translationCache.Key("RETURN $value + 1", 1, parameters), parameters, cacheableBuild("select @p0", map[string]any{"p0": int64(1)}, map[string]string{"p0": "value"}))
	require.NoError(t, err)

	stats := translationCache.Stats()
	require.Equal(t, int64(1), stats.Evictions)
	require.Equal(t, int64(1), stats.Size)
}

func TestTranslationCacheCoalescesCacheableMisses(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	started := make(chan struct{})
	release := make(chan struct{})
	errs := make(chan error, 8)
	var builds atomic.Int64
	var group sync.WaitGroup

	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
				if builds.Add(1) == 1 {
					close(started)
					<-release
				}
				return "select @p0", translationCacheBuildResult{
					parameters:       map[string]any{"p0": int64(1)},
					parameterSources: map[string]string{"p0": "value"},
				}, nil
			})
			errs <- err
		}()
	}

	<-started
	time.Sleep(10 * time.Millisecond)
	close(release)
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), builds.Load())
}

func TestTranslationCacheCoalescedRequestsBindTheirOwnValues(t *testing.T) {
	translationCache := newTranslationCache(2)
	source := "RETURN $value"
	leaderParameters := map[string]any{"value": graph.ID(1)}
	key := translationCache.Key(source, 1, leaderParameters)
	started := make(chan struct{})
	release := make(chan struct{})

	type result struct {
		bindings map[string]any
		err      error
	}
	leaderDone := make(chan result, 1)
	go func() {
		_, bindings, err := translationCache.GetOrBuild(key, leaderParameters, func() (string, translationCacheBuildResult, error) {
			close(started)
			<-release
			return "select @p0", translationCacheBuildResult{
				parameters:       map[string]any{"p0": uint64(1)},
				parameterSources: map[string]string{"p0": "value"},
			}, nil
		})
		leaderDone <- result{
			bindings: bindings,
			err:      err,
		}
	}()

	<-started
	values := []graph.ID{2, 3, 4, 5}
	waiters := make(chan result, len(values))
	for _, value := range values {
		value := value
		go func() {
			parameters := map[string]any{"value": value}
			_, bindings, err := translationCache.GetOrBuild(translationCache.Key(source, 1, parameters), parameters, func() (string, translationCacheBuildResult, error) {
				return "", translationCacheBuildResult{}, errors.New("coalesced request unexpectedly rebuilt the translation")
			})
			waiters <- result{
				bindings: bindings,
				err:      err,
			}
		}()
	}

	close(release)
	leader := <-leaderDone
	require.NoError(t, leader.err)
	require.Equal(t, map[string]any{"p0": uint64(1)}, leader.bindings)

	seen := map[uint64]struct{}{}
	for range values {
		waiter := <-waiters
		require.NoError(t, waiter.err)
		value, ok := waiter.bindings["p0"].(uint64)
		require.True(t, ok)
		seen[value] = struct{}{}
	}
	require.Equal(t, map[uint64]struct{}{2: {}, 3: {}, 4: {}, 5: {}}, seen)
}

func TestTranslationCacheWaiterCancellationDoesNotInterruptLeader(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": graph.ID(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan error, 1)

	go func() {
		_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			close(started)
			<-release
			return "select @p0", translationCacheBuildResult{
				parameters:       map[string]any{"p0": uint64(1)},
				parameterSources: map[string]string{"p0": "value"},
			}, nil
		})
		leaderDone <- err
	}()

	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := translationCache.GetOrBuildContext(ctx, key, parameters, func() (string, translationCacheBuildResult, error) {
		return "", translationCacheBuildResult{}, errors.New("cancelled request unexpectedly rebuilt the translation")
	})
	require.ErrorIs(t, err, context.Canceled)

	close(release)
	require.NoError(t, <-leaderDone)
	_, bindings, err := translationCache.GetOrBuild(key, map[string]any{"value": graph.ID(2)}, func() (string, translationCacheBuildResult, error) {
		return "", translationCacheBuildResult{}, errors.New("completed leader did not publish a reusable translation")
	})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"p0": uint64(2)}, bindings)
}

func TestTranslationCacheParameterlessHitAndClose(t *testing.T) {
	translationCache := newTranslationCache(1)
	key := translationCache.Key("RETURN 1", 1, nil)
	build := cacheableBuild("select 1", map[string]any{}, map[string]string{})

	_, bindings, err := translationCache.GetOrBuild(key, nil, build)
	require.NoError(t, err)
	require.Empty(t, bindings)
	_, bindings, err = translationCache.GetOrBuild(key, nil, build)
	require.NoError(t, err)
	require.Nil(t, bindings)

	translationCache.Close()
	builds := 0
	_, _, err = translationCache.GetOrBuild(key, nil, func() (string, translationCacheBuildResult, error) {
		builds++
		return "select 1", translationCacheBuildResult{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, builds)
}

func TestTranslationCacheNonCacheableWaitersBuildIndependently(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	uncachedStarted := make(chan struct{}, 7)
	releaseUncached := make(chan struct{})
	errs := make(chan error, 8)
	var builds atomic.Int64
	var group sync.WaitGroup

	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
				if builds.Add(1) == 1 {
					close(leaderStarted)
					<-releaseLeader
				} else {
					uncachedStarted <- struct{}{}
					<-releaseUncached
				}

				return "select @p0", translationCacheBuildResult{
					parameters: map[string]any{"p0": int64(1)},
				}, nil
			})
			errs <- err
		}()
	}

	<-leaderStarted
	time.Sleep(10 * time.Millisecond)
	close(releaseLeader)
	for range 7 {
		select {
		case <-uncachedStarted:
		case <-time.After(time.Second):
			t.Fatal("non-cacheable waiters were serialized")
		}
	}
	close(releaseUncached)
	group.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(8), builds.Load())
}

func TestTranslationCachePanicReleasesWaiters(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	leaderDone := make(chan struct{})
	panicValue := make(chan any, 1)
	waiterDone := make(chan error, 1)

	go func() {
		defer close(leaderDone)
		defer func() {
			panicValue <- recover()
		}()
		_, _, _ = translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			close(leaderStarted)
			<-releaseLeader
			panic("boom")
		})
	}()

	<-leaderStarted
	go func() {
		_, _, err := translationCache.GetOrBuild(key, parameters, cacheableBuild(
			"select @p0",
			map[string]any{"p0": int64(1)},
			map[string]string{"p0": "value"},
		))
		waiterDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	close(releaseLeader)
	<-leaderDone
	require.Equal(t, "boom", <-panicValue)
	select {
	case err := <-waiterDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("panic left a cache waiter blocked")
	}
}

func TestTranslationCacheCloseReleasesWaiters(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	leaderDone := make(chan struct{})
	waiterDone := make(chan error, 1)

	go func() {
		defer close(leaderDone)
		_, _, _ = translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			close(leaderStarted)
			<-releaseLeader
			return "select @p0", translationCacheBuildResult{}, nil
		})
	}()

	<-leaderStarted
	go func() {
		_, _, err := translationCache.GetOrBuildContext(context.Background(), key, parameters, cacheableBuild(
			"select @p0",
			map[string]any{"p0": int64(1)},
			map[string]string{"p0": "value"},
		))
		waiterDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	translationCache.Close()
	select {
	case err := <-waiterDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cache close left a waiter blocked")
	}
	close(releaseLeader)
	<-leaderDone
}

func TestTranslationCacheInvalidationDoesNotPublishStaleBuild(t *testing.T) {
	translationCache := newTranslationCache(2)
	parameters := map[string]any{"value": int64(1)}
	key := translationCache.Key("RETURN $value", 1, parameters)
	started := make(chan struct{})
	release := make(chan struct{})
	var builds atomic.Int64

	done := make(chan error, 1)
	go func() {
		_, _, err := translationCache.GetOrBuild(key, parameters, func() (string, translationCacheBuildResult, error) {
			if builds.Add(1) == 1 {
				close(started)
				<-release
			}
			return "select @p0", translationCacheBuildResult{
				parameters:       map[string]any{"p0": int64(1)},
				parameterSources: map[string]string{"p0": "value"},
			}, nil
		})
		done <- err
	}()

	<-started
	translationCache.Invalidate()
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int64(2), builds.Load())
	require.Equal(t, int64(1), translationCache.Stats().Size)
	require.Equal(t, uint64(1), translationCache.Stats().Generation)
}
