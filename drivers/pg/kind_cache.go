package pg

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs/graph"
)

// kindSnapshot is immutable after publication. epoch fences private catalog
// views across destructive reloads; pointer identity also fences raced fetches.
type kindSnapshot struct {
	kinds map[graph.Kind]int16
	ids   map[int16]graph.Kind
	epoch uint64
	// loaded distinguishes a complete empty catalog from an uninitialized one.
	loaded bool
}

type kindCache struct {
	current      atomic.Pointer[kindSnapshot]
	publication  sync.Mutex
	refresh      chan struct{}
	requested    atomic.Uint64
	acknowledged atomic.Uint64
}

func newKindCache() *kindCache {
	c := &kindCache{refresh: make(chan struct{}, 1)}
	c.current.Store(&kindSnapshot{kinds: map[graph.Kind]int16{}, ids: map[int16]graph.Kind{}})
	c.invalidate()
	return c
}

func (c *kindCache) pending() bool { return c.requested.Load() != c.acknowledged.Load() }
func (c *kindCache) invalidate()   { c.requested.Add(1) }

func (c *kindCache) lockRefresh(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.refresh <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *kindCache) tryLockRefresh() bool {
	select {
	case c.refresh <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *kindCache) unlockRefresh() { <-c.refresh }

// merge publishes only changes learned by a successfully completed operation.
// An old catalog view must never resurrect a mapping removed by a reload.
func (c *kindCache) merge(changes map[graph.Kind]int16, epoch uint64) bool {
	if len(changes) == 0 {
		return true
	}
	c.publication.Lock()
	defer c.publication.Unlock()
	current := c.current.Load()
	if current.epoch != epoch {
		c.invalidate()
		return false
	}
	changed := false
	for kind, id := range changes {
		if previous, ok := current.kinds[kind]; !ok || previous != id {
			changed = true
			break
		}
	}
	if !changed {
		return true
	}
	next := &kindSnapshot{kinds: maps.Clone(current.kinds), ids: maps.Clone(current.ids), epoch: current.epoch, loaded: current.loaded}
	for kind, id := range changes {
		if previous, ok := next.kinds[kind]; ok && previous != id {
			c.invalidate()
			return false
		}
		if previous, ok := next.ids[id]; ok && previous != kind {
			c.invalidate()
			return false
		}
		next.kinds[kind], next.ids[id] = id, kind
	}
	c.current.Store(next)
	return true
}

// fence prevents in-flight private views and fetches from publishing across an
// explicit reset. The maps remain usable if the ensuing database read fails.
func (c *kindCache) fence() {
	c.publication.Lock()
	defer c.publication.Unlock()
	old := c.current.Load()
	c.current.Store(&kindSnapshot{kinds: old.kinds, ids: old.ids, epoch: old.epoch + 1, loaded: old.loaded})
	c.invalidate()
}

// replace does no database I/O. A commit or invalidation racing a fetch leaves
// the request pending, rather than overwriting more recent metadata.
func (c *kindCache) replace(base *kindSnapshot, generation uint64, kinds map[graph.Kind]int16, force bool, invalidateTranslations func()) bool {
	c.publication.Lock()
	defer c.publication.Unlock()
	if c.current.Load() != base || c.requested.Load() != generation {
		return false
	}
	destructive := false
	for kind, id := range base.kinds {
		if next, ok := kinds[kind]; !ok || next != id {
			destructive = true
			break
		}
	}
	epoch := base.epoch
	if destructive {
		epoch++
	}
	ids := make(map[int16]graph.Kind, len(kinds))
	for kind, id := range kinds {
		ids[id] = kind
	}
	c.current.Store(&kindSnapshot{kinds: kinds, ids: ids, epoch: epoch, loaded: true})
	// Publish the new catalog before advancing translation generations, so a
	// compiler retry in the new generation cannot read the replaced catalog.
	if destructive || force {
		invalidateTranslations()
	}
	c.acknowledged.Store(generation)
	return true
}
