package algo

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gammazero/deque"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type reachCursor struct {
	component   uint64
	adjacent    []uint64
	adjacentIdx int
	reach       cardinality.Duplex[uint64]
	ancestor    *reachCursor
}

func (s *reachCursor) Complete() {
	if s.ancestor != nil {
		s.ancestor.reach.Or(s.reach)
	}
}

func (s *reachCursor) NextAdjacent() (uint64, bool) {
	if s.adjacentIdx < len(s.adjacent) {
		next := s.adjacent[s.adjacentIdx]
		s.adjacentIdx += 1

		return next, true
	}

	return 0, false
}

type ReachabilityCacheStats struct {
	Cached uint64
	Hits   uint64
}

type reachabilityCacheLookup struct {
	ComponentReach cardinality.Duplex[uint64]
	MemberReach    cardinality.Duplex[uint64]
}

type ReachabilityCache struct {
	Components             ComponentGraph
	inboundComponentReach  map[uint64]cardinality.Duplex[uint64]
	outboundComponentReach map[uint64]cardinality.Duplex[uint64]
	inboundMemberReach     map[uint64]cardinality.Duplex[uint64]
	outboundMemberReach    map[uint64]cardinality.Duplex[uint64]
	cacheHits              *atomic.Uint64
	maxCacheSize           int
	resolved               cardinality.Duplex[uint64]
	cacheLock              *sync.RWMutex
}

func NewReachabilityCache(ctx context.Context, digraph container.DirectedGraph, maxCacheSize int) *ReachabilityCache {
	return &ReachabilityCache{
		Components:             NewComponentGraph(ctx, digraph),
		inboundComponentReach:  make(map[uint64]cardinality.Duplex[uint64]),
		inboundMemberReach:     make(map[uint64]cardinality.Duplex[uint64]),
		outboundComponentReach: make(map[uint64]cardinality.Duplex[uint64]),
		outboundMemberReach:    make(map[uint64]cardinality.Duplex[uint64]),
		cacheHits:              &atomic.Uint64{},
		maxCacheSize:           maxCacheSize,
		resolved:               cardinality.NewBitmap64(),
		cacheLock:              &sync.RWMutex{},
	}
}

func (s *ReachabilityCache) newReachCursor(component uint64, direction graph.Direction, previous *reachCursor) *reachCursor {
	var (
		adjacentComponents = container.AdjacentNodes(s.Components.Digraph(), component, direction)
		componentReach     = cardinality.NewBitmap64With(append(adjacentComponents, component)...)
	)

	return &reachCursor{
		component:   component,
		adjacent:    adjacentComponents,
		adjacentIdx: 0,
		reach:       componentReach,
		ancestor:    previous,
	}
}

func (s *ReachabilityCache) newRootReachCursor(component uint64, direction graph.Direction) *reachCursor {
	var (
		adjacentComponents = container.AdjacentNodes(s.Components.Digraph(), component, direction)
		componentReach     = cardinality.NewBitmap64With(append(adjacentComponents, component)...)
	)

	return &reachCursor{
		component:   component,
		adjacent:    adjacentComponents,
		adjacentIdx: 0,
		reach:       componentReach,
	}
}

func (s *ReachabilityCache) Stats() ReachabilityCacheStats {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return ReachabilityCacheStats{
		Cached: s.resolved.Cardinality(),
		Hits:   s.cacheHits.Load(),
	}
}

func (s *ReachabilityCache) CanReach(startID, endID uint64, direction graph.Direction) bool {
	var (
		startComponent, hasStart = s.Components.ContainingComponent(startID)
		endComponent, hasEnd     = s.Components.ContainingComponent(endID)
	)

	if hasStart && hasEnd {
		return s.Components.ComponentReachable(startComponent, endComponent, direction)
	}

	return false
}

func (s *ReachabilityCache) canCacheComponent(component uint64) bool {
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	return !s.resolved.Contains(component) && len(s.inboundComponentReach)+len(s.outboundComponentReach) < s.maxCacheSize
}

func (s *ReachabilityCache) cacheComponentReach(component uint64, direction graph.Direction, reach cardinality.Duplex[uint64]) {
	if !s.canCacheComponent(component) {
		return
	}

	// Collect the component members outside of the lock
	componentMembers := cardinality.NewBitmap64()

	reach.Each(func(reachableComponent uint64) bool {
		s.Components.CollectComponentMembers(reachableComponent, componentMembers)
		return true
	})

	// Lock the cache to save the updated component reach and component member reach
	s.cacheLock.Lock()
	defer s.cacheLock.Unlock()

	if s.resolved.CheckedAdd(component) {
		switch direction {
		case graph.DirectionInbound:
			s.inboundComponentReach[component] = reach
			s.inboundMemberReach[component] = componentMembers

		case graph.DirectionOutbound:
			s.outboundComponentReach[component] = reach
			s.outboundMemberReach[component] = componentMembers
		}
	}
}

func (s *ReachabilityCache) cachedComponentReach(component uint64, direction graph.Direction) (reachabilityCacheLookup, bool) {
	// Take the read lock to do a contains check for this component's cached reach
	s.cacheLock.RLock()
	defer s.cacheLock.RUnlock()

	if s.resolved.Contains(component) {
		var (
			cachedComponentReach cardinality.Duplex[uint64]
			cachedMemberReach    cardinality.Duplex[uint64]
			componentReachCached bool
		)

		switch direction {
		case graph.DirectionInbound:
			cachedComponentReach, componentReachCached = s.inboundComponentReach[component]
			cachedMemberReach = s.inboundMemberReach[component]

		case graph.DirectionOutbound:
			cachedComponentReach, componentReachCached = s.outboundComponentReach[component]
			cachedMemberReach = s.outboundMemberReach[component]
		}

		if componentReachCached {
			s.cacheHits.Add(1)

			return reachabilityCacheLookup{
				ComponentReach: cachedComponentReach,
				MemberReach:    cachedMemberReach,
			}, true
		}
	}

	return reachabilityCacheLookup{}, false
}

func (s *ReachabilityCache) componentMemberReachDFS(component uint64, direction graph.Direction) {
	var (
		stack             deque.Deque[*reachCursor]
		visitedComponents = cardinality.NewBitmap64()
	)

	// Mark the root component as visited and add it to the stack
	visitedComponents.Add(component)
	stack.PushBack(s.newRootReachCursor(component, direction))

	for stack.Len() > 0 {
		reachCursor := stack.Back()

		if nextAdjacent, hasNext := reachCursor.NextAdjacent(); !hasNext {
			stack.PopBack()

			// Complete the cursor to roll up reach cardinalities
			reachCursor.Complete()

			// Update the cache with this component's reach
			s.cacheComponentReach(reachCursor.component, direction, reachCursor.reach)
		} else if visitedComponents.CheckedAdd(nextAdjacent) {
			if cachedReach, cached := s.cachedComponentReach(nextAdjacent, direction); cached {
				visitedComponents.Or(cachedReach.ComponentReach)

				if reachCursor.reach != visitedComponents {
					reachCursor.reach.Or(cachedReach.ComponentReach)
				}
			} else {
				stack.PushBack(s.newReachCursor(nextAdjacent, direction, reachCursor))
			}
		}
	}
}

func (s *ReachabilityCache) ReachOfComponentContainingMember(member uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if rootComponent, rootInComponent := s.Components.ContainingComponent(member); rootInComponent {
		if cachedReach, cached := s.cachedComponentReach(rootComponent, direction); cached {
			return cachedReach.MemberReach
		} else {
			s.componentMemberReachDFS(rootComponent, direction)

			if cachedReach, cached := s.cachedComponentReach(rootComponent, direction); cached {
				return cachedReach.MemberReach
			}
		}
	}

	return cardinality.NewBitmap64()
}

func (s *ReachabilityCache) OrReach(node uint64, direction graph.Direction, duplex cardinality.Duplex[uint64]) {
	// Reach bitmap will contain the member due to resolution of component reach
	duplex.Or(s.ReachOfComponentContainingMember(node, direction))
	duplex.Remove(node)
}

func (s *ReachabilityCache) XorReach(node uint64, direction graph.Direction, duplex cardinality.Duplex[uint64]) {
	// Reach bitmap will contain the member due to resolution of component reach
	reachBitmap := s.ReachOfComponentContainingMember(node, direction).Clone()
	reachBitmap.Remove(node)

	duplex.Xor(reachBitmap)
}

func edgesFilteredByKinds(kinds ...graph.Kind) graph.Criteria {
	return query.KindIn(query.Relationship(), kinds...)
}

func FetchReachabilityCache(ctx context.Context, db graph.Database, criteria graph.Criteria) (*ReachabilityCache, error) {
	if digraph, err := container.FetchDirectedGraph(ctx, db, criteria); err != nil {
		return nil, err
	} else {
		// TODO: Present a more sane config here
		return NewReachabilityCache(ctx, digraph, 1_700_000), nil
	}
}

func FetchFilteredReachabilityCache(ctx context.Context, db graph.Database, traversalKinds ...graph.Kind) (*ReachabilityCache, error) {
	return FetchReachabilityCache(ctx, db, edgesFilteredByKinds(traversalKinds...))
}
