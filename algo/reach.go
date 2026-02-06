package algo

import (
	"context"

	"github.com/gammazero/deque"
	"github.com/specterops/dawgs/cache"
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

type ReachabilityCache struct {
	components             ComponentGraph
	inboundComponentReach  cache.Cache[uint64, cardinality.Duplex[uint64]]
	outboundComponentReach cache.Cache[uint64, cardinality.Duplex[uint64]]
}

func NewReachabilityCache(ctx context.Context, digraph container.DirectedGraph, maxCacheSize int) *ReachabilityCache {
	return &ReachabilityCache{
		components:             NewComponentGraph(ctx, digraph),
		inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize),
		outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize),
	}
}

func (s *ReachabilityCache) newReachCursor(component uint64, direction graph.Direction, previous *reachCursor) *reachCursor {
	var (
		adjacentComponents = container.AdjacentNodes(s.components.Digraph(), component, direction)
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
		adjacentComponents = container.AdjacentNodes(s.components.Digraph(), component, direction)
		componentReach     = cardinality.NewBitmap64With(component)
	)

	return &reachCursor{
		component:   component,
		adjacent:    adjacentComponents,
		adjacentIdx: 0,
		reach:       componentReach,
	}
}

func (s *ReachabilityCache) Stats() cache.Stats {
	return s.inboundComponentReach.Stats().Combined(s.outboundComponentReach.Stats())
}

func (s *ReachabilityCache) CanReach(startID, endID uint64, direction graph.Direction) bool {
	var (
		startComponent, hasStart = s.components.ContainingComponent(startID)
		endComponent, hasEnd     = s.components.ContainingComponent(endID)
	)

	if hasStart && hasEnd {
		return s.components.ComponentReachable(startComponent, endComponent, direction)
	}

	return false
}

func (s *ReachabilityCache) componentReachToMemberReachBitmap(componentReach cardinality.Duplex[uint64]) cardinality.Duplex[uint64] {
	componentMembers := cardinality.NewBitmap64()

	componentReach.Each(func(reachableComponent uint64) bool {
		s.components.CollectComponentMembers(reachableComponent, componentMembers)
		return true
	})

	return componentMembers
}

func (s *ReachabilityCache) componentReachToMemberReachSlice(componentReach cardinality.Duplex[uint64]) []cardinality.Duplex[uint64] {
	componentMembers := make([]cardinality.Duplex[uint64], 0, componentReach.Cardinality())

	componentReach.Each(func(reachableComponent uint64) bool {
		componentMembers = append(componentMembers, s.components.ComponentMembers(reachableComponent))
		return true
	})

	return componentMembers
}

func (s *ReachabilityCache) cacheComponentReach(cursor *reachCursor, direction graph.Direction) {
	switch direction {
	case graph.DirectionInbound:
		s.inboundComponentReach.Put(cursor.component, cursor.reach)

	case graph.DirectionOutbound:
		s.outboundComponentReach.Put(cursor.component, cursor.reach)
	}
}

func (s *ReachabilityCache) cachedComponentReach(component uint64, direction graph.Direction) (cardinality.Duplex[uint64], bool) {
	var (
		entry cardinality.Duplex[uint64]
		found = false
	)

	switch direction {
	case graph.DirectionInbound:
		entry, found = s.inboundComponentReach.Get(component)

	case graph.DirectionOutbound:
		entry, found = s.outboundComponentReach.Get(component)
	}

	return entry, found
}

func (s *ReachabilityCache) componentReachDFS(component uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if cachedReach, cached := s.cachedComponentReach(component, direction); cached {
		return cachedReach
	}

	var (
		stack      deque.Deque[*reachCursor]
		rootCursor = s.newRootReachCursor(component, direction)
	)

	// Mark the root component as visited and add it to the stack
	stack.PushBack(rootCursor)

	for stack.Len() > 0 {
		nextCursor := stack.Back()

		if nextAdjacentComponent, hasNext := nextCursor.NextAdjacent(); !hasNext {
			stack.PopBack()

			// Complete the cursor to roll up reach cardinalities
			nextCursor.Complete()

			// Update the cache with this component's reach
			s.cacheComponentReach(nextCursor, direction)
		} else if rootCursor.reach.CheckedAdd(nextAdjacentComponent) {
			// This is a component not yet visited, check if it is cached. If it
			// is cached, Or(...) its reach and if not traverse into it.
			if cachedReach, cached := s.cachedComponentReach(nextAdjacentComponent, direction); cached {
				nextCursor.reach.Or(cachedReach)
			} else {
				stack.PushBack(s.newReachCursor(nextAdjacentComponent, direction, nextCursor))
			}
		}
	}

	return rootCursor.reach
}

// ReachSliceOfComponentContainingMember returns the reach of the component containing the given member and direction as
// a slice of membership bitmaps. These bitmaps are not combined for the purposes of maintaining performance when dealing
// with large scale digraphs. The computation cost of producing a single bitmap far exceeds the cost of iteratively
// scanning the returned slice of bitmaps. Additionally, the returned slice may be used by commutative bitmap operations.
func (s *ReachabilityCache) ReachSliceOfComponentContainingMember(member uint64, direction graph.Direction) []cardinality.Duplex[uint64] {
	if rootComponent, rootInComponent := s.components.ContainingComponent(member); rootInComponent {
		return s.componentReachToMemberReachSlice(s.componentReachDFS(rootComponent, direction))
	}

	return nil
}

// ReachOfComponentContainingMember returns the reach of the component containing the given member and direction as a single
// bitwise ORed bitmap. For large scale digraphs use of this function may come at a high computational cost. If this function
// is utilized in a tight loop, consider utilizing ReachSliceOfComponentContainingMember with commutative bitmap opreations.
func (s *ReachabilityCache) ReachOfComponentContainingMember(member uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if rootComponent, rootInComponent := s.components.ContainingComponent(member); rootInComponent {
		return s.componentReachToMemberReachBitmap(s.componentReachDFS(rootComponent, direction))
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
		maxCacheCap := int(float64(digraph.NumNodes()) * .15)
		return NewReachabilityCache(ctx, digraph, maxCacheCap), nil
	}
}

func FetchFilteredReachabilityCache(ctx context.Context, db graph.Database, traversalKinds ...graph.Kind) (*ReachabilityCache, error) {
	return FetchReachabilityCache(ctx, db, edgesFilteredByKinds(traversalKinds...))
}
