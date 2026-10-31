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

// ReachabilityCacheStats aggregates cache statistics for both inbound and
// outbound component‑reach caches.
type ReachabilityCacheStats struct {
	Cached uint64
	Hits   uint64
}

// ReachabilityCache holds a component graph derived from a directed graph and
// two caches (inbound and outbound) that store the reachability set
// for requested components. Only complete results are cached. The caches are
// sized by the caller and use second-chance eviction when full.
type ReachabilityCache struct {
	components             ComponentGraph
	inboundComponentReach  cache.Cache[uint64, cardinality.Duplex[uint64]]
	outboundComponentReach cache.Cache[uint64, cardinality.Duplex[uint64]]
}

// NewReachabilityCache creates a ReachabilityCache for the supplied directed
// graph. The component graph is built first, then two bounded caches are
// allocated with the given maxCacheSize (the maximum number of component
// reachability entries to retain per direction, with a minimum of one).
func NewReachabilityCache(ctx context.Context, digraph container.DirectedGraph, maxCacheSize int) *ReachabilityCache {
	return &ReachabilityCache{
		components:             NewComponentGraph(ctx, digraph),
		inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize),
		outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize),
	}
}

// Stats returns the combined cache statistics for both inbound and outbound
// component‑reach caches.
func (s *ReachabilityCache) Stats() cache.Stats {
	return s.inboundComponentReach.Stats().Combined(s.outboundComponentReach.Stats())
}

// CanReach determines whether a directed path exists from startID to endID in
// the original graph, following the supplied direction (inbound or outbound).
// The method works on the component graph: if both IDs belong to known
// components, it asks the component graph whether the start component can
// reach the end component.
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

// componentReachToMemberReachBitmap converts a bitmap of reachable component
// identifiers into a bitmap of reachable member identifiers. For each
// component in componentReach the method adds all of its member nodes to the
// returned bitmap.
func (s *ReachabilityCache) componentReachToMemberReachBitmap(componentReach cardinality.Duplex[uint64]) cardinality.Duplex[uint64] {
	componentMembers := cardinality.NewBitmap64()

	componentReach.Each(func(reachableComponent uint64) bool {
		s.components.CollectComponentMembers(reachableComponent, componentMembers)
		return true
	})

	return componentMembers
}

// componentReachToMemberReachSlice converts a bitmap of reachable component
// identifiers into a slice where each element is a bitmap of the members of a
// single reachable component. This representation avoids the cost of
// materialising a single huge bitmap when the graph is very large.
func (s *ReachabilityCache) componentReachToMemberReachSlice(componentReach cardinality.Duplex[uint64]) []cardinality.Duplex[uint64] {
	componentMembers := make([]cardinality.Duplex[uint64], 0, componentReach.Cardinality())

	componentReach.Each(func(reachableComponent uint64) bool {
		componentMembers = append(componentMembers, s.components.ComponentMembers(reachableComponent))
		return true
	})

	return componentMembers
}

// cacheComponentReach stores the reach bitmap for a component in the appropriate
// cache (inbound or outbound) based on the supplied direction.
func (s *ReachabilityCache) cacheComponentReach(component uint64, direction graph.Direction, reach cardinality.Duplex[uint64]) {
	switch direction {
	case graph.DirectionInbound:
		s.inboundComponentReach.Put(component, reach)

	case graph.DirectionOutbound:
		s.outboundComponentReach.Put(component, reach)
	}
}

// cachedComponentReach attempts to retrieve a component’s reach bitmap from the
// cache. The boolean indicates whether the entry was present.
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

// componentReachDFS performs a depth-first search over the component graph to
// compute the set of components reachable from the given start component in
// the specified direction. Only the requested component's completed result is
// cached: a shared visited set cannot establish intermediate components' reach.
// Cached bitmaps are immutable after publication and can safely be merged into
// the local result, including when their entries are concurrently evicted.
func (s *ReachabilityCache) componentReachDFS(component uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if cachedReach, cached := s.cachedComponentReach(component, direction); cached {
		return cachedReach
	}

	var (
		stack deque.Deque[uint64]
		reach = cardinality.NewBitmap64With(component)
	)
	visitAdjacent := func(adjacent uint64) bool {
		if reach.CheckedAdd(adjacent) {
			stack.PushBack(adjacent)
		}
		return true
	}

	stack.PushBack(component)

	for stack.Len() > 0 {
		nextComponent := stack.PopBack()
		if nextComponent != component {
			if cachedReach, cached := s.cachedComponentReach(nextComponent, direction); cached {
				reach.Or(cachedReach)
				continue
			}
		}

		s.components.Digraph().EachAdjacentNode(nextComponent, direction, visitAdjacent)
	}

	s.cacheComponentReach(component, direction, reach)
	return reach
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
// is utilized in a tight loop, consider utilizing ReachSliceOfComponentContainingMember with commutative bitmap operations.
func (s *ReachabilityCache) ReachOfComponentContainingMember(member uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if rootComponent, rootInComponent := s.components.ContainingComponent(member); rootInComponent {
		return s.componentReachToMemberReachBitmap(s.componentReachDFS(rootComponent, direction))
	}

	return cardinality.NewBitmap64()
}

// OrReach OR‑s the reachability set of the given node (in the supplied
// direction) into the provided duplex bitmap. The node itself is removed
// from the result so that only other reachable members remain.
func (s *ReachabilityCache) OrReach(node uint64, direction graph.Direction, duplex cardinality.Duplex[uint64]) {
	// Reach bitmap will contain the member due to resolution of component reach
	duplex.Or(s.ReachOfComponentContainingMember(node, direction))
	duplex.Remove(node)
}

// XorReach XOR‑s the reachability set of the given node (in the supplied
// direction) into the provided duplex bitmap. The node itself is removed
// from the result before the XOR operation.
func (s *ReachabilityCache) XorReach(node uint64, direction graph.Direction, duplex cardinality.Duplex[uint64]) {
	// Reach bitmap will contain the member due to resolution of component reach
	reachBitmap := s.ReachOfComponentContainingMember(node, direction).Clone()
	reachBitmap.Remove(node)

	duplex.Xor(reachBitmap)
}

// edgesFilteredByKinds returns a query. Criteria that selects only edges whose
// kind matches one of the supplied kinds.
func edgesFilteredByKinds(kinds ...graph.Kind) graph.Criteria {
	return query.KindIn(query.Relationship(), kinds...)
}

// FetchReachabilityCache builds a ReachabilityCache for the entire directed
// graph represented by the supplied database and criteria. The cache size is set
// to roughly 15% of the number of nodes in the graph (rounded down).
func FetchReachabilityCache(ctx context.Context, db graph.Database, criteria graph.Criteria) (*ReachabilityCache, error) {
	if digraph, err := container.FetchDirectedGraph(ctx, db, criteria); err != nil {
		return nil, err
	} else {
		maxCacheCap := int(float64(digraph.NumNodes()) * .15)
		return NewReachabilityCache(ctx, digraph, maxCacheCap), nil
	}
}

// FetchFilteredReachabilityCache builds a ReachabilityCache for a graph that
// contains only edges of the supplied kinds. It is a convenience wrapper
// around FetchReachabilityCache that constructs the appropriate criteria.
func FetchFilteredReachabilityCache(ctx context.Context, db graph.Database, traversalKinds ...graph.Kind) (*ReachabilityCache, error) {
	return FetchReachabilityCache(ctx, db, edgesFilteredByKinds(traversalKinds...))
}
