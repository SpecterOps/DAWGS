package algo

import (
	"context"
	"sync"

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
// for components completed during single-direction traversals. Only complete
// results are cached, including intermediate components visited while serving
// a requested root. The caches are sized by the caller and use second-chance
// eviction when full.
//
// Two additional mechanisms shape traversal behavior. memoCapacity bounds how
// many complete intermediate results a single traversal retains in its local
// memo; zero means unbounded. If a completed closure is lost to eviction,
// traversal falls back to one visited-set walk rather than recomputing it. When
// inFlight is non-nil, concurrent traversals computing the same
// component/direction closure coordinate through it instead of duplicating
// work during cold-start bursts.
type ReachabilityCache struct {
	components             ComponentGraph
	inboundComponentReach  cache.Cache[uint64, cardinality.Duplex[uint64]]
	outboundComponentReach cache.Cache[uint64, cardinality.Duplex[uint64]]
	memoCapacity           int
	inFlight               *reachFlight
}

// reachShardCount spreads shared-cache lock and statistics traffic across
// independent sieve shards for concurrently used caches.
const reachShardCount = 16

// reachMemoCapacityFloor keeps very small caches from retaining a uselessly
// small traversal memo.
const reachMemoCapacityFloor = 1024

// NewReachabilityCache creates a ReachabilityCache for the supplied directed
// graph. The component graph is built first, then two bounded caches are
// allocated with the given maxCacheSize (the maximum number of component
// reachability entries to retain per direction, with a minimum of one). The
// caches are sharded so concurrent traversals do not serialize on a single
// lock, traversal memos are bounded by maxCacheSize (with a floor), and
// concurrent duplicate computations are coordinated.
func NewReachabilityCache(ctx context.Context, digraph container.DirectedGraph, maxCacheSize int) *ReachabilityCache {
	return newReachabilityCache(NewComponentGraph(ctx, digraph), maxCacheSize)
}

func newReachabilityCache(components ComponentGraph, maxCacheSize int) *ReachabilityCache {
	shardFn := func(key uint64) uint64 { return key }

	return &ReachabilityCache{
		components:             components,
		inboundComponentReach:  cache.NewShardedSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize, reachShardCount, shardFn),
		outboundComponentReach: cache.NewShardedSieve[uint64, cardinality.Duplex[uint64]](maxCacheSize, reachShardCount, shardFn),
		memoCapacity:           max(reachMemoCapacityFloor, maxCacheSize),
		inFlight:               NewReachFlight(),
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

// reachCursor tracks the depth-first traversal state for a single component.
// It stores the component identifier, the component's adjacent components in
// the traversal direction, the next adjacent index to explore, the bitmap of
// components reachable from this component (including itself), a pointer to
// the parent cursor that receives this cursor's reach on completion and, when
// this traversal registered the component with the in-flight registry, the
// registration to settle when the cursor completes.
type reachCursor struct {
	component   uint64
	adjacent    []uint64
	adjacentIdx int
	reach       cardinality.Duplex[uint64]
	parent      *reachCursor
	lead        *reachLead
}

// NextAdjacent returns the next adjacent component identifier to be explored.
// The boolean indicates whether a component was returned (true) or the slice
// has been exhausted (false). The internal index is advanced on each call.
func (s *reachCursor) NextAdjacent() (uint64, bool) {
	if s.adjacentIdx < len(s.adjacent) {
		next := s.adjacent[s.adjacentIdx]
		s.adjacentIdx += 1

		return next, true
	}

	return 0, false
}

// adjacencyProvider is implemented by digraphs that can expose a node's
// adjacency without copying, such as CSR-backed digraphs. The returned slice
// aliases the digraph's storage and must never be mutated.
type adjacencyProvider interface {
	AdjacentNodes(node uint64, direction graph.Direction) []uint64
}

// adjacencySlice returns the components adjacent to the given component in the
// traversal direction. The result is a borrowed view for CSR-backed digraphs
// (the production component graph) and a freshly built slice otherwise.
func (s *ReachabilityCache) adjacencySlice(component uint64, direction graph.Direction) []uint64 {
	if provider, ok := s.components.Digraph().(adjacencyProvider); ok {
		return provider.AdjacentNodes(component, direction)
	}

	return container.AdjacentNodes(s.components.Digraph(), component, direction)
}

// reachMemo retains complete reach sets in FIFO order. evicted remembers IDs
// after their bitmaps are released, so losing a result cannot cause repeated
// expansion of a completed subgraph. It retains IDs, not closure bitmaps.
type reachMemo struct {
	entries  map[uint64]cardinality.Duplex[uint64]
	order    []uint64
	head     int
	capacity int
	evicted  cardinality.Duplex[uint64]
}

func newReachMemo(capacity int) reachMemo {
	orderHint := capacity
	if orderHint > 1024 {
		orderHint = 1024
	}

	return reachMemo{
		entries:  make(map[uint64]cardinality.Duplex[uint64]),
		order:    make([]uint64, 0, orderHint),
		capacity: capacity,
	}
}

func (m *reachMemo) get(component uint64) (cardinality.Duplex[uint64], bool) {
	reach, known := m.entries[component]
	return reach, known
}

func (m *reachMemo) put(component uint64, reach cardinality.Duplex[uint64]) {
	// Every call site puts only after a memo miss. Evicted results may be
	// restored from the shared cache without expanding their subgraphs again.
	if m.capacity > 0 && len(m.entries) >= m.capacity {
		// Evict the oldest still-present entry. Order slots whose key was
		// already evicted (and possibly re-inserted later) are skipped.
		for m.head < len(m.order) {
			victim := m.order[m.head]
			m.head++

			if _, present := m.entries[victim]; present {
				delete(m.entries, victim)
				if m.evicted == nil {
					m.evicted = cardinality.NewBitmap64()
				}
				m.evicted.Add(victim)
				break
			}
		}
	}

	m.entries[component] = reach
	m.order = append(m.order, component)

	// Compact the FIFO once consumed slots dominate it.
	if m.head > len(m.order)/2 {
		remaining := copy(m.order, m.order[m.head:])
		m.order = m.order[:remaining]
		m.head = 0
	}
}

func (m *reachMemo) wasEvicted(component uint64) bool {
	return m.evicted != nil && m.evicted.Contains(component)
}

// reachFlightKey identifies an in-progress closure computation. Direction is
// part of the key because a component's inbound and outbound closures differ.
type reachFlightKey struct {
	component uint64
	direction graph.Direction
}

// reachFlightEntry is one in-progress computation. result is assigned once,
// before done closes; a nil result means the leading traversal abandoned the
// computation and waiters must compute the closure themselves.
type reachFlightEntry struct {
	done   chan struct{}
	result cardinality.Duplex[uint64]
}

// reachFlight coordinates concurrent traversals that would compute the same
// component/direction closure. The first traversal to reach a component
// becomes its leader and eventually publishes the complete result; concurrent
// traversals either wait for that result or recompute locally.
//
// To keep the wait graph acyclic, a traversal only waits while it leads no
// registrations of its own: leaders never wait, and pure followers lead
// nothing, so no cycle of mutually waiting traversals can form.
type reachFlight struct {
	mu      sync.RWMutex
	entries map[reachFlightKey]*reachFlightEntry
}

// NewReachFlight creates an empty in-flight registry.
func NewReachFlight() *reachFlight {
	return &reachFlight{
		entries: map[reachFlightKey]*reachFlightEntry{},
	}
}

// register records the caller as leader for the component unless another
// traversal is already computing it. The boolean reports whether the caller
// became the leader.
func (s *reachFlight) register(key reachFlightKey) (*reachFlightEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, inProgress := s.entries[key]; inProgress {
		return existing, false
	}

	entry := &reachFlightEntry{done: make(chan struct{})}
	s.entries[key] = entry

	return entry, true
}

// complete publishes the leader's result (nil signals abandonment), removes
// the registry entry if it is still the registered one and wakes all waiters.
func (s *reachFlight) complete(key reachFlightKey, entry *reachFlightEntry, result cardinality.Duplex[uint64]) {
	s.mu.Lock()
	if s.entries[key] == entry {
		delete(s.entries, key)
	}
	s.mu.Unlock()

	entry.result = result
	close(entry.done)
}

// reachLead pairs an in-flight registration with its settlement state so that
// deferred cleanup can abandon registrations a panicked traversal never
// completed.
type reachLead struct {
	key     reachFlightKey
	entry   *reachFlightEntry
	settled bool
}

// settleLead publishes a completed closure for one of this traversal's
// in-flight registrations and wakes its waiters. The lead must be non-nil;
// call sites only register leads for components they descend into.
func (s *ReachabilityCache) settleLead(lead *reachLead, result cardinality.Duplex[uint64]) {
	lead.settled = true
	s.inFlight.complete(lead.key, lead.entry, result)
	// Followers own their entry references. Cleanup only needs the settled
	// flag, so release our reference to its completed bitmap immediately.
	lead.entry = nil
}

func (s *ReachabilityCache) abandonLeads(leads []*reachLead) {
	for _, lead := range leads {
		if !lead.settled {
			s.settleLead(lead, nil)
		}
	}
}

// componentReachDFS performs a depth-first search over the component graph to
// compute the set of components reachable from the given start component in
// the specified direction. Cached bitmaps are immutable after publication and
// can safely be merged into traversal-local results, including when their
// entries are concurrently evicted.
//
// Single-direction traversals use componentReachMemoDFS, which also publishes
// complete intermediate results. DirectionBoth permits cycles between
// components, so it is served by componentReachUncachedDFS, whose results are
// never cached.
func (s *ReachabilityCache) componentReachDFS(component uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	if cachedReach, cached := s.cachedComponentReach(component, direction); cached {
		return cachedReach
	}

	if direction == graph.DirectionInbound || direction == graph.DirectionOutbound {
		return s.componentReachMemoDFS(component, direction)
	}

	return s.componentReachUncachedDFS(component, direction)
}

// componentReachMemoDFS computes the set of components reachable from the
// given start component in a single direction using iterative post-order
// cursors.
//
// The condensation of strongly connected components is acyclic for a single
// direction, so a component's reach set is complete once every adjacent
// component has been explored. Each completed set is published to the shared
// bounded cache and recorded in a traversal-local memo before the traversal
// continues; publication happens exactly once, and published bitmaps are
// never mutated afterwards.
//
// The memo serves two purposes. It restores reuse of intermediate results
// across branches of one traversal without re-expanding them, and it keeps
// those complete results available even when the shared cache evicts their
// entries mid-traversal. If both caches lose a completed result, the traversal
// switches to a visited-set walk for the requested root. This bounds expansion
// to two walks of the reachable graph instead of allowing exponential
// re-descent. Completed intermediates remain valid; unfinished cursors are
// discarded without publication, and their registrations are abandoned.
//
// When in-flight coordination is enabled, concurrent traversals computing the
// same closure deduplicate: the traversal that first reaches a component
// leads its computation, and other traversals wait for the published result
// only while they lead nothing themselves (see reachFlight).
func (s *ReachabilityCache) componentReachMemoDFS(component uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	var (
		memo      = newReachMemo(s.memoCapacity)
		stack     deque.Deque[*reachCursor]
		leads     []*reachLead
		openLeads int
	)

	// Panic-safe settlement: any registration this traversal still leads when
	// it exits is abandoned, waking waiters with a nil result so they compute
	// the closure themselves.
	defer func() {
		if len(leads) > 0 {
			s.abandonLeads(leads)
		}
	}()

	var rootLead *reachLead
	if s.inFlight != nil {
		rootKey := reachFlightKey{component: component, direction: direction}

		if entry, leader := s.inFlight.register(rootKey); !leader {
			// This traversal leads nothing yet, so waiting cannot deadlock.
			<-entry.done

			if entry.result != nil {
				return entry.result
			}

			// The leader abandoned the computation; fall through and compute
			// locally without registering the root again.
		} else {
			rootLead = &reachLead{key: rootKey, entry: entry}
			leads = append(leads, rootLead)
			openLeads++
		}
	}

	rootCursor := &reachCursor{
		component: component,
		adjacent:  s.adjacencySlice(component, direction),
		reach:     cardinality.NewBitmap64With(component),
		lead:      rootLead,
	}

	stack.PushBack(rootCursor)

	for stack.Len() > 0 {
		nextCursor := stack.Back()

		if adjacentComponent, hasNext := nextCursor.NextAdjacent(); !hasNext {
			stack.PopBack()

			// This component's reach is complete: roll it up to the parent
			// cursor and publish it as an immutable intermediate result.
			if nextCursor.parent != nil {
				nextCursor.parent.reach.Or(nextCursor.reach)
			}

			memo.put(nextCursor.component, nextCursor.reach)
			s.cacheComponentReach(nextCursor.component, direction, nextCursor.reach)

			if nextCursor.lead != nil {
				s.settleLead(nextCursor.lead, nextCursor.reach)
				openLeads--
			}
		} else if memoReach, inMemo := memo.get(adjacentComponent); inMemo {
			// The adjacent component completed earlier in this traversal.
			// Merge its complete reach without descending into it again.
			nextCursor.reach.Or(memoReach)
		} else if cachedReach, cached := s.cachedComponentReach(adjacentComponent, direction); cached {
			// The adjacent component was completed by an earlier traversal.
			// Remember it in the memo so later branches of this traversal can
			// not lose it to a mid-traversal eviction of the shared cache.
			memo.put(adjacentComponent, cachedReach)
			nextCursor.reach.Or(cachedReach)
		} else if memo.wasEvicted(adjacentComponent) {
			// Acyclicity alone does not bound recomputation after eviction.
			// Restart once with a visited set, preserving complete publication
			// and releasing the unfinished cursors when this call returns.
			reach := s.componentReachUncachedDFS(component, direction)
			s.cacheComponentReach(component, direction, reach)
			if rootLead != nil {
				s.settleLead(rootLead, reach)
			}
			return reach
		} else {
			adjacent := s.adjacencySlice(adjacentComponent, direction)

			if len(adjacent) == 0 {
				// Leaf fast path: the component's closure is itself, so
				// complete it without a stack round-trip. Leaves complete
				// immediately and never register in flight.
				leafReach := cardinality.NewBitmap64With(adjacentComponent)
				nextCursor.reach.Add(adjacentComponent)

				memo.put(adjacentComponent, leafReach)
				s.cacheComponentReach(adjacentComponent, direction, leafReach)
				continue
			}

			var lead *reachLead
			if s.inFlight != nil {
				adjacentKey := reachFlightKey{component: adjacentComponent, direction: direction}

				if entry, leader := s.inFlight.register(adjacentKey); leader {
					lead = &reachLead{key: adjacentKey, entry: entry}
					leads = append(leads, lead)
					openLeads++
				} else if openLeads == 0 {
					// Pure follower: waiting cannot deadlock. Traversals that
					// lead registrations skip the wait and recompute locally,
					// which keeps the wait graph acyclic.
					<-entry.done

					if entry.result != nil {
						memo.put(adjacentComponent, entry.result)
						nextCursor.reach.Or(entry.result)
						continue
					}

					// The leader abandoned the computation; fall through and
					// descend locally without registering.
				}
			}

			stack.PushBack(&reachCursor{
				component: adjacentComponent,
				adjacent:  adjacent,
				reach:     cardinality.NewBitmap64With(adjacentComponent),
				parent:    nextCursor,
				lead:      lead,
			})
		}
	}

	return rootCursor.reach
}

// componentReachUncachedDFS performs a frontier walk over the component graph
// for directions whose adjacency can contain cycles, such as DirectionBoth.
// Results are computed per call and are not published to either directional
// cache; lookups in those caches never match such directions, so this walk
// consults no cache at all.
func (s *ReachabilityCache) componentReachUncachedDFS(component uint64, direction graph.Direction) cardinality.Duplex[uint64] {
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
		s.components.Digraph().EachAdjacentNode(nextComponent, direction, visitAdjacent)
	}

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
