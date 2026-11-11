package algo

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/specterops/dawgs/cache"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestReachabilityMemoEvictionFallback drives componentReachMemoDFS with a
// memo capacity of one so that every intermediate completion evicts the
// previous memo entry. Results must remain complete, both when the sieve retains the
// intermediates and when it evicts them as aggressively as the memo does.
func TestReachabilityMemoEvictionFallback(t *testing.T) {
	adjacency := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}

	expected := map[uint64][]uint64{
		1: {1, 2, 3, 4, 5},
		2: {2, 4, 5},
		3: {3, 4, 5},
		4: {4, 5},
		5: {5},
	}

	for _, sieveCapacity := range []int{16, 1} {
		t.Run(fmt.Sprintf("sieve_capacity_%d", sieveCapacity), func(t *testing.T) {
			builder := container.NewCSRDigraphBuilder()
			for start, ends := range adjacency {
				builder.AddNode(start)
				for _, end := range ends {
					builder.AddEdge(start, end)
				}
			}

			reach := &ReachabilityCache{
				components:             NewComponentGraph(context.Background(), builder.Build()),
				inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](sieveCapacity),
				outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](sieveCapacity),
				memoCapacity:           1,
			}

			// Repeat the sweep twice: once while entries are cold and once
			// against whatever the sieve retained from the first sweep.
			// componentReachDFS returns component IDs, so expectations are
			// mapped from member IDs through the component graph.
			for round := 0; round < 2; round++ {
				for member := uint64(1); member <= 5; member++ {
					component, _ := reach.components.ContainingComponent(member)

					expectedComponents := make([]uint64, 0, len(expected[member]))
					for _, expectedMember := range expected[member] {
						expectedComponent, _ := reach.components.ContainingComponent(expectedMember)
						expectedComponents = append(expectedComponents, expectedComponent)
					}
					slices.Sort(expectedComponents)

					require.Equalf(t, expectedComponents, reach.componentReachDFS(component, graph.DirectionOutbound).Slice(), "sieve capacity %d, round %d, member %d", sieveCapacity, round, member)
				}
			}
		})
	}
}

// TestReachabilityFlightSettlementReleasesResult checks that completed flight entries release their retained closure bitmap.
func TestReachabilityFlightSettlementReleasesResult(t *testing.T) {
	for _, result := range []cardinality.Duplex[uint64]{nil, cardinality.NewBitmap64With(42)} {
		reach := &ReachabilityCache{
			inFlight: NewReachFlight(),
		}
		key := reachFlightKey{
			component: 42,
			direction: graph.DirectionOutbound,
		}
		entry, leader := reach.inFlight.register(key)
		require.True(t, leader)
		lead := &reachLead{
			key:   key,
			entry: entry,
		}
		reach.settleLead(lead, result)
		require.True(t, lead.settled)
		require.Nil(t, lead.entry, "cleanup must not retain a completed closure")
		select {
		case <-entry.done:
		default:
			t.Fatal("follower was not released")
		}
		require.Equal(t, result, entry.result, "followers must retain the published result")
		reach.abandonLeads([]*reachLead{lead})
		require.Empty(t, reach.inFlight.entries)
	}
}

// sharedLeavesDiamondGraph flushes the memo between the two paths to each
// shared predecessor. Without persistent eviction history, every level doubles
// the computation of earlier levels despite adding only two vertices.
func sharedLeavesDiamondGraph(levels int, direction graph.Direction) (container.DirectedGraph, uint64) {
	const leaves = uint64(reachMemoCapacityFloor + 26)
	builder := container.NewCSRDigraphBuilder()
	for node := uint64(0); node <= leaves; node++ {
		builder.AddNode(node)
	}
	addEdge := func(start, end uint64) {
		if direction == graph.DirectionInbound {
			builder.AddEdge(end, start)
		} else {
			builder.AddEdge(start, end)
		}
	}
	previous := leaves
	for level := 0; level < levels; level++ {
		side := leaves + uint64(level)*2 + 1
		root := side + 1
		builder.AddNode(side)
		builder.AddNode(root)
		addEdge(root, previous)
		addEdge(root, side)
		for leaf := uint64(0); leaf < leaves; leaf++ {
			addEdge(side, leaf)
		}
		addEdge(side, previous)
		previous = root
	}
	return builder.Build(), previous
}

// reachVisitCounter wraps a graph to count adjacency visits and coordinate traversal observations.
type reachVisitCounter struct {
	// DirectedGraph provides the graph behavior wrapped by adjacency observation.
	container.DirectedGraph
	// visits counts adjacency lookups for each component.
	visits map[uint64]int
	// onWalk runs the observation hook when the wrapped graph is walked.
	onWalk func()
}

// AdjacentNodes counts adjacency lookups before returning the wrapped graph's neighbors.
func (s *reachVisitCounter) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	s.visits[node]++
	return container.AdjacentNodes(s.DirectedGraph, node, direction)
}

// EachAdjacentNode delegates neighbor iteration while preserving the wrapper's observation hooks.
func (s *reachVisitCounter) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(uint64) bool) {
	if s.onWalk != nil {
		s.onWalk()
		s.onWalk = nil
	}
	s.visits[node]++
	s.DirectedGraph.EachAdjacentNode(node, direction, delegate)
}

// TestReachabilityMemoEvictionBoundsExpansion checks that evicting memoized closures does not multiply completed subgraph expansion.
func TestReachabilityMemoEvictionBoundsExpansion(t *testing.T) {
	for _, direction := range []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound} {
		t.Run(fmt.Sprint(direction), func(t *testing.T) {
			digraph, root := sharedLeavesDiamondGraph(10, direction)
			reach := NewReachabilityCache(context.Background(), digraph, int(float64(digraph.NumNodes())*.15))
			rootComponent, _ := reach.components.ContainingComponent(root)
			pending := map[reachFlightKey]*reachFlightEntry{}
			counter := &reachVisitCounter{
				DirectedGraph: reach.components.Digraph(),
				visits:        map[uint64]int{},
			}
			counter.onWalk = func() {
				for key, entry := range reach.inFlight.entries {
					pending[key] = entry
				}
			}
			reach.components.digraph = counter
			result := reach.componentReachDFS(rootComponent, direction)
			require.Equal(t, digraph.NumNodes(), result.Cardinality())
			require.NotEmpty(t, pending, "the graph must exercise fallback with unfinished registrations")
			for component, visits := range counter.visits {
				require.LessOrEqualf(t, visits, 2, "component %d expanded repeatedly", component)
			}
			require.Empty(t, reach.inFlight.entries)
			for key, entry := range pending {
				select {
				case <-entry.done:
				default:
					t.Fatal("fallback left a follower blocked")
				}
				if key.component == rootComponent {
					require.Equal(t, result, entry.result)
				} else {
					require.Nil(t, entry.result, "unfinished intermediate must be abandoned")
				}
			}
			// A side cursor abandoned during fallback must not have published
			// a partial closure that a later direct query can return.
			const firstSide = uint64(reachMemoCapacityFloor + 27)
			require.Equal(t, uint64(reachMemoCapacityFloor+28), reach.ReachOfComponentContainingMember(firstSide, direction).Cardinality())
			require.Equal(t, digraph.NumNodes(), reach.ReachOfComponentContainingMember(root, direction).Cardinality())
		})
	}
}

// TestReachabilityFlightAbandonment verifies the panic-safety contract of
// in-flight coordination: registrations a traversal never settles are
// abandoned with a nil result, and a waiter that wakes on an abandoned entry
// observes the nil result and can compute the closure itself.
func TestReachabilityFlightAbandonment(t *testing.T) {
	reach := &ReachabilityCache{
		components:             ComponentGraph{},
		inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](1),
		outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](1),
		inFlight:               NewReachFlight(),
	}

	key := reachFlightKey{
		component: 42,
		direction: graph.DirectionOutbound,
	}

	// A traversal registers the component but never completes it.
	entry, leader := reach.inFlight.register(key)
	require.True(t, leader)

	// A concurrent waiter blocks on the registration.
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		<-entry.done
	}()

	// A small delay ensures the waiter is parked on the channel before the
	// registration is abandoned.
	time.Sleep(10 * time.Millisecond)
	reach.abandonLeads([]*reachLead{{key: key, entry: entry}})

	select {
	case <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("waiter did not wake on abandoned registration")
	}

	require.Nil(t, entry.result)

	// The registry entry is gone, so a later traversal can register again.
	_, leaderAgain := reach.inFlight.register(key)
	require.True(t, leaderAgain)
}

// TestReachabilityFlightAbandonedRoot tests a traversal whose requested root
// is registered in flight by a leader that then abandons it: the follower
// must wake with a nil result and compute the closure itself.
func TestReachabilityFlightAbandonedRoot(t *testing.T) {
	adjacency := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}

	reach := newDiamondCache(t, adjacency, container.NewCSRDigraphBuilder())
	rootComponent, _ := reach.components.ContainingComponent(1)
	rootKey := reachFlightKey{
		component: rootComponent,
		direction: graph.DirectionOutbound,
	}

	// Pose as the leader for the root, then never settle it.
	entry, leader := reach.inFlight.register(rootKey)
	require.True(t, leader)

	resultReady := make(chan cardinality.Duplex[uint64], 1)
	go func() {
		resultReady <- reach.componentReachDFS(rootComponent, graph.DirectionOutbound)
	}()

	time.Sleep(10 * time.Millisecond)
	reach.abandonLeads([]*reachLead{{key: rootKey, entry: entry}})

	select {
	case result := <-resultReady:
		require.Len(t, result.Slice(), 5)
	case <-time.After(time.Second):
		t.Fatal("follower did not complete after the root registration was abandoned")
	}
}

// TestReachabilityFlightLeaderRecomputes tests a traversal that leads its own
// registrations and encounters a component registered in flight by another
// traversal: it must skip the wait and recompute locally rather than risk a
// cycle of waiting leaders.
func TestReachabilityFlightLeaderRecomputes(t *testing.T) {
	adjacency := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}

	reach := newDiamondCache(t, adjacency, container.NewCSRDigraphBuilder())
	tailComponent, _ := reach.components.ContainingComponent(4)
	tailKey := reachFlightKey{
		component: tailComponent,
		direction: graph.DirectionOutbound,
	}

	// A foreign, never-settled registration for the shared tail.
	entry, leader := reach.inFlight.register(tailKey)
	require.True(t, leader)

	rootComponent, _ := reach.components.ContainingComponent(1)
	require.Len(t, reach.componentReachDFS(rootComponent, graph.DirectionOutbound).Slice(), 5)

	// Release the stale registration so later tests are unaffected.
	reach.abandonLeads([]*reachLead{{key: tailKey, entry: entry}})
}

// adjacencyOnlyDigraph hides the optional AdjacentNodes method of its inner
// digraph so the adjacency fallback path is exercised.
type adjacencyOnlyDigraph struct {
	// inner supplies graph operations without exposing the adjacency-slice optimization.
	inner container.DirectedGraph
}

// NumNodes returns the wrapped graph's node count without exposing its adjacency slice interface.
func (s adjacencyOnlyDigraph) NumNodes() uint64 {
	return s.inner.NumNodes()
}

// EachNode delegates node iteration through the restricted graph interface.
func (s adjacencyOnlyDigraph) EachNode(delegate func(node uint64) bool) {
	s.inner.EachNode(delegate)
}

// EachAdjacentNode delegates neighbor iteration while preserving the wrapper's observation hooks.
func (s adjacencyOnlyDigraph) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	s.inner.EachAdjacentNode(node, direction, delegate)
}

// TestReachabilityNonCSRDigraphAdjacency exercises the adjacency fallback for
// component graphs whose digraph does not expose zero-copy adjacency. The
// component graph's digraph is replaced with a minimal wrapper that hides the
// optional AdjacentNodes method.
func TestReachabilityNonCSRDigraphAdjacency(t *testing.T) {
	adjacency := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}

	reach := newDiamondCache(t, adjacency, container.NewCSRDigraphBuilder())
	reach.components.digraph = adjacencyOnlyDigraph{
		inner: reach.components.digraph,
	}

	expected := map[uint64]int{1: 5, 2: 3, 3: 3, 4: 2, 5: 1}
	for member := uint64(1); member <= 5; member++ {
		component, _ := reach.components.ContainingComponent(member)
		require.Lenf(t, reach.componentReachDFS(component, graph.DirectionOutbound).Slice(), expected[member], "member %d", member)
	}
}

// TestReachabilityFlightFollowerWaitsAndAbandonedChild drive the two child-level
// in-flight outcomes deterministically. A traversal whose root registration is
// abandoned by its leader computes locally while leading nothing, so at a
// child registered in flight by another traversal it must wait as a pure
// follower and merge either the published result or, when that leader also
// abandons the entry, recompute locally.
func TestReachabilityFlightFollowerWaitsAndAbandonedChild(t *testing.T) {
	for _, tc := range []struct {
		// name identifies the flight settlement scenario in subtest output.
		name string
		// abandonChild selects abandonment instead of publishing the child closure.
		abandonChild bool
	}{
		{
			name: "published",
		},
		{
			name:         "abandoned",
			abandonChild: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adjacency := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}
			reach := newDiamondCache(t, adjacency, container.NewCSRDigraphBuilder())

			var (
				rootComponent, _   = reach.components.ContainingComponent(1)
				branchComponent, _ = reach.components.ContainingComponent(2)
				tailComponent, _   = reach.components.ContainingComponent(4)
				leafComponent, _   = reach.components.ContainingComponent(5)
			)

			// Foreign leaders hold both the root and the first branch.
			rootKey := reachFlightKey{
				component: rootComponent,
				direction: graph.DirectionOutbound,
			}
			rootEntry, rootLeader := reach.inFlight.register(rootKey)
			require.True(t, rootLeader)

			branchKey := reachFlightKey{
				component: branchComponent,
				direction: graph.DirectionOutbound,
			}
			branchEntry, branchLeader := reach.inFlight.register(branchKey)
			require.True(t, branchLeader)

			resultReady := make(chan cardinality.Duplex[uint64], 1)
			go func() {
				resultReady <- reach.componentReachDFS(rootComponent, graph.DirectionOutbound)
			}()

			// Let the traversal park on the root registration, then abandon
			// it: the traversal continues locally, leading nothing.
			time.Sleep(10 * time.Millisecond)
			reach.inFlight.complete(rootKey, rootEntry, nil)

			// The traversal now parks on the branch registration as a pure
			// follower before the branch leader settles it.
			time.Sleep(10 * time.Millisecond)

			if tc.abandonChild {
				reach.inFlight.complete(branchKey, branchEntry, nil)
			} else {
				reach.inFlight.complete(branchKey, branchEntry, cardinality.NewBitmap64With(branchComponent, tailComponent, leafComponent))
			}

			select {
			case result := <-resultReady:
				require.Len(t, result.Slice(), 5)
			case <-time.After(2 * time.Second):
				t.Fatal("follower traversal did not finish")
			}
		})
	}
}

// mutableDigraphBuilder adapts a MutableDirectedGraph to the DigraphBuilder
// interface expected by newDiamondCache.
type mutableDigraphBuilder struct {
	// MutableDirectedGraph supplies mutable graph construction and is returned by Build.
	container.MutableDirectedGraph
}

// Build returns the mutable graph held by the fixture builder.
func (s mutableDigraphBuilder) Build() container.DirectedGraph {
	return s
}

// newDiamondCache constructs a convergent graph whose repeated suffixes exercise memo eviction.
func newDiamondCache(t *testing.T, adjacency map[uint64][]uint64, digraph container.DigraphBuilder) *ReachabilityCache {
	t.Helper()

	for start, ends := range adjacency {
		digraph.AddNode(start)
		for _, end := range ends {
			digraph.AddEdge(start, end)
		}
	}

	return &ReachabilityCache{
		components:             NewComponentGraph(context.Background(), digraph.Build()),
		inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](16),
		outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](16),
		inFlight:               NewReachFlight(),
	}
}
