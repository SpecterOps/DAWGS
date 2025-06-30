package algo

import (
	"context"
	"math"

	"github.com/gammazero/deque"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util"
)

func StronglyConnectedComponents(ctx context.Context, digraph container.DirectedGraph) ([]cardinality.Duplex[uint64], map[uint64]uint64) {
	defer util.SLogMeasureFunction("StronglyConnectedComponents")()

	type descentCursor struct {
		id        uint64   // current vertex
		branches  []uint64 // outbound neighbours (cached)
		branchIdx int      // next neighbour to explore
	}

	var (
		numNodes   = digraph.NumNodes()                                // num nodes in directed graph
		initialCap = int64(math.Sqrt(float64(numNodes)))               // inital capacity
		index      = 0                                                 // discovery counter
		discIdx    = make(map[uint64]int, numNodes)                    // discovery index per vertex
		lowLink    = make(map[uint64]int, numNodes)                    // low‑link per vertex
		onStack    = cardinality.NewBitmap64()                         // Tarjan stack membership
		stack      = make([]uint64, 0, initialCap)                     // classic Tarjan stack
		dfsStack   = make([]*descentCursor, 0, initialCap)             // dfs cursors
		components = make([]cardinality.Duplex[uint64], 0, initialCap) // tracked component membership
		nodeToComp = make(map[uint64]uint64, numNodes)                 // final mapping
	)

	// tarjanPush pushes a vertex onto the Tarjan stack and initialise its indices
	tarjanPush := func(v uint64) {
		discIdx[v] = index
		lowLink[v] = index
		index++

		stack = append(stack, v)
		onStack.Add(v)
	}

	// Start a DFS from every unvisited node
	digraph.EachNode(func(start uint64) bool {
		if _, seen := discIdx[start]; seen {
			// Early exit for already visited nodes
			return true
		}

		dfsStack = append(dfsStack, &descentCursor{
			id:        start,
			branches:  container.AdjacentNodes(digraph, start, graph.DirectionOutbound),
			branchIdx: 0,
		})

		tarjanPush(start)

		for len(dfsStack) > 0 {
			nextEntry := dfsStack[len(dfsStack)-1]

			// If there are still neighbours left, descend into the next one
			if nextEntry.branchIdx < len(nextEntry.branches) {
				neighbour := nextEntry.branches[nextEntry.branchIdx]
				nextEntry.branchIdx++

				if _, visited := discIdx[neighbour]; !visited {
					// Unvisited; push onto both stacks
					dfsStack = append(dfsStack, &descentCursor{
						id:        neighbour,
						branches:  container.AdjacentNodes(digraph, neighbour, graph.DirectionOutbound),
						branchIdx: 0,
					})

					tarjanPush(neighbour)
				} else if onStack.Contains(neighbour) {
					// Create a back‑edge to a vertex still on the tarjan stack
					if lowLink[nextEntry.id] > discIdx[neighbour] {
						lowLink[nextEntry.id] = discIdx[neighbour]
					}
				}

				// Continue the outer loop. Either we descended deeper or will come back
				// after processing the child node.
				continue
			}

			// All neighbours processed – pop this cursor
			dfsStack = dfsStack[:len(dfsStack)-1]

			// Propagate low‑link up to the parent (if any)
			if len(dfsStack) > 0 {
				parent := dfsStack[len(dfsStack)-1].id

				if lowLink[parent] > lowLink[nextEntry.id] {
					lowLink[parent] = lowLink[nextEntry.id]
				}
			}

			// If the next entry is a root, unwind the tarjan stack
			if lowLink[nextEntry.id] == discIdx[nextEntry.id] {
				compBitmap := cardinality.NewBitmap64()
				compIdx := uint64(len(components))

				for {
					top := stack[len(stack)-1]
					stack = stack[:len(stack)-1]

					onStack.Remove(top)
					compBitmap.Add(top)
					nodeToComp[top] = compIdx

					if top == nextEntry.id {
						break
					}
				}
				components = append(components, compBitmap)
			}
		}

		return util.IsContextLive(ctx)
	})

	return components, nodeToComp
}

type ComponentGraph struct {
	componentMembers      []cardinality.Duplex[uint64]
	memberComponentLookup map[uint64]uint64
	digraph               container.DirectedGraph
}

func (s ComponentGraph) HasMember(memberID uint64) bool {
	_, hasMember := s.memberComponentLookup[memberID]
	return hasMember
}

func (s ComponentGraph) KnownMembers() cardinality.Duplex[uint64] {
	members := cardinality.NewBitmap64()

	for memberID := range s.memberComponentLookup {
		members.Add(memberID)
	}

	return members
}

func (s ComponentGraph) Digraph() container.DirectedGraph {
	return s.digraph
}

func (s ComponentGraph) ContainingComponent(memberID uint64) (uint64, bool) {
	component, inComponentDigraph := s.memberComponentLookup[memberID]
	return component, inComponentDigraph
}

func (s ComponentGraph) ComponentMembers(componentID uint64) cardinality.Duplex[uint64] {
	return s.componentMembers[componentID]
}

func (s ComponentGraph) CollectComponentMembers(componentID uint64, members cardinality.Duplex[uint64]) {
	members.Or(s.componentMembers[componentID])
}

func (s ComponentGraph) ComponentSearch(startComponent, endComponent uint64, direction graph.Direction) bool {
	if startComponent == endComponent {
		return true
	}

	var (
		traversals deque.Deque[uint64]
		visited    = cardinality.NewBitmap64()
		reachable  = false
	)

	traversals.PushBack(startComponent)

	for remainingTraversals := traversals.Len(); !reachable && remainingTraversals > 0; remainingTraversals = traversals.Len() {
		nextComponent := traversals.PopFront()

		s.digraph.EachAdjacentNode(nextComponent, direction, func(adjacentComponent uint64) bool {
			reachable = adjacentComponent == endComponent

			if !reachable {
				if visited.CheckedAdd(adjacentComponent) {
					traversals.PushBack(adjacentComponent)
				}
			}

			return !reachable
		})
	}

	return reachable
}

func (s ComponentGraph) ComponentReachable(startComponent, endComponent uint64, direction graph.Direction) bool {
	if startComponent == endComponent {
		return true
	}

	var (
		outboundQueue      deque.Deque[uint64]
		inboundQueue       deque.Deque[uint64]
		outboundComponents = cardinality.NewBitmap64()
		inboundComponents  = cardinality.NewBitmap64()
		visitedComponents  = cardinality.NewBitmap64()
		reachable          = false
	)

	outboundQueue.PushBack(startComponent)
	outboundComponents.Add(startComponent)

	inboundQueue.PushBack(endComponent)
	inboundComponents.Add(endComponent)

	for !reachable {
		var (
			outboundQueueLen = outboundQueue.Len()
			inboundQueueLen  = inboundQueue.Len()
		)

		if outboundQueueLen > 0 && outboundQueueLen <= inboundQueueLen {
			nextComponent := outboundQueue.PopFront()

			if !visitedComponents.CheckedAdd(nextComponent) {
				continue
			}

			s.digraph.EachAdjacentNode(nextComponent, direction, func(adjacentComponent uint64) bool {
				if outboundComponents.CheckedAdd(adjacentComponent) {
					// Haven't seen this component yet, append to the traversal queue and check for reachability
					outboundQueue.PushBack(adjacentComponent)
					reachable = inboundComponents.Contains(adjacentComponent)
				}

				// Continue iterating if not reachable
				return !reachable
			})
		} else if inboundQueueLen > 0 {
			nextComponent := inboundQueue.PopFront()

			s.digraph.EachAdjacentNode(nextComponent, direction.Reverse(), func(adjacentComponent uint64) bool {
				if inboundComponents.CheckedAdd(adjacentComponent) {
					// Haven't seen this component yet, append to the traversal queue and check for reachability
					inboundQueue.PushBack(adjacentComponent)
					reachable = outboundComponents.Contains(adjacentComponent)
				}

				// Continue iterating if not reachable
				return !reachable
			})
		} else {
			// No more expansions remain
			break
		}
	}

	return reachable
}

func (s ComponentGraph) ComponentHistogram(originNodes []uint64) map[uint64]uint64 {
	histogram := map[uint64]uint64{}

	for _, originNode := range originNodes {
		if component, inComponent := s.ContainingComponent(originNode); inComponent {
			histogram[component] += 1
		}
	}

	return histogram
}

func (s ComponentGraph) OriginReachable(startID, endID uint64) bool {
	var (
		startComponent, startInComponent = s.ContainingComponent(startID)
		endComponent, endInComponent     = s.ContainingComponent(endID)
	)

	if !startInComponent || !endInComponent {
		return false
	}

	return s.ComponentReachable(startComponent, endComponent, graph.DirectionBoth)
}

func NewComponentGraph(ctx context.Context, originGraph container.DirectedGraph) ComponentGraph {
	var (
		componentMembers, memberComponentLookup = StronglyConnectedComponents(ctx, originGraph)
		componentDigraphBuilder                 = container.NewCSRDigraphBuilder()
	)

	defer util.SLogMeasureFunction("NewComponentGraph")()

	// Ensure all components are present as vertices, even if they have no edges
	for componentID := range componentMembers {
		componentDigraphBuilder.AddNode(uint64(componentID))
	}

	originGraph.EachNode(func(node uint64) bool {
		nodeComponent := memberComponentLookup[node]

		originGraph.EachAdjacentNode(node, graph.DirectionInbound, func(adjacent uint64) bool {
			if adjacentComponent := memberComponentLookup[adjacent]; nodeComponent != adjacentComponent {
				componentDigraphBuilder.AddEdge(adjacentComponent, nodeComponent)
			}

			return util.IsContextLive(ctx)
		})

		originGraph.EachAdjacentNode(node, graph.DirectionOutbound, func(adjacent uint64) bool {
			if adjacentComponent := memberComponentLookup[adjacent]; nodeComponent != adjacentComponent {
				componentDigraphBuilder.AddEdge(nodeComponent, adjacentComponent)
			}

			return util.IsContextLive(ctx)
		})

		return util.IsContextLive(ctx)
	})

	return ComponentGraph{
		componentMembers:      componentMembers,
		memberComponentLookup: memberComponentLookup,
		digraph:               componentDigraphBuilder.Build(),
	}
}
