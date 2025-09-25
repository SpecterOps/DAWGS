package algo

import (
	"math"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

func StronglyConnectedComponents(digraph container.DirectedGraph, direction graph.Direction) ([]cardinality.Duplex[uint64], map[uint64]int) {
	type descentCursor struct {
		id        uint64
		branches  []uint64
		branchIdx int
	}

	var (
		numNodes                    = digraph.NumNodes()
		initialAlloc                = int64(math.Sqrt(float64(numNodes)))
		lastSearchedNodeID          = uint64(0)
		index                       = 0
		visitedIndex                = make(map[uint64]int, numNodes)
		lowLinks                    = make(map[uint64]int, numNodes)
		onStack                     = cardinality.NewBitmap64()
		stack                       = make([]uint64, 0, initialAlloc)
		dfsDescentStack             = make([]*descentCursor, 0, initialAlloc)
		stronglyConnectedComponents = make([]cardinality.Duplex[uint64], 0, initialAlloc)
		nodeToSCCIndex              = make(map[uint64]int, numNodes)
	)

	digraph.EachNode(func(node uint64) bool {
		if _, visited := visitedIndex[node]; visited {
			return true
		}

		dfsDescentStack = append(dfsDescentStack, &descentCursor{
			id:        node,
			branches:  digraph.Adjacent(node, direction),
			branchIdx: 0,
		})

		for len(dfsDescentStack) > 0 {
			nextCursor := dfsDescentStack[len(dfsDescentStack)-1]

			if nextCursor.branchIdx == 0 {
				// First visit of this node
				visitedIndex[nextCursor.id] = index
				lowLinks[nextCursor.id] = index
				index += 1

				stack = append(stack, nextCursor.id)
				onStack.Add(nextCursor.id)
			} else if lastSearchedNodeID != nextCursor.id {
				// Revisiting this node from a descending DFS
				lowLinks[nextCursor.id] = min(lowLinks[nextCursor.id], lowLinks[lastSearchedNodeID])
			}

			// Set to the current cursor ID for ascent
			lastSearchedNodeID = nextCursor.id

			if nextCursor.branchIdx < len(nextCursor.branches) {
				// Advance to the next branch
				nextBranchID := nextCursor.branches[nextCursor.branchIdx]
				nextCursor.branchIdx += 1

				if _, visited := visitedIndex[nextBranchID]; !visited {
					// This node has not been visited yet, run a DFS for it
					lastSearchedNodeID = nextBranchID

					dfsDescentStack = append(dfsDescentStack, &descentCursor{
						id:        nextBranchID,
						branches:  digraph.Adjacent(nextBranchID, direction),
						branchIdx: 0,
					})
				} else if onStack.Contains(nextBranchID) {
					// Branch is on the traversal stack; hence it is also in the current SCC
					lowLinks[nextCursor.id] = min(lowLinks[nextCursor.id], visitedIndex[nextBranchID])
				}
			} else {
				// Finished visiting branches; exiting node
				dfsDescentStack = dfsDescentStack[:len(dfsDescentStack)-1]

				if lowLinks[nextCursor.id] == visitedIndex[nextCursor.id] {
					var (
						scc   = cardinality.NewBitmap64()
						sccID = len(stronglyConnectedComponents)
					)

					for {
						// Unwind the stack to the root of the component
						currentNode := stack[len(stack)-1]
						stack = stack[:len(stack)-1]

						onStack.Remove(currentNode)

						scc.Add(currentNode)

						// Reverse index origin node to SCC
						nodeToSCCIndex[currentNode] = sccID

						if currentNode == nextCursor.id {
							break
						}
					}

					stronglyConnectedComponents = append(stronglyConnectedComponents, scc)
				}
			}
		}

		return true
	})

	return stronglyConnectedComponents, nodeToSCCIndex
}

type ComponentDirectedGraph struct {
	Components                 []cardinality.Duplex[uint64]
	OriginNodeToComponentIndex map[uint64]int
	Digraph                    container.DirectedGraph
}

func NewComponentDirectedGraph(digraph container.DirectedGraph, direction graph.Direction) ComponentDirectedGraph {
	var (
		components, nodeToComponentIndex = StronglyConnectedComponents(digraph, direction)
		componentDigraph                 = container.NewDirectedGraph()
	)

	// Ensure all components are present as vertices, even if they have no edges
	for idx := range components {
		componentDigraph.Nodes().CheckedAdd(uint64(idx))
	}

	digraph.EachNode(func(node uint64) bool {
		nodeComponent := graph.ID(nodeToComponentIndex[node])

		digraph.EachAdjacent(node, direction, func(adjacent uint64) bool {
			if adjacentComponent := graph.ID(nodeToComponentIndex[adjacent]); nodeComponent != adjacentComponent {
				switch direction {
				case graph.DirectionInbound:
					componentDigraph.AddEdge(adjacentComponent, nodeComponent)
				case graph.DirectionOutbound:
					componentDigraph.AddEdge(nodeComponent, adjacentComponent)
				}
			}

			return true
		})

		return true
	})

	return ComponentDirectedGraph{
		Components:                 components,
		OriginNodeToComponentIndex: nodeToComponentIndex,
		Digraph:                    componentDigraph,
	}
}
