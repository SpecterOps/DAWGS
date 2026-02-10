package container

import (
	"github.com/gammazero/deque"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

type Weight = float64
type AdjacencyMap map[uint64]cardinality.Duplex[uint64]

type KindMap map[graph.Kind]cardinality.Duplex[uint64]

func (s KindMap) Add(kind graph.Kind, member uint64) {
	if members, hasMembers := s[kind]; hasMembers {
		members.Add(member)
	} else {
		s[kind] = cardinality.NewBitmap64With(member)
	}
}

func (s KindMap) FindFirst(id uint64) graph.Kind {
	for kind, membership := range s {
		if membership.Contains(id) {
			return kind
		}
	}

	return nil
}

func (s KindMap) FindAll(id uint64) graph.Kinds {
	var matchedKinds graph.Kinds

	for kind, membership := range s {
		if membership.Contains(id) {
			matchedKinds = matchedKinds.Add(kind)
		}
	}

	return matchedKinds
}

type DirectedGraph interface {
	NumNodes() uint64
	EachNode(delegate func(node uint64) bool)
	EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool)
}

type DigraphBuilder interface {
	AddNode(node uint64)
	AddEdge(start, end uint64)
	Build() DirectedGraph
}

type MutableDirectedGraph interface {
	DirectedGraph

	AddNode(node uint64)
	AddEdge(start, end uint64)
}

type KindDatabase struct {
	EdgeKindMap KindMap
	NodeKindMap KindMap
}

func (s KindDatabase) NodeKind(nodeID uint64) graph.Kinds {
	return s.NodeKindMap.FindAll(nodeID)
}

func (s KindDatabase) EdgeKind(edgeID uint64) graph.Kind {
	return s.EdgeKindMap.FindFirst(edgeID)
}

type PathTerminal struct {
	Node     uint64
	Weight   Weight
	Distance int
}

func Degrees(digraph DirectedGraph, node uint64, direction graph.Direction) uint64 {
	degrees := uint64(0)

	digraph.EachAdjacentNode(node, direction, func(adjacent uint64) bool {
		degrees += 1
		return true
	})

	return degrees
}

func AdjacentNodes(digraph DirectedGraph, node uint64, direction graph.Direction) []uint64 {
	var nodes []uint64

	digraph.EachAdjacentNode(node, direction, func(adjacent uint64) bool {
		nodes = append(nodes, adjacent)
		return true
	})

	return nodes
}

func Dimensions(digraph DirectedGraph, direction graph.Direction) (uint64, uint64) {
	var largestRow uint64 = 0

	digraph.EachNode(func(node uint64) bool {
		if degrees := Degrees(digraph, node, direction); degrees > largestRow {
			largestRow = degrees
		}

		return true
	})

	return digraph.NumNodes(), largestRow
}

func Reach(digraph DirectedGraph, nodeID uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	var (
		visited = cardinality.NewBitmap64()
		queue   deque.Deque[uint64]
	)

	queue.PushBack(nodeID)

	for queue.Len() > 0 {
		nextNode := queue.PopFront()

		digraph.EachAdjacentNode(nextNode, direction, func(adjacentNodeID uint64) bool {
			if visited.CheckedAdd(adjacentNodeID) {
				// If not visited, descend into this node
				queue.PushBack(adjacentNodeID)
			}

			return true
		})
	}

	return visited
}

func BFSTree(digraph DirectedGraph, nodeID uint64, direction graph.Direction) []PathTerminal {
	var (
		visited   = cardinality.NewBitmap64()
		queue     deque.Deque[PathTerminal]
		terminals []PathTerminal
	)

	queue.PushBack(PathTerminal{
		Node:     nodeID,
		Distance: 0,
	})

	for queue.Len() > 0 {
		nextCursor := queue.PopFront()

		digraph.EachAdjacentNode(nextCursor.Node, direction, func(adjacentNodeID uint64) bool {
			if visited.CheckedAdd(adjacentNodeID) {
				terminalCursor := PathTerminal{
					Node:     adjacentNodeID,
					Distance: nextCursor.Distance + 1,
				}

				// If not visited, descend into this node next
				queue.PushBack(terminalCursor)

				// This reachable node represents one of the shortest path terminals
				terminals = append(terminals, terminalCursor)
			}

			return true
		})
	}

	return terminals
}
