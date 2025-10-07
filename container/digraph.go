package container

import (
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

type Weight = float64
type AdjacencyMap map[uint64]cardinality.Duplex[uint64]

type DirectedGraph interface {
	AddEdge(start, end graph.ID)
	NumNodes() uint64
	Nodes() cardinality.Duplex[uint64]
	Degrees(node uint64, direction graph.Direction) uint64
	Adjacent(node uint64, direction graph.Direction) []uint64
	EachNode(delegate func(node uint64) bool)
	EachAdjacent(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool)
	BFSTree(nodeID uint64, direction graph.Direction) []ShortestPathTerminal
	Normalize() ([]uint64, DirectedGraph)
	Dimensions(direction graph.Direction) (uint64, uint64)
}

type directedGraph struct {
	inbound  AdjacencyMap
	outbound AdjacencyMap
	nodes    cardinality.Duplex[uint64]
}

func NewDirectedGraph() DirectedGraph {
	return &directedGraph{
		inbound:  AdjacencyMap{},
		outbound: AdjacencyMap{},
		nodes:    cardinality.NewBitmap64(),
	}
}

type ShortestPathTerminal struct {
	NodeID   uint64
	Distance Weight
}

func (s *directedGraph) Dimensions(direction graph.Direction) (uint64, uint64) {
	var largestRow uint64 = 0

	s.EachNode(func(node uint64) bool {
		if adjacent := s.getAdjacent(node, direction); adjacent != nil {
			count := adjacent.Cardinality()

			if count > largestRow {
				largestRow = count
			}
		}

		return true
	})

	return s.Nodes().Cardinality(), largestRow
}

func (s *directedGraph) BFSTree(nodeID uint64, direction graph.Direction) []ShortestPathTerminal {
	var (
		visited   = cardinality.NewBitmap64()
		stack     []ShortestPathTerminal
		terminals []ShortestPathTerminal
	)

	stack = append(stack, ShortestPathTerminal{
		NodeID:   nodeID,
		Distance: 0,
	})

	for len(stack) > 0 {
		nextCursor := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		s.EachAdjacent(nextCursor.NodeID, direction, func(adjacentNodeID uint64) bool {
			if visited.CheckedAdd(adjacentNodeID) {
				terminalCursor := ShortestPathTerminal{
					NodeID:   adjacentNodeID,
					Distance: nextCursor.Distance + 1,
				}

				// If not visited, descend into this node next
				stack = append(stack, terminalCursor)

				// This reachable node represents one of the shortest path terminals
				terminals = append(terminals, terminalCursor)
			}

			return true
		})
	}

	return terminals
}

func (s *directedGraph) Normalize() ([]uint64, DirectedGraph) {
	var (
		numNodes       = s.NumNodes()
		idIndex        = make(map[uint64]uint64, numNodes)
		reverseIDIndex = make([]uint64, numNodes)
		newGraph       = &directedGraph{
			inbound:  AdjacencyMap{},
			outbound: AdjacencyMap{},
			nodes:    cardinality.NewBitmap64(),
		}
	)

	s.EachNode(func(node uint64) bool {
		normalID := uint64(len(idIndex))

		newGraph.nodes.Add(normalID)
		idIndex[node] = normalID
		reverseIDIndex[normalID] = node

		return true
	})

	for originID, originAdjacentBitmap := range s.inbound {
		var (
			normalID       = idIndex[originID]
			normalAdjacent = cardinality.NewBitmap64()
		)

		originAdjacentBitmap.Each(func(originAdjacent uint64) bool {
			normalAdjacent.Add(idIndex[originAdjacent])
			return true
		})

		newGraph.inbound[normalID] = normalAdjacent
	}

	for originID, originAdjacentBitmap := range s.outbound {
		var (
			normalID       = idIndex[originID]
			normalAdjacent = cardinality.NewBitmap64()
		)

		originAdjacentBitmap.Each(func(originAdjacent uint64) bool {
			normalAdjacent.Add(idIndex[originAdjacent])
			return true
		})

		newGraph.outbound[normalID] = normalAdjacent
	}

	return reverseIDIndex, newGraph
}

func (s *directedGraph) NumNodes() uint64 {
	return s.nodes.Cardinality()
}

func (s *directedGraph) NumEdges() uint64 {
	return s.nodes.Cardinality()
}

func (s *directedGraph) getAdjacent(node uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	switch direction {
	case graph.DirectionOutbound:
		if adjacent, exists := s.outbound[node]; exists {
			return adjacent
		}

	case graph.DirectionInbound:
		if adjacent, exists := s.inbound[node]; exists {
			return adjacent
		}

	case graph.DirectionBoth:
		var (
			outboundAdjacent, hasOutbound = s.outbound[node]
			inboundAdjacent, hasInbound   = s.inbound[node]
		)

		if hasOutbound {
			if hasInbound {
				combinedAdjacent := outboundAdjacent.Clone()
				combinedAdjacent.Or(inboundAdjacent)

				return combinedAdjacent
			}

			return outboundAdjacent
		} else if hasInbound {
			return inboundAdjacent
		}
	}

	return nil
}

func (s *directedGraph) Adjacent(node uint64, direction graph.Direction) []uint64 {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		return adjacent.Slice()
	}

	return nil
}

func (s *directedGraph) Degrees(node uint64, direction graph.Direction) uint64 {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		return adjacent.Cardinality()
	}

	return 0
}

func (s *directedGraph) Nodes() cardinality.Duplex[uint64] {
	return s.nodes
}

func (s *directedGraph) EachNode(delegate func(node uint64) bool) {
	s.nodes.Each(delegate)
}

func (s *directedGraph) EachAdjacent(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		adjacent.Each(delegate)
	}
}

func (s *directedGraph) AddEdge(start, end graph.ID) {
	var (
		startUint64 = start.Uint64()
		endUint64   = end.Uint64()
	)

	if edgeBitmap, exists := s.outbound[startUint64]; exists {
		edgeBitmap.Add(endUint64)
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(endUint64)

		s.outbound[startUint64] = edgeBitmap
	}

	if edgeBitmap, exists := s.inbound[endUint64]; exists {
		edgeBitmap.Add(startUint64)
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(startUint64)

		s.inbound[endUint64] = edgeBitmap
	}

	s.nodes.Add(startUint64, endUint64)
}
