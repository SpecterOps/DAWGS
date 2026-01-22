package container

import (
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

type adjacencyMapDigraph struct {
	inbound  AdjacencyMap
	outbound AdjacencyMap
	nodes    cardinality.Duplex[uint64]
}

func NewAdjacencyMapGraph() MutableDirectedGraph {
	return &adjacencyMapDigraph{
		inbound:  AdjacencyMap{},
		outbound: AdjacencyMap{},
		nodes:    cardinality.NewBitmap64(),
	}
}

func BuildAdjacencyMapGraph(adj map[uint64][]uint64) MutableDirectedGraph {
	digraph := NewAdjacencyMapGraph()

	for src, outs := range adj {
		digraph.AddNode(src)

		for _, dst := range outs {
			digraph.AddNode(dst)
			digraph.AddEdge(src, dst)
		}
	}

	return digraph
}
func (s *adjacencyMapDigraph) AddNode(node uint64) {
	s.nodes.Add(node)
}

func (s *adjacencyMapDigraph) Normalize() ([]uint64, DirectedGraph) {
	var (
		numNodes       = s.NumNodes()
		idIndex        = make(map[uint64]uint64, numNodes)
		reverseIDIndex = make([]uint64, numNodes)
		newGraph       = &adjacencyMapDigraph{
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

func (s *adjacencyMapDigraph) NumNodes() uint64 {
	return s.nodes.Cardinality()
}

func (s *adjacencyMapDigraph) NumEdges() uint64 {
	return s.nodes.Cardinality()
}

func (s *adjacencyMapDigraph) getAdjacent(node uint64, direction graph.Direction) cardinality.Duplex[uint64] {
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

func (s *adjacencyMapDigraph) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		return adjacent.Slice()
	}

	return nil
}

func (s *adjacencyMapDigraph) Degrees(node uint64, direction graph.Direction) uint64 {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		return adjacent.Cardinality()
	}

	return 0
}

func (s *adjacencyMapDigraph) Nodes() cardinality.Duplex[uint64] {
	return s.nodes
}

func (s *adjacencyMapDigraph) EachNode(delegate func(node uint64) bool) {
	s.nodes.Each(delegate)
}

func (s *adjacencyMapDigraph) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	if adjacent := s.getAdjacent(node, direction); adjacent != nil {
		adjacent.Each(delegate)
	}
}

func (s *adjacencyMapDigraph) AddEdge(start, end uint64) {
	if edgeBitmap, exists := s.outbound[start]; exists {
		edgeBitmap.Add(end)
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(end)

		s.outbound[start] = edgeBitmap
	}

	if edgeBitmap, exists := s.inbound[end]; exists {
		edgeBitmap.Add(start)
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(start)

		s.inbound[end] = edgeBitmap
	}

	s.nodes.Add(start, end)
}
