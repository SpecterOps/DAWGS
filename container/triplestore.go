package container

import (
	"slices"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

type Edge struct {
	ID    uint64
	Start uint64
	End   uint64
}

func (s Edge) Pick(direction graph.Direction) uint64 {
	if direction == graph.DirectionOutbound {
		return s.End
	}

	return s.Start
}

type Triplestore interface {
	DirectedGraph

	NumEdges() uint64
	EachEdge(delegate func(next Edge) bool)
	EachAdjacentEdge(node uint64, direction graph.Direction, delegate func(next Edge) bool)
}

type MutableTriplestore interface {
	Triplestore

	Sort()
	AddTriple(edge, start, end uint64)
	Projection(deletedNodes, deletedEdges cardinality.Duplex[uint64]) MutableTriplestore
}

type triplestore struct {
	nodes        cardinality.Duplex[uint64]
	edges        []Edge
	deletedEdges cardinality.Duplex[uint64]
	startIndex   map[uint64]cardinality.Duplex[uint64]
	endIndex     map[uint64]cardinality.Duplex[uint64]
}

func NewTriplestore() MutableTriplestore {
	return &triplestore{
		nodes:        cardinality.NewBitmap64(),
		deletedEdges: cardinality.NewBitmap64(),
		startIndex:   map[uint64]cardinality.Duplex[uint64]{},
		endIndex:     map[uint64]cardinality.Duplex[uint64]{},
	}
}

func (s *triplestore) Sort() {
	slices.SortFunc(s.edges, func(a, b Edge) int {
		return int(a.ID) - int(b.ID)
	})
}

func (s *triplestore) DeleteEdge(id uint64) {
	s.deletedEdges.Add(id)
}

func (s *triplestore) Edges() []Edge {
	return s.edges
}

func (s *triplestore) NumNodes() uint64 {
	return s.nodes.Cardinality()
}

func (s *triplestore) AddNode(node uint64) {
	s.nodes.Add(node)
}

func (s *triplestore) EachNode(delegate func(node uint64) bool) {
	s.nodes.Each(delegate)
}

func (s *triplestore) EachEdge(delegate func(edge Edge) bool) {
	for _, nextEdge := range s.edges {
		if !delegate(nextEdge) {
			break
		}
	}
}

func (s *triplestore) AddTriple(edge, start, end uint64) {
	s.edges = append(s.edges, Edge{
		ID:    edge,
		Start: start,
		End:   end,
	})

	edgeIdx := len(s.edges) - 1

	if edgeBitmap, exists := s.startIndex[start]; exists {
		edgeBitmap.Add(uint64(edgeIdx))
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(uint64(edgeIdx))

		s.startIndex[start] = edgeBitmap
	}

	if edgeBitmap, exists := s.endIndex[end]; exists {
		edgeBitmap.Add(uint64(edgeIdx))
	} else {
		edgeBitmap = cardinality.NewBitmap64()
		edgeBitmap.Add(uint64(edgeIdx))

		s.endIndex[end] = edgeBitmap
	}

	s.nodes.Add(start, end)
}

func (s *triplestore) adjacentEdgeIndices(node uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	edgeIndices := cardinality.NewBitmap64()

	switch direction {
	case graph.DirectionOutbound:
		if outboundEdges, hasOutbound := s.startIndex[node]; hasOutbound {
			edgeIndices.Or(outboundEdges)
		}

	case graph.DirectionInbound:
		if inboundEdges, hasInbound := s.endIndex[node]; hasInbound {
			edgeIndices.Or(inboundEdges)
		}

	default:
		if outboundEdges, hasOutbound := s.startIndex[node]; hasOutbound {
			edgeIndices.Or(outboundEdges)
		}

		if inboundEdges, hasInbound := s.endIndex[node]; hasInbound {
			edgeIndices.Or(inboundEdges)
		}
	}

	return edgeIndices
}

func (s *triplestore) AdjacentEdges(node uint64, direction graph.Direction) []uint64 {
	var (
		edgeIndices = s.adjacentEdgeIndices(node, direction)
		edgeIDs     = make([]uint64, 0, edgeIndices.Cardinality())
	)

	edgeIndices.Each(func(value uint64) bool {
		edgeIDs = append(edgeIDs, s.edges[value].ID)
		return true
	})

	return edgeIDs
}

func (s *triplestore) adjacent(node uint64, direction graph.Direction) cardinality.Duplex[uint64] {
	nodes := cardinality.NewBitmap64()

	s.adjacentEdgeIndices(node, direction).Each(func(edgeIndex uint64) bool {
		if edge := s.edges[edgeIndex]; !s.deletedEdges.Contains(edge.ID) {
			switch direction {
			case graph.DirectionOutbound:
				nodes.Add(edge.End)

			case graph.DirectionInbound:
				nodes.Add(edge.Start)

			default:
				nodes.Add(edge.End)
				nodes.Add(edge.Start)
			}
		}

		return true
	})

	return nodes
}

func (s *triplestore) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	return s.adjacent(node, direction).Slice()
}

func (s *triplestore) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	s.adjacent(node, direction).Each(delegate)
}

func (s *triplestore) Degrees(node uint64, direction graph.Direction) uint64 {
	if adjacent := s.adjacent(node, direction); adjacent != nil {
		return adjacent.Cardinality()
	}

	return 0
}

func (s *triplestore) NumEdges() uint64 {
	return uint64(len(s.edges))
}

func (s *triplestore) EachAdjacentEdge(node uint64, direction graph.Direction, delegate func(next Edge) bool) {
	s.adjacentEdgeIndices(node, direction).Each(func(edgeIndex uint64) bool {
		return delegate(s.edges[edgeIndex])
	})
}

func (s *triplestore) Projection(deletedNodes, deletedEdges cardinality.Duplex[uint64]) MutableTriplestore {
	return &triplestoreProjection{
		origin:       s,
		deletedNodes: deletedNodes,
		deletedEdges: deletedEdges,
	}
}

type triplestoreProjection struct {
	origin       *triplestore
	deletedNodes cardinality.Duplex[uint64]
	deletedEdges cardinality.Duplex[uint64]
}

func (s *triplestoreProjection) AddTriple(edge, start, end uint64) {
	panic("unsupported")
}

func (s *triplestoreProjection) Sort() {
	panic("unsupported")
}

func (s *triplestoreProjection) Projection(deletedNodes, deletedEdges cardinality.Duplex[uint64]) MutableTriplestore {
	var (
		allDeletedNodes = s.deletedNodes.Clone()
		allDeletedEdges = s.deletedEdges.Clone()
	)

	allDeletedNodes.Or(deletedNodes)
	allDeletedEdges.Or(deletedEdges)

	return &triplestoreProjection{
		origin:       s.origin,
		deletedNodes: allDeletedNodes,
		deletedEdges: allDeletedEdges,
	}
}

func (s *triplestoreProjection) NumNodes() uint64 {
	count := uint64(0)

	s.origin.EachNode(func(value uint64) bool {
		if !s.deletedNodes.Contains(value) {
			count += 1
		}

		return true
	})

	return count
}

func (s *triplestoreProjection) NumEdges() uint64 {
	count := uint64(0)

	s.origin.EachEdge(func(next Edge) bool {
		if !s.deletedEdges.Contains(next.ID) {
			count += 1
		}

		return true
	})

	return count
}

func (s *triplestoreProjection) EachNode(delegate func(node uint64) bool) {
	s.origin.EachNode(func(node uint64) bool {
		if !s.deletedNodes.Contains(node) {
			return delegate(node)
		}

		return true
	})
}

func (s *triplestoreProjection) EachEdge(delegate func(next Edge) bool) {
	s.origin.EachEdge(func(next Edge) bool {
		if !s.deletedEdges.Contains(next.ID) && !s.deletedNodes.Contains(next.Start) && !s.deletedNodes.Contains(next.Start) {
			return delegate(next)
		}

		return true
	})
}

func (s *triplestoreProjection) EachAdjacentEdge(node uint64, direction graph.Direction, delegate func(next Edge) bool) {
	s.origin.EachAdjacentEdge(node, direction, func(next Edge) bool {
		if !s.deletedEdges.Contains(next.ID) && !s.deletedNodes.Contains(next.Start) && !s.deletedNodes.Contains(next.Start) {
			return delegate(next)
		}

		return true
	})
}

func (s *triplestoreProjection) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	s.EachAdjacentEdge(node, direction, func(next Edge) bool {
		return delegate(next.Pick(direction))
	})
}
