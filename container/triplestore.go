package container

import (
	"slices"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

type Edge struct {
	ID    uint64     `json:"id"`
	Kind  graph.Kind `json:"kind"`
	Start uint64     `json:"start_id"`
	End   uint64     `json:"end_id"`
}

type Path struct {
	Edges []Edge `json:"edges"`
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
	AddNode(node uint64)
	AddEdge(edge Edge)
}

func AdjacentEdges(ts Triplestore, node uint64, direction graph.Direction) []Edge {
	var edges []Edge

	ts.EachAdjacentEdge(node, direction, func(next Edge) bool {
		edges = append(edges, next)
		return true
	})

	return edges
}

type triplestore struct {
	edges      []Edge
	startIndex map[uint64][]uint64
	endIndex   map[uint64][]uint64
}

func NewTriplestore() MutableTriplestore {
	return &triplestore{
		startIndex: map[uint64][]uint64{},
		endIndex:   map[uint64][]uint64{},
	}
}

func (s *triplestore) Sort() {
	// Clear but preserve allocations
	clear(s.startIndex)
	clear(s.endIndex)

	// Sort edges by ID
	slices.SortFunc(s.edges, func(a, b Edge) int {
		if a.ID > b.ID {
			return 1
		}

		if a.ID < b.ID {
			return -1
		}

		return 0
	})

	// Rebuild node to edge index lookups
	for edgeIdx, edge := range s.edges {
		s.startIndex[edge.Start] = append(s.startIndex[edge.Start], uint64(edgeIdx))
		s.endIndex[edge.End] = append(s.endIndex[edge.End], uint64(edgeIdx))
	}
}

func (s *triplestore) Edges() []Edge {
	return s.edges
}

func (s *triplestore) ContainsNode(node uint64) bool {
	_, hasNode := s.startIndex[node]

	if hasNode {
		return true
	}

	_, hasNode = s.endIndex[node]
	return hasNode
}

func (s *triplestore) nodeBitmap() cardinality.Duplex[uint64] {
	nodes := cardinality.NewBitmap64()

	for nodeID := range s.startIndex {
		nodes.Add(nodeID)
	}

	for nodeID := range s.endIndex {
		nodes.Add(nodeID)
	}

	return nodes
}

func (s *triplestore) NumNodes() uint64 {
	return s.nodeBitmap().Cardinality()
}

func (s *triplestore) AddNode(node uint64) {
	if _, exists := s.startIndex[node]; !exists {
		s.startIndex[node] = nil
	}

	if _, exists := s.endIndex[node]; !exists {
		s.endIndex[node] = nil
	}
}

func (s *triplestore) EachNode(delegate func(node uint64) bool) {
	nodes := cardinality.NewBitmap64()

	for nodeID := range s.startIndex {
		nodes.Add(nodeID)

		if !delegate(nodeID) {
			return
		}
	}

	for nodeID := range s.endIndex {
		if nodes.CheckedAdd(nodeID) && !delegate(nodeID) {
			return
		}
	}
}

func (s *triplestore) EachEdge(delegate func(edge Edge) bool) {
	for _, nextEdge := range s.edges {
		if !delegate(nextEdge) {
			break
		}
	}
}

func (s *triplestore) AddEdge(edge Edge) {
	s.edges = append(s.edges, edge)
	edgeIdx := len(s.edges) - 1

	s.startIndex[edge.Start] = append(s.startIndex[edge.Start], uint64(edgeIdx))
	s.endIndex[edge.End] = append(s.endIndex[edge.End], uint64(edgeIdx))
}

func (s *triplestore) adjacentEdgeIndices(node uint64, direction graph.Direction) []uint64 {
	switch direction {
	case graph.DirectionOutbound:
		if outboundEdges, hasOutbound := s.startIndex[node]; hasOutbound {
			return outboundEdges
		}

	case graph.DirectionInbound:
		if inboundEdges, hasInbound := s.endIndex[node]; hasInbound {
			return inboundEdges
		}

	default:
		edgeIndices := cardinality.NewBitmap64()

		if outboundEdges, hasOutbound := s.startIndex[node]; hasOutbound {
			edgeIndices.Add(outboundEdges...)
		}

		if inboundEdges, hasInbound := s.endIndex[node]; hasInbound {
			edgeIndices.Add(inboundEdges...)
		}

		return edgeIndices.Slice()
	}

	return nil
}

func (s *triplestore) AdjacentEdges(node uint64, direction graph.Direction) []uint64 {
	var (
		edgeIndices = s.adjacentEdgeIndices(node, direction)
		edgeIDs     = make([]uint64, len(edgeIndices))
	)

	for idx, edgeIdx := range edgeIndices {
		edgeIDs[idx] = s.edges[edgeIdx].ID
	}

	return edgeIDs
}

func (s *triplestore) adjacent(node uint64, direction graph.Direction) []uint64 {
	nodes := cardinality.NewBitmap64()

	for _, edgeIndex := range s.adjacentEdgeIndices(node, direction) {
		edge := s.edges[edgeIndex]

		switch direction {
		case graph.DirectionOutbound:
			nodes.Add(edge.End)

		case graph.DirectionInbound:
			nodes.Add(edge.Start)

		default:
			if node == edge.Start {
				nodes.Add(edge.End)
			} else {
				nodes.Add(edge.Start)
			}
		}
	}

	return nodes.Slice()
}

func (s *triplestore) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	return s.adjacent(node, direction)
}

func (s *triplestore) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	for _, node := range s.adjacent(node, direction) {
		if !delegate(node) {
			break
		}
	}
}

func (s *triplestore) Degrees(node uint64, direction graph.Direction) uint64 {
	if adjacent := s.adjacent(node, direction); adjacent != nil {
		return uint64(len(adjacent))
	}

	return 0
}

func (s *triplestore) NumEdges() uint64 {
	return uint64(len(s.edges))
}

func (s *triplestore) EachAdjacentEdge(node uint64, direction graph.Direction, delegate func(next Edge) bool) {
	for _, edgeIndex := range s.adjacentEdgeIndices(node, direction) {
		if !delegate(s.edges[edgeIndex]) {
			break
		}
	}
}
