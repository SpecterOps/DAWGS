package container

import (
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

// csrDigraph implements a mutable directed graph using compressed sparse row storage.
type csrDigraph struct {
	// Mapping between external IDs and dense CSR indices
	idToDenseIdx map[uint64]uint64 // external ID → dense index
	denseIdxToID []uint64          // dense index → external ID

	// CSR storage for outgoing edges
	outOffsets []uint64 // length = NumNodes()+1
	outAdj     []uint64 // concatenated outgoing neighbour IDs (external IDs)

	// CSR storage for incoming edges
	inOffsets []uint64
	inAdj     []uint64
}

// idx returns the dense CSR index for an external node ID. Returns (idx, true) if the node exists, otherwise (0, false).
func (s *csrDigraph) idx(node uint64) (uint64, bool) {
	idx, exists := s.idToDenseIdx[node]
	return idx, exists
}

// csrRange returns the slice of neighbours for a given vertex index and direction. The returned slice contains external IDs.
func (s *csrDigraph) csrRange(idx uint64, dir graph.Direction) []uint64 {
	switch dir {
	case graph.DirectionOutbound:
		var (
			start = s.outOffsets[idx]
			end   = s.outOffsets[idx+1]
		)

		return s.outAdj[start:end]

	case graph.DirectionInbound:
		var (
			start = s.inOffsets[idx]
			end   = s.inOffsets[idx+1]
		)

		return s.inAdj[start:end]

	default:
		var (
			outStart, outEnd = s.outOffsets[idx], s.outOffsets[idx+1]
			inStart, inEnd   = s.inOffsets[idx], s.inOffsets[idx+1]
		)

		merged := make([]uint64, 0, (outEnd-outStart)+(inEnd-inStart))
		merged = append(merged, s.outAdj[outStart:outEnd]...)
		merged = append(merged, s.inAdj[inStart:inEnd]...)

		return merged
	}
}

func (s *csrDigraph) NumNodes() uint64 {
	return uint64(len(s.denseIdxToID))
}

func (s *csrDigraph) NumEdges() uint64 {
	// each directed edge appears once in outAdj
	return uint64(len(s.outAdj))
}

func (s *csrDigraph) ContainsNode(node uint64) bool {
	for _, storedNode := range s.denseIdxToID {
		if node == storedNode {
			return true
		}
	}

	return false
}

func (s *csrDigraph) EachNode(delegate func(node uint64) bool) {
	for _, id := range s.denseIdxToID {
		if !delegate(id) {
			return
		}
	}
}

func (s *csrDigraph) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	if idx, ok := s.idx(node); ok {
		return s.csrRange(idx, direction)
	}

	return nil
}

func (s *csrDigraph) Degrees(node uint64, direction graph.Direction) uint64 {
	if idx, ok := s.idx(node); ok {
		switch direction {
		case graph.DirectionOutbound:
			return s.outOffsets[idx+1] - s.outOffsets[idx]
		case graph.DirectionInbound:
			return s.inOffsets[idx+1] - s.inOffsets[idx]
		case graph.DirectionBoth:
			return (s.outOffsets[idx+1] - s.outOffsets[idx]) + (s.inOffsets[idx+1] - s.inOffsets[idx])
		}
	}

	return 0
}

func (s *csrDigraph) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	if idx, exists := s.idx(node); exists {
		switch direction {
		case graph.DirectionOutbound:
			for next, end := s.outOffsets[idx], s.outOffsets[idx+1]; next < end; next++ {
				if !delegate(s.outAdj[next]) {
					return
				}
			}

		case graph.DirectionInbound:
			for next, end := s.inOffsets[idx], s.inOffsets[idx+1]; next < end; next++ {
				if !delegate(s.inAdj[next]) {
					return
				}
			}

		default:
			for next, end := s.outOffsets[idx], s.outOffsets[idx+1]; next < end; next++ {
				if !delegate(s.outAdj[next]) {
					return
				}
			}

			for next, end := s.inOffsets[idx], s.inOffsets[idx+1]; next < end; next++ {
				if !delegate(s.inAdj[next]) {
					return
				}
			}
		}
	}
}

func (s *csrDigraph) Normalize() ([]uint64, DirectedGraph) {
	// Reverse map: denseIdx → original ID
	reverse := make([]uint64, len(s.denseIdxToID))
	copy(reverse, s.denseIdxToID)

	// Build a new CSR graph where the external IDs are the dense indices
	newGraph := &csrDigraph{
		idToDenseIdx: make(map[uint64]uint64, len(s.denseIdxToID)),
		denseIdxToID: make([]uint64, len(s.denseIdxToID)),

		// Directly reuse the CSR structure (but translate neighbour IDs).
		outOffsets: make([]uint64, len(s.outOffsets)),
		inOffsets:  make([]uint64, len(s.inOffsets)),
		outAdj:     make([]uint64, len(s.outAdj)),
		inAdj:      make([]uint64, len(s.inAdj)),
	}

	// Populate bitmap + identity maps for dense IDs.
	for denseIdx := range s.denseIdxToID {
		newGraph.idToDenseIdx[uint64(denseIdx)] = uint64(denseIdx)
		newGraph.denseIdxToID[denseIdx] = uint64(denseIdx)
	}

	// Copy offsets (they are already correct because vertex ordering is identical).
	copy(newGraph.outOffsets, s.outOffsets)
	copy(newGraph.inOffsets, s.inOffsets)

	// Translate neighbour IDs from original → dense.
	for i, origNeighbour := range s.outAdj {
		newGraph.outAdj[i] = s.idToDenseIdx[origNeighbour]
	}

	for i, origNeighbour := range s.inAdj {
		newGraph.inAdj[i] = s.idToDenseIdx[origNeighbour]
	}

	return reverse, newGraph
}

type CSRDigraphBuilder struct {
	idToDenseIdx map[uint64]uint64 // external ID → dense index
	denseIdxToID []uint64          // dense index → external ID
	outTmp       AdjacencyMap
	inTmp        AdjacencyMap
}

func NewCSRDigraphBuilder() DigraphBuilder {
	return &CSRDigraphBuilder{
		idToDenseIdx: make(map[uint64]uint64),
		outTmp:       AdjacencyMap{},
		inTmp:        AdjacencyMap{},
	}
}

// ensureNode registers a vertex if it does not already exist. Returns the dense CSR index for the given external ID.
func (s *CSRDigraphBuilder) ensureNode(id uint64) uint64 {
	if idx, ok := s.idToDenseIdx[id]; ok {
		return idx
	}

	idx := uint64(len(s.denseIdxToID))

	s.idToDenseIdx[id] = idx
	s.denseIdxToID = append(s.denseIdxToID, id)

	// Allocate empty adjacency sets for the builder.
	s.outTmp[idx] = cardinality.NewBitmap64()
	s.inTmp[idx] = cardinality.NewBitmap64()

	return idx
}

func (s *CSRDigraphBuilder) AddNode(node uint64) {
	s.ensureNode(node)
}

func (s *CSRDigraphBuilder) AddEdge(start, end uint64) {
	var (
		startIdx = s.ensureNode(start)
		endIdx   = s.ensureNode(end)
	)

	// Outgoing edge
	if existingBitmap, exists := s.outTmp[startIdx]; exists {
		existingBitmap.Add(endIdx)
	} else {
		s.outTmp[startIdx] = cardinality.NewBitmap64With(endIdx)
	}

	// Incoming edge
	if existingBitmap, exists := s.inTmp[endIdx]; exists {
		existingBitmap.Add(startIdx)
	} else {
		s.inTmp[endIdx] = cardinality.NewBitmap64With(startIdx)
	}
}

func (s *CSRDigraphBuilder) Build() DirectedGraph {
	numNodes := uint64(len(s.denseIdxToID))

	// Allocate offset slices (len = numNodes+1)
	var (
		outOffsets = make([]uint64, numNodes+1)
		inOffsets  = make([]uint64, numNodes+1)
	)

	// Compute prefix sums (total edge counts per vertex)
	var outTotal, inTotal uint64

	for nextNode := uint64(0); nextNode < numNodes; nextNode++ {
		outTotal += s.outTmp[nextNode].Cardinality()
		inTotal += s.inTmp[nextNode].Cardinality()

		outOffsets[nextNode+1] = outTotal
		inOffsets[nextNode+1] = inTotal
	}

	// Allocate and fill adjacency arrays
	var (
		outAdj = make([]uint64, outTotal)
		inAdj  = make([]uint64, inTotal)
	)

	for nextNode := uint64(0); nextNode < numNodes; nextNode++ {
		var (
			outDenseIdx = outOffsets[nextNode]
			inDenseIdx  = inOffsets[nextNode]
		)

		s.outTmp[nextNode].Each(func(adjacentDenseIndex uint64) bool {
			outAdj[outDenseIdx] = s.denseIdxToID[adjacentDenseIndex]
			outDenseIdx++

			return true
		})

		s.inTmp[nextNode].Each(func(adjacentDenseIndex uint64) bool {
			inAdj[inDenseIdx] = s.denseIdxToID[adjacentDenseIndex]
			inDenseIdx++

			return true
		})
	}

	return &csrDigraph{
		idToDenseIdx: s.idToDenseIdx,
		denseIdxToID: s.denseIdxToID,
		outOffsets:   outOffsets,
		outAdj:       outAdj,
		inOffsets:    inOffsets,
		inAdj:        inAdj,
	}
}
