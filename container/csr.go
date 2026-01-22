package container

import (
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
)

// csrDigraph implements a mutable directed graph using compressed sparse row storage.
type csrDigraph struct {
	// Mapping between external IDs and dense CSR indices
	idToIdx map[uint64]uint64 // external ID → dense index
	idxToID []uint64          // dense index → external ID

	// CSR storage for outgoing edges
	outOffsets []uint64 // length = NumNodes()+1
	outAdj     []uint64 // concatenated outgoing neighbour IDs (external IDs)

	// CSR storage for incoming edges
	inOffsets []uint64
	inAdj     []uint64

	// Builder‑only temporary adjacency maps (dense indices)
	outTmp map[uint64]map[uint64]struct{}
	inTmp  map[uint64]map[uint64]struct{}

	// frozen indicates whether the CSR slices have been built
	frozen bool
}

// NewCSRGraph creates an empty mutable directed graph that uses CSR internally.
func NewCSRGraph() MutableDirectedGraph {
	return &csrDigraph{
		idToIdx: make(map[uint64]uint64),
		outTmp:  make(map[uint64]map[uint64]struct{}),
		inTmp:   make(map[uint64]map[uint64]struct{}),
	}
}

// ensureNode registers a vertex if it does not already exist. Returns the dense CSR index for the given external ID.
func (g *csrDigraph) ensureNode(id uint64) uint64 {
	if idx, ok := g.idToIdx[id]; ok {
		return idx
	}

	idx := uint64(len(g.idxToID))

	g.idToIdx[id] = idx
	g.idxToID = append(g.idxToID, id)

	// Allocate empty adjacency sets for the builder.
	g.outTmp[idx] = make(map[uint64]struct{})
	g.inTmp[idx] = make(map[uint64]struct{})

	return idx
}

// freeze converts the builder maps (outTmp/inTmp) into CSR slices. This function is idempotent.
func (g *csrDigraph) freeze() {
	if g.frozen {
		return
	}

	numNodes := uint64(len(g.idxToID))

	// Allocate offset slices (len = numNodes+1)
	g.outOffsets = make([]uint64, numNodes+1)
	g.inOffsets = make([]uint64, numNodes+1)

	// First pass: compute prefix sums (total edge counts per vertex).
	var outTotal, inTotal uint64
	for v := uint64(0); v < numNodes; v++ {
		outCount := uint64(len(g.outTmp[v]))
		inCount := uint64(len(g.inTmp[v]))

		g.outOffsets[v+1] = g.outOffsets[v] + outCount
		g.inOffsets[v+1] = g.inOffsets[v] + inCount

		outTotal += outCount
		inTotal += inCount
	}

	// Allocate adjacency arrays of exact size.
	g.outAdj = make([]uint64, outTotal)
	g.inAdj = make([]uint64, inTotal)

	// Second pass: fill adjacency arrays.
	for v := uint64(0); v < numNodes; v++ {
		// Outbound
		baseOut := g.outOffsets[v]
		i := baseOut
		for nbrIdx := range g.outTmp[v] {
			g.outAdj[i] = g.idxToID[nbrIdx] // store external ID
			i++
		}

		// Inbound
		baseIn := g.inOffsets[v]
		j := baseIn
		for srcIdx := range g.inTmp[v] {
			g.inAdj[j] = g.idxToID[srcIdx]
			j++
		}
	}

	// Discard temporary maps
	g.outTmp = nil
	g.inTmp = nil

	g.frozen = true
}

// idx returns the dense CSR index for an external node ID. Returns (idx, true) if the node exists, otherwise (0, false).
func (g *csrDigraph) idx(node uint64) (uint64, bool) {
	idx, exists := g.idToIdx[node]
	return idx, exists
}

// csrRange returns the slice of neighbours for a given vertex index and direction. The returned slice contains external IDs.
func (g *csrDigraph) csrRange(idx uint64, dir graph.Direction) []uint64 {
	switch dir {
	case graph.DirectionOutbound:
		var (
			start = g.outOffsets[idx]
			end   = g.outOffsets[idx+1]
		)

		return g.outAdj[start:end]

	case graph.DirectionInbound:
		var (
			start = g.inOffsets[idx]
			end   = g.inOffsets[idx+1]
		)

		return g.inAdj[start:end]

	default:
		var (
			outStart, outEnd = g.outOffsets[idx], g.outOffsets[idx+1]
			inStart, inEnd   = g.inOffsets[idx], g.inOffsets[idx+1]
		)

		merged := make([]uint64, 0, (outEnd-outStart)+(inEnd-inStart))
		merged = append(merged, g.outAdj[outStart:outEnd]...)
		merged = append(merged, g.inAdj[inStart:inEnd]...)

		return merged
	}
}

func (g *csrDigraph) AddNode(node uint64) {
	if g.frozen {
		panic("AddNode called after graph has been frozen")
	}

	g.ensureNode(node)
}

func (g *csrDigraph) AddEdge(start, end uint64) {
	if g.frozen {
		panic("AddEdge called after graph has been frozen")
	}

	startIdx := g.ensureNode(start)
	endIdx := g.ensureNode(end)

	// Outgoing edge start → end
	if _, exists := g.outTmp[startIdx][endIdx]; !exists {
		g.outTmp[startIdx][endIdx] = struct{}{}
	}
	// Incoming edge end ← start
	if _, exists := g.inTmp[endIdx][startIdx]; !exists {
		g.inTmp[endIdx][startIdx] = struct{}{}
	}
}

func (g *csrDigraph) NumNodes() uint64 {
	return uint64(len(g.idxToID))
}

func (g *csrDigraph) NumEdges() uint64 {
	g.freeze()

	// each directed edge appears once in outAdj
	return uint64(len(g.outAdj))
}

func (g *csrDigraph) Nodes() cardinality.Duplex[uint64] {
	return cardinality.NewBitmap64With(g.idxToID...)
}

func (g *csrDigraph) EachNode(delegate func(node uint64) bool) {
	for _, id := range g.idxToID {
		if !delegate(id) {
			return
		}
	}
}

func (g *csrDigraph) AdjacentNodes(node uint64, direction graph.Direction) []uint64 {
	g.freeze()

	if idx, ok := g.idx(node); ok {
		return g.csrRange(idx, direction)
	}

	return nil
}

func (g *csrDigraph) Degrees(node uint64, direction graph.Direction) uint64 {
	g.freeze()

	if idx, ok := g.idx(node); ok {
		switch direction {
		case graph.DirectionOutbound:
			return g.outOffsets[idx+1] - g.outOffsets[idx]
		case graph.DirectionInbound:
			return g.inOffsets[idx+1] - g.inOffsets[idx]
		case graph.DirectionBoth:
			return (g.outOffsets[idx+1] - g.outOffsets[idx]) + (g.inOffsets[idx+1] - g.inOffsets[idx])
		}
	}

	return 0
}

func (g *csrDigraph) EachAdjacentNode(node uint64, direction graph.Direction, delegate func(adjacent uint64) bool) {
	g.freeze()

	if idx, exists := g.idx(node); exists {
		switch direction {
		case graph.DirectionOutbound:
			for next, end := g.outOffsets[idx], g.outOffsets[idx+1]; next < end; next++ {
				if !delegate(g.outAdj[next]) {
					return
				}
			}

		case graph.DirectionInbound:
			for next, end := g.inOffsets[idx], g.inOffsets[idx+1]; next < end; next++ {
				if !delegate(g.inAdj[next]) {
					return
				}
			}

		default:
			for next, end := g.outOffsets[idx], g.outOffsets[idx+1]; next < end; next++ {
				if !delegate(g.outAdj[next]) {
					return
				}
			}

			for next, end := g.inOffsets[idx], g.inOffsets[idx+1]; next < end; next++ {
				if !delegate(g.inAdj[next]) {
					return
				}
			}
		}
	}
}

func (g *csrDigraph) Normalize() ([]uint64, DirectedGraph) {
	g.freeze()

	// Reverse map: denseIdx → original ID
	reverse := make([]uint64, len(g.idxToID))
	copy(reverse, g.idxToID)

	// Build a new CSR graph where the external IDs are the dense indices
	newGraph := &csrDigraph{
		idToIdx: make(map[uint64]uint64, len(g.idxToID)),
		idxToID: make([]uint64, len(g.idxToID)),

		// Directly reuse the CSR structure (but translate neighbour IDs).
		outOffsets: make([]uint64, len(g.outOffsets)),
		inOffsets:  make([]uint64, len(g.inOffsets)),
		outAdj:     make([]uint64, len(g.outAdj)),
		inAdj:      make([]uint64, len(g.inAdj)),

		frozen: true,
	}

	// Populate bitmap + identity maps for dense IDs.
	for denseIdx := range g.idxToID {
		newGraph.idToIdx[uint64(denseIdx)] = uint64(denseIdx)
		newGraph.idxToID[denseIdx] = uint64(denseIdx)
	}

	// Copy offsets (they are already correct because vertex ordering is identical).
	copy(newGraph.outOffsets, g.outOffsets)
	copy(newGraph.inOffsets, g.inOffsets)

	// Translate neighbour IDs from original → dense.
	for i, origNeighbour := range g.outAdj {
		newGraph.outAdj[i] = g.idToIdx[origNeighbour]
	}

	for i, origNeighbour := range g.inAdj {
		newGraph.inAdj[i] = g.idToIdx[origNeighbour]
	}

	return reverse, newGraph
}
