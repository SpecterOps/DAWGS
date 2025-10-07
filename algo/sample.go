package algo

import (
	"math/rand"
	"sort"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

type SampleFunc func(digraph container.DirectedGraph, nSamples int) []uint64

func sampleHighestDegrees(digraph container.DirectedGraph, nSamples int, direction graph.Direction) []uint64 {
	type entry struct {
		NodeID  uint64
		Degrees uint64
	}

	if numNodes := int(digraph.Nodes().Cardinality()); nSamples <= 0 || numNodes == 0 {
		return nil
	} else if nSamples > numNodes {
		nSamples = numNodes
	}

	entries := make([]entry, 0, digraph.Nodes().Cardinality())

	digraph.EachNode(func(node uint64) bool {
		entries = append(entries, entry{
			NodeID:  node,
			Degrees: digraph.Degrees(node, direction),
		})

		return true
	})

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Degrees > entries[j].Degrees
	})

	nodeSamples := make([]uint64, 0, nSamples)

	for idx := 0; idx < nSamples; idx++ {
		nodeSamples = append(nodeSamples, entries[idx].NodeID)
	}

	return nodeSamples
}

func SampleHighestDegrees(direction graph.Direction) SampleFunc {
	return func(digraph container.DirectedGraph, nSamples int) []uint64 {
		return sampleHighestDegrees(digraph, nSamples, direction)
	}
}

func SampleRandom(digraph container.DirectedGraph, nSamples int) []uint64 {
	if numNodes := int(digraph.Nodes().Cardinality()); nSamples <= 0 || numNodes == 0 {
		return nil
	} else if nSamples > numNodes {
		nSamples = numNodes
	}

	var (
		samples   = make([]uint64, 0, nSamples)
		stride    = digraph.Nodes().Cardinality() / uint64(nSamples)
		counter   = (rand.Uint64() % stride) + 1
		remainder = stride - counter
	)

	digraph.Nodes().Each(func(value uint64) bool {
		if counter -= 1; counter == 0 {
			samples = append(samples, value)
			counter = (rand.Uint64() % stride) + 1 + remainder
			remainder = stride - (counter - remainder)
		}

		return len(samples) < nSamples
	})

	return samples
}
