package algo

import (
	"maps"
	"math"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

/*
CalculateKatzCentrality is a simple power iterative implementation of katz centrality.

Parameters:

The parameter `alpha` is the attenuation factor. The parameter is used to "attenuate" or reduce the weight of each
connection as the distance between nodes increases. This means a path of length 2 (two links) is less influential than
a path of length 1.

This is used to balance the influence of longer paths.

The parameter `beta` represents a baseline importance of each node.

The parameter `epsilon` represents convergence tolerance. It is used in the iterative calculation to determine when
the algorithm has reached a stable solution and should stop.

This function returns a map that contains the computed katz centrality for each node along with a boolean representing
whether the algorithm reached a convergence that meets the epsilon threshold or not.

High katz centrality values indicate that a node has significant influence within a graph by having many indirect
edges, as well as direct ones. The centrality score also accounts for the declining importance of more distant
relationships.
*/
func CalculateKatzCentrality(digraph container.DirectedGraph, alpha, beta, epsilon Weight, iterations int) (map[uint64]float64, bool) {
	var (
		numNodes       = digraph.Nodes().Cardinality()
		centrality     = make(map[uint64]float64, numNodes)
		prevCentrality = make(map[uint64]float64, numNodes)
	)

	// Initialize centrality scores to baseline
	digraph.Nodes().Each(func(value uint64) bool {
		centrality[value] = beta
		return true
	})

	for iter := 0; iter < iterations; iter++ {
		maps.Copy(prevCentrality, centrality)

		changed := false

		digraph.Nodes().Each(func(sourceNode uint64) bool {
			sum := 0.0

			digraph.EachAdjacent(sourceNode, graph.DirectionBoth, func(adjacentNode uint64) bool {
				sum += prevCentrality[adjacentNode]
				return true
			})

			centrality[sourceNode] = beta + alpha*sum

			if math.Abs(centrality[sourceNode]-prevCentrality[sourceNode]) > epsilon {
				changed = true
			}

			return true
		})

		if !changed {
			return centrality, true
		}
	}

	return centrality, false
}
