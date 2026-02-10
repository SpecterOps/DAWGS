package container

import (
	"github.com/gammazero/deque"
	"github.com/specterops/dawgs/graph"
)

func TSDFS(ts Triplestore, nodeID uint64, direction graph.Direction, maxDepth int, descentFilter func(edge Edge) bool, handler func(segment *Segment) bool) int {
	var (
		traversals         deque.Deque[*Segment]
		numImcompletePaths = 0
	)

	traversals.PushBack(&Segment{
		Node: nodeID,
	})

	for remainingTraversals := traversals.Len(); remainingTraversals > 0; remainingTraversals = traversals.Len() {
		var (
			nextSegment  = traversals.PopBack()
			segmentDepth = nextSegment.Depth()
			depthExceded = maxDepth > 0 && maxDepth < segmentDepth
		)

		if !depthExceded {
			ts.EachAdjacentEdge(nextSegment.Node, direction, func(nextEdge Edge) bool {
				if descentFilter(nextEdge) {
					traversals.PushBack(&Segment{
						Node:     nextEdge.Pick(direction),
						Edge:     nextEdge.ID,
						Previous: nextSegment,
					})
				}

				return true
			})
		}

		if segmentDepth > 1 && remainingTraversals-1 == traversals.Len() {
			if depthExceded {
				numImcompletePaths += 1
			}

			if !handler(nextSegment) {
				break
			}
		}
	}

	return numImcompletePaths
}

func TSBFS(ts Triplestore, nodeID uint64, direction graph.Direction, maxDepth int, descentFilter func(edge Edge) bool, handler func(segment *Segment) bool) int {
	var (
		traversals         deque.Deque[*Segment]
		numImcompletePaths = 0
	)

	traversals.PushBack(&Segment{
		Node: nodeID,
	})

	for remainingTraversals := traversals.Len(); remainingTraversals > 0; remainingTraversals = traversals.Len() {
		var (
			nextSegment  = traversals.PopFront()
			segmentDepth = nextSegment.Depth()
			depthExceded = maxDepth > 0 && maxDepth < segmentDepth
		)

		if !depthExceded {
			ts.EachAdjacentEdge(nextSegment.Node, direction, func(nextEdge Edge) bool {
				if descentFilter(nextEdge) {
					traversals.PushBack(&Segment{
						Node:     nextEdge.Pick(direction),
						Edge:     nextEdge.ID,
						Previous: nextSegment,
					})
				}

				return true
			})
		}

		if segmentDepth > 1 && remainingTraversals-1 == traversals.Len() {
			if depthExceded {
				numImcompletePaths += 1
			}

			if !handler(nextSegment) {
				break
			}
		}
	}

	return numImcompletePaths
}

// SSPBFS is a parallel stateless shortest-path breadth-first search.
func TSStatelessBFS(ts Triplestore, rootNode uint64, direction graph.Direction, maxDepth int, descentFilter func(edge Edge) (Weight, bool), terminalHandler func(terminal PathTerminal) bool, numWorkers int) int {
	var (
		traversals         deque.Deque[PathTerminal]
		numImcompletePaths = 0
	)

	traversals.PushBack(PathTerminal{
		Node:     rootNode,
		Weight:   0,
		Distance: 0,
	})

	for traversals.Len() > 0 {
		var (
			nextSegment   = traversals.PopFront()
			hasExpansions = false
			depthExceded  = maxDepth > 0 && maxDepth < nextSegment.Distance
		)

		if !depthExceded {
			nextDistance := nextSegment.Distance + 1

			ts.EachAdjacentEdge(nextSegment.Node, direction, func(nextEdge Edge) bool {
				if weight, shouldDescend := descentFilter(nextEdge); shouldDescend {
					hasExpansions = true

					if nextSegment.Distance > 0 {
						weight *= nextSegment.Weight
					}

					traversals.PushBack(PathTerminal{
						Node:     nextEdge.Pick(direction),
						Distance: nextDistance,
						Weight:   weight,
					})
				}

				return true
			})
		}

		if nextSegment.Distance >= 1 && !hasExpansions {
			if depthExceded {
				numImcompletePaths += 1
			}

			if !terminalHandler(nextSegment) {
				break
			}
		}
	}

	return numImcompletePaths
}
