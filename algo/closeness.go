package algo

import (
	"runtime"
	"sync"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

func ClosenessForDirectedUnweightedGraph(digraph container.DirectedGraph, direction graph.Direction, sampleFunc SampleFunc, nSamples int) map[uint64]Weight {
	scores := make(map[uint64]Weight, nSamples)

	for _, nodeID := range sampleFunc(digraph, nSamples) {
		if shortestPathTerminals := digraph.BFSTree(nodeID, direction); len(shortestPathTerminals) > 0 {
			var distanceSum Weight = 0

			for _, shortestPathTerminal := range shortestPathTerminals {
				distanceSum += shortestPathTerminal.Distance
			}

			if distanceSum > 0 {
				// Normalize by (len(shortestPathTerminals) - 1) for disconnected components
				scores[nodeID] += Weight(len(shortestPathTerminals)-1) / distanceSum
			}
		}
	}

	return scores
}

func ClosenessForDirectedUnweightedGraphParallel(digraph container.DirectedGraph, direction graph.Direction, sampleFunc SampleFunc, nSamples int) map[uint64]Weight {
	var (
		scores     = make(map[uint64]Weight, nSamples)
		scoresLock = &sync.Mutex{}
		workerWG   = &sync.WaitGroup{}
		nodeC      = make(chan uint64)
	)

	for workerID := 0; workerID < runtime.NumCPU(); workerID++ {
		workerWG.Add(1)

		go func() {
			defer workerWG.Done()

			for nodeID := range nodeC {
				if shortestPathTerminals := digraph.BFSTree(nodeID, direction); len(shortestPathTerminals) > 0 {
					var distanceSum Weight = 0

					for _, shortestPathTerminal := range shortestPathTerminals {
						distanceSum += shortestPathTerminal.Distance
					}

					if distanceSum > 0 {
						scoresLock.Lock()

						// Normalize by (len(shortestPathTerminals) - 1) for disconnected components
						scores[nodeID] += Weight(len(shortestPathTerminals)-1) / distanceSum

						scoresLock.Unlock()
					}
				}
			}
		}()
	}

	for _, nextNodeID := range sampleFunc(digraph, nSamples) {
		nodeC <- nextNodeID
	}

	close(nodeC)
	workerWG.Wait()

	return scores
}
