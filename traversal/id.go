package traversal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/util"
	"github.com/specterops/dawgs/util/channels"
)

// IDDriver is a function that drives sending queries to the graph and retrieving vertexes and edges. Traversal
// drivers are expected to operate on a cactus tree representation of path space using the graph.PathSegment data
// structure. Path segments returned by a traversal drivers are considered extensions of path space that require
// further expansion. If a traversal drivers returns no descending path segments then the given segment may be
// considered terminal.
type IDDriver func(ctx context.Context, tx graph.Transaction, segment *graph.IDSegment) ([]*graph.IDSegment, error)

type IDPlan struct {
	Root     graph.ID
	Delegate IDDriver
}

type IDTraversal struct {
	db         graph.Database
	numWorkers int
}

func NewIDTraversal(db graph.Database, numParallelWorkers int) IDTraversal {
	return IDTraversal{
		db:         db,
		numWorkers: numParallelWorkers,
	}
}

func (s IDTraversal) BreadthFirst(ctx context.Context, plan IDPlan) error {
	var (
		// workerWG keeps count of background workers launched in goroutines
		workerWG = &sync.WaitGroup{}

		// descentWG keeps count of in-flight traversal work. When this wait group reaches a count of 0 the traversal
		// is considered complete.
		completionC  = make(chan struct{}, s.numWorkers*2)
		descentCount = &atomic.Int64{}

		errorCollector                 = util.NewErrorCollector()
		pathTree                       = graph.NewRootIDSegment(plan.Root)
		traversalCtx, doneFunc         = context.WithCancel(ctx)
		segmentWriterC, segmentReaderC = channels.BufferedPipe[*graph.IDSegment](traversalCtx)
	)

	// Defer calling the cancellation function of the context to ensure that all workers join, no matter what
	defer doneFunc()

	// Close the writer channel to the buffered pipe
	defer close(segmentWriterC)

	// Launch the background traversal workers
	for workerID := 0; workerID < s.numWorkers; workerID++ {
		workerWG.Add(1)

		go func(workerID int) {
			defer workerWG.Done()

			if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				for {
					if nextDescent, ok := channels.Receive(traversalCtx, segmentReaderC); !ok {
						return nil
					} else if tx.GraphQueryMemoryLimit() > 0 && pathTree.SizeOf() > tx.GraphQueryMemoryLimit() {
						return fmt.Errorf("%w - Limit: %.2f MB - Memory In-Use: %.2f MB", ops.ErrGraphQueryMemoryLimit, tx.GraphQueryMemoryLimit().Mebibytes(), pathTree.SizeOf().Mebibytes())
					} else if descendingSegments, err := plan.Delegate(traversalCtx, tx, nextDescent); err != nil {
						return err
					} else if len(descendingSegments) > 0 {
						for _, descendingSegment := range descendingSegments {
							// Add to the descent count before submitting to the channel
							descentCount.Add(1)
							channels.Submit(traversalCtx, segmentWriterC, descendingSegment)
						}
					}

					// Mark descent for this segment as complete
					descentCount.Add(-1)

					if !channels.Submit(traversalCtx, completionC, struct{}{}) {
						return nil
					}
				}
			}); err != nil && !errors.Is(err, graph.ErrContextTimedOut) {
				// A worker encountered a fatal error, kill the traversal context
				doneFunc()

				errorCollector.Add(fmt.Errorf("reader %d failed: %w", workerID, err))
			}
		}(workerID)
	}

	// Add to the descent wait group and then queue the root of the path tree for traversal
	descentCount.Add(1)
	if channels.Submit(traversalCtx, segmentWriterC, pathTree) {
		for {
			if _, ok := channels.Receive(traversalCtx, completionC); !ok || descentCount.Load() == 0 {
				break
			}
		}
	}

	// Actively cancel the traversal context to force any idle workers to join and exit
	doneFunc()

	// Wait for all workers to exit
	workerWG.Wait()

	return errorCollector.Combined()
}
