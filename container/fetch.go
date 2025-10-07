package container

import (
	"context"
	"sync"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func FetchDirectedGraph(ctx context.Context, graphDB graph.Database, relationshipFilter graph.Criteria) (DirectedGraph, error) {
	type anonymousEdge struct {
		StartID graph.ID
		EndID   graph.ID
	}

	var (
		edgeC   = make(chan anonymousEdge, 4096)
		mergeWG = &sync.WaitGroup{}
		digraph = NewDirectedGraph()
	)

	mergeWG.Add(1)

	go func() {
		defer mergeWG.Done()

		for edge := range edgeC {
			digraph.AddEdge(edge.StartID, edge.EndID)
		}
	}()

	if err := graphDB.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(relationshipFilter).Query(func(results graph.Result) error {
			for results.Next() {
				var (
					startID graph.ID
					endID   graph.ID
				)

				if err := results.Scan(&startID, &endID); err != nil {
					return err
				}

				edgeC <- anonymousEdge{
					StartID: startID,
					EndID:   endID,
				}
			}

			return results.Error()
		}, query.Returning(query.StartID(), query.EndID()))
	}); err != nil {
		// Ensure that the edge channel is closed to prevent goroutine leaks
		close(edgeC)
		return nil, err
	}

	close(edgeC)
	mergeWG.Wait()

	return digraph, nil
}
