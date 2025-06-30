package container

import (
	"context"
	"log/slog"

	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/database/v1compat/query"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util"
)

func FetchDirectedGraph(ctx context.Context, db v1compat.Database, criteria v1compat.Criteria) (DirectedGraph, error) {
	var (
		measuref   = util.SLogMeasureFunction("FetchDirectedGraph")
		digraph    = NewCSRGraph()
		numResults = uint64(0)
	)

	if err := db.ReadTransaction(ctx, func(tx v1compat.Transaction) error {
		return tx.Relationships().Filter(criteria).Query(
			func(results v1compat.Result) error {
				var (
					startID graph.ID
					endID   graph.ID
				)

				for results.Next() {
					if err := results.Scan(&startID, &endID); err != nil {
						return err
					}

					digraph.AddEdge(startID.Uint64(), endID.Uint64())
					numResults += 1
				}

				return results.Error()
			},
			query.Returning(
				query.StartID(),
				query.EndID(),
			),
		)
	}); err != nil {
		return nil, err
	}

	measuref(
		slog.Uint64("num_nodes", digraph.NumNodes()),
		slog.Uint64("num_edges", numResults),
	)

	return digraph, nil
}

func FetchFilteredDirectedGraph(ctx context.Context, db v1compat.Database, traversalKinds ...graph.Kind) (DirectedGraph, error) {
	return FetchDirectedGraph(ctx, db, query.KindIn(query.Relationship(), traversalKinds...))
}
