package container

import (
	"context"
	"log/slog"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/database/v1compat/query"
	"github.com/specterops/dawgs/graph"
	v2Query "github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util"
)

func FetchDirectedGraph(ctx context.Context, db v1compat.Database, criteria v1compat.Criteria) (DirectedGraph, error) {
	var (
		measuref   = util.SLogMeasureFunction("FetchDirectedGraph")
		builder    = NewCSRDigraphBuilder()
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

					builder.AddEdge(startID.Uint64(), endID.Uint64())
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

	digraph := builder.Build()

	measuref(
		slog.Uint64("num_nodes", digraph.NumNodes()),
		slog.Uint64("num_edges", numResults),
	)

	return digraph, nil
}

func FetchKindDatabase(ctx context.Context, graphDB database.Instance) (KindDatabase, error) {
	defer util.SLogMeasureFunction("FetchKindDatabase")()

	edgeKinds := KindMap{}

	if err := graphDB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		builder := v2Query.New()
		builder.Return(v2Query.Relationship().ID(), v2Query.Relationship().Kind())

		if builtQuery, err := builder.Build(); err != nil {
			return err
		} else {
			var (
				result = driver.Exec(ctx, builtQuery.Query, builtQuery.Parameters)
				edgeID uint64
				kind   graph.Kind
			)

			for result.HasNext(ctx) {
				if err := result.Scan(&edgeID, &kind); err != nil {
					return err
				}

				edgeKinds.Add(kind, edgeID)
			}

			result.Close(ctx)
			return result.Error()
		}
	}); err != nil {
		return KindDatabase{}, err
	}

	return KindDatabase{
		EdgeKindMap: edgeKinds,
	}, nil
}

type TSDB struct {
	Triplestore MutableTriplestore
	EdgeKinds   KindMap
}

func (s TSDB) Projection(deletedNodes, deletedEdges cardinality.Duplex[uint64]) TSDB {
	return TSDB{
		Triplestore: s.Triplestore.Projection(deletedNodes, deletedEdges),
		EdgeKinds:   s.EdgeKinds,
	}
}

func NewTSDB(ts MutableTriplestore, edgeKinds KindMap) TSDB {
	return TSDB{
		Triplestore: ts,
		EdgeKinds:   edgeKinds,
	}
}

func EmptyTSDB() TSDB {
	return NewTSDB(NewTriplestore(), KindMap{})
}

func FetchTSDB(ctx context.Context, graphDB database.Instance, filter cypher.SyntaxNode) (TSDB, error) {
	defer util.SLogMeasureFunction("FetchTriplestore")()

	var (
		store = NewTriplestore()
		tsdb  = TSDB{
			Triplestore: store,
			EdgeKinds:   KindMap{},
		}
		err = graphDB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
			query := v2Query.New().Return(
				v2Query.Start().ID(),
				v2Query.Relationship().ID(),
				v2Query.Relationship().Kind(),
				v2Query.End().ID(),
			)

			if filter != nil {
				query.Where(filter)
			}

			if builtQuery, err := query.Build(); err != nil {
				return err
			} else {
				result := driver.Exec(ctx, builtQuery.Query, builtQuery.Parameters)
				defer result.Close(ctx)

				for result.HasNext(ctx) {
					var (
						startID          uint64
						relationshipID   uint64
						relationshipKind graph.Kind
						endID            uint64
					)

					if err := result.Scan(&startID, &relationshipID, &relationshipKind, &endID); err != nil {
						return err
					}

					tsdb.Triplestore.AddTriple(relationshipID, startID, endID)
					tsdb.EdgeKinds.Add(relationshipKind, relationshipID)
				}

				return result.Error()
			}
		})
	)

	return tsdb, err
}

func FetchFilteredDirectedGraph(ctx context.Context, db v1compat.Database, traversalKinds ...graph.Kind) (DirectedGraph, error) {
	return FetchDirectedGraph(ctx, db, query.KindIn(query.Relationship(), traversalKinds...))
}
