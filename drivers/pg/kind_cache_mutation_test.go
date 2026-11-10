package pg

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestKindCacheDirectMutationsAdoptBeforeBinding(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		name := "node"
		if relationship {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			manager := testKindManager(t)
			kind := graph.StringKind("Refreshed")
			cache := manager.kindCache
			require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{kind: 1}, false, func() {}))
			manager.hasDefaultGraph = true
			manager.defaultGraph = model.Graph{ID: 1}
			cfg, err := renderConfig(1, readWriteTxOptions, nil)
			require.NoError(t, err)
			tx, err := newTransactionWrapper(ctx, nil, manager, cfg, false)
			require.NoError(t, err)
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			tx.tx = mock
			require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{kind: 2}, false, func() {}))
			properties := graph.NewProperties()
			jsonb, err := pgsql.PropertiesToJSONB(properties)
			require.NoError(t, err)
			if relationship {
				mock.ExpectQuery(createEdgeStatement).WithArgs(pgx.NamedArgs{
					"graph_id": int32(1), "start_id": graph.ID(10), "end_id": graph.ID(20), "kind_id": int16(2), "properties": jsonb,
				}).WillReturnRows(pgxmock.NewRows([]string{"edge"}).AddRow(&edgeComposite{ID: 3, StartID: 10, EndID: 20, KindID: 2}))
				edge, err := tx.CreateRelationshipByIDs(10, 20, kind, properties)
				require.NoError(t, err)
				require.Equal(t, kind, edge.Kind)
			} else {
				mock.ExpectQuery(createNodeStatement).WithArgs(pgx.NamedArgs{
					"graph_id": int32(1), "kind_ids": []int16{2}, "properties": jsonb,
				}).WillReturnRows(pgxmock.NewRows([]string{"node"}).AddRow(&nodeComposite{ID: 3, KindIDs: []int16{2}}))
				node, err := tx.CreateNode(properties, kind)
				require.NoError(t, err)
				require.Equal(t, graph.Kinds{kind}, node.Kinds)
			}
			require.Same(t, cache.current.Load(), tx.kindMapper.snapshot)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
