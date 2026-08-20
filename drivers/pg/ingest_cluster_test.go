package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	pgquery "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/stretchr/testify/require"
)

func TestIngestClusterDiscoversAndClustersOnlyTargetChildrenInNodeEdgeOrder(t *testing.T) {
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	target := model.Graph{
		Partitions: model.GraphPartitions{
			Node: model.NewGraphPartition(`node"; strange`),
			Edge: model.NewGraphPartition(`edge with space`),
		},
	}
	nodeIndex := `node_idx"; strange`
	edgeIndex := `edge idx`

	pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
		WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
		WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow(nodeIndex)).
		RowsWillBeClosed()
	pool.ExpectExec(`cluster "node""; strange" using "node_idx""; strange";`).
		WillReturnResult(pgxmock.NewResult("CLUSTER", 0))
	pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
		WithArgs(pgx.Identifier{target.Partitions.Edge.Name}.Sanitize()).
		WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow(edgeIndex)).
		RowsWillBeClosed()
	pool.ExpectExec(`cluster "edge with space" using "edge idx";`).
		WillReturnResult(pgxmock.NewResult("CLUSTER", 0))

	engine := &ingestEngine{clusterDB: pool, graphTarget: target}
	require.NoError(t, engine.clusterTargetPartitions(context.Background()))
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestIngestClusterRequiresExactlyOneHashIndexPerTargetPartition(t *testing.T) {
	for name, rows := range map[string]*pgxmock.Rows{
		"missing":   pgxmock.NewRows([]string{"relname"}),
		"ambiguous": pgxmock.NewRows([]string{"relname"}).AddRow("first").AddRow("second"),
	} {
		t.Run(name, func(t *testing.T) {
			pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			target := testDriverIngestTarget()
			pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
				WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
				WillReturnRows(rows).
				RowsWillBeClosed()

			engine := &ingestEngine{clusterDB: pool, graphTarget: target}
			err = engine.clusterTargetPartitions(context.Background())

			require.Error(t, err)
			require.Contains(t, err.Error(), "exactly one")
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

type countingIngestClusterDB struct {
	ingestClusterDB
	execCalls int
}

func (s *countingIngestClusterDB) Exec(
	_ context.Context,
	_ string,
	_ ...any,
) (pgconn.CommandTag, error) {
	s.execCalls++
	return pgconn.CommandTag{}, errors.New("unexpected cluster execution")
}

func TestIngestClusterRejectsWrongColumnOnlyCatalogResultBeforeExec(t *testing.T) {
	statement := pgquery.FormatFindIngestHashIndex()
	require.Contains(t, statement, "attribute.attname = 'id_hash'")

	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	target := testDriverIngestTarget()
	pool.ExpectQuery(statement).
		WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
		WillReturnRows(pgxmock.NewRows([]string{"relname"})).
		RowsWillBeClosed()
	db := &countingIngestClusterDB{ingestClusterDB: pool}

	engine := &ingestEngine{clusterDB: db, graphTarget: target}
	err = engine.clusterTargetPartitions(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one")
	require.Zero(t, db.execCalls)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestIngestClusterStopsAtFirstDiscoveryOrExecutionError(t *testing.T) {
	tests := map[string]struct {
		expect func(pgxmock.PgxPoolIface, model.Graph, error)
	}{
		"discovery": {
			expect: func(pool pgxmock.PgxPoolIface, target model.Graph, cause error) {
				pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
					WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
					WillReturnError(cause)
			},
		},
		"execution": {
			expect: func(pool pgxmock.PgxPoolIface, target model.Graph, cause error) {
				pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
					WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
					WillReturnRows(pgxmock.NewRows([]string{"relname"}).AddRow("node_hash_idx")).
					RowsWillBeClosed()
				pool.ExpectExec(`cluster "node_42" using "node_hash_idx";`).WillReturnError(cause)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			target := testDriverIngestTarget()
			cause := errors.New("first cluster failure")
			test.expect(pool, target, cause)

			engine := &ingestEngine{clusterDB: pool, graphTarget: target}
			err = engine.clusterTargetPartitions(context.Background())

			require.ErrorIs(t, err, cause)
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestIngestClusterChecksRowsErrors(t *testing.T) {
	rowsErr := errors.New("catalog rows failed")
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	target := testDriverIngestTarget()
	pool.ExpectQuery(pgquery.FormatFindIngestHashIndex()).
		WithArgs(pgx.Identifier{target.Partitions.Node.Name}.Sanitize()).
		WillReturnRows(pgxmock.NewRows([]string{"relname"}).
			AddRow("node_hash_idx").
			RowError(0, rowsErr)).
		RowsWillBeClosed()

	engine := &ingestEngine{clusterDB: pool, graphTarget: target}
	err = engine.clusterTargetPartitions(context.Background())

	require.ErrorIs(t, err, rowsErr)
	require.NoError(t, pool.ExpectationsWereMet())
}
