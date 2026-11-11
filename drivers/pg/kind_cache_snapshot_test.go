package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v6"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestKindCacheStableRefreshBeforeFirstQuery checks initial cache reconciliation within a stable transaction snapshot.
func TestKindCacheStableRefreshBeforeFirstQuery(t *testing.T) {
	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		t.Run(string(isolation), func(t *testing.T) {
			manager := testKindManager(t)
			kind := graph.StringKind("SnapshotKind")
			cache := manager.kindCache
			require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{kind: 1}, false, func() {}))
			manager.setDefaultGraph(model.Graph{
				ID: 1,
			}, graph.Graph{
				Name: "snapshot",
			})
			cfg := &Config{
				Options: pgx.TxOptions{
					IsoLevel: isolation,
				},
			}
			tx, err := newTransactionWrapper(context.Background(), nil, manager, cfg, false)
			require.NoError(t, err)
			mock, err := pgxmock.NewConn()
			require.NoError(t, err)
			tx.tx = mock // BEGIN alone has not established PostgreSQL's snapshot.
			require.True(t, cache.replace(cache.current.Load(), cache.requested.Load(), map[graph.Kind]int16{kind: 2}, false, manager.invalidateKindTranslations))
			shared := cache.current.Load()
			mock.ExpectQuery("select id, name from kind").
				WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(int16(2), kind.String()))
			mock.ExpectQuery(`array \[2\]::int2\[\]`).
				WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))

			result := tx.Query("MATCH (n:SnapshotKind) RETURN count(n)", nil)
			require.NoError(t, result.Error())
			require.True(t, result.Next())
			var count int64
			require.NoError(t, graph.ScanNextResult(result, &count))
			result.Close()
			require.NoError(t, result.Error())
			require.EqualValues(t, 1, count)
			require.True(t, tx.kindMapper.privateCatalog)
			require.True(t, tx.kindMapper.bypassTranslations())
			require.Same(t, shared, cache.current.Load(), "the transaction's catalog must stay private")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestKindMapperStableCatalogReconciliationFailure checks that stable-snapshot catalog errors propagate without partial adoption.
func TestKindMapperStableCatalogReconciliationFailure(t *testing.T) {
	manager := testKindManager(t)
	known := graph.StringKind("Known")
	require.True(t, manager.kindCache.merge(map[graph.Kind]int16{known: 1}, 0))
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	tx := &transaction{
		ctx:           context.Background(),
		schemaManager: manager,
		tx:            mock,
		isolation:     pgx.RepeatableRead,
	}
	mapper := &transactionKindMapper{
		tx:       tx,
		snapshot: manager.kindCache.current.Load(),
	}
	tx.kindMapper = mapper
	mapper.learn(graph.StringKind("Learned"), 2)
	before := mapper.snapshot
	manager.kindCache.fence()
	failure := errors.New("catalog unavailable")
	mock.ExpectQuery("select id, name from kind").WillReturnError(failure)

	require.ErrorIs(t, tx.serviceKinds(), failure)
	require.Same(t, before, mapper.snapshot)
	require.False(t, mapper.privateCatalog)
	require.Len(t, mapper.kinds, 1, "failed reconciliation must preserve private lookups")
	require.NoError(t, mock.ExpectationsWereMet())
}
