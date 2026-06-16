package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

func TestOptimizeStorage(t *testing.T) {
	t.Run("skips vacuum when dead tuple ratios are below threshold", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectOptimizeStorageStats(conn, "node", 9, 91)
		expectOptimizeStorageStats(conn, "edge", 0, 0)

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("vacuums node only", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectOptimizeStorageStats(conn, "node", 10, 90)
		expectOptimizeStorageStats(conn, "edge", 9, 91)
		expectOptimizeStorageVacuum(conn, "VACUUM (ANALYZE) node")

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("vacuums edge only", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectOptimizeStorageStats(conn, "node", 9, 91)
		expectOptimizeStorageStats(conn, "edge", 10, 90)
		expectOptimizeStorageVacuum(conn, "VACUUM (ANALYZE) edge")

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("vacuums node and edge", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectOptimizeStorageStats(conn, "node", 10, 90)
		expectOptimizeStorageStats(conn, "edge", 10, 90)
		expectOptimizeStorageVacuum(conn, "VACUUM (ANALYZE) node, edge")

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("returns query error", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)
		expectedErr := errors.New("stats unavailable")

		conn.ExpectQuery(optimizeStorageStatsQuery).
			WithArgs("node").
			WillReturnError(expectedErr)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, expectedErr)
		require.ErrorContains(t, err, "query dead tuple stats for node")
		require.NoError(t, conn.ExpectationsWereMet())
	})
}

func newOptimizeStorageMockConn(t *testing.T) pgxmock.PgxConnIface {
	t.Helper()

	conn, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	return conn
}

func expectOptimizeStorageStats(conn pgxmock.PgxConnIface, table string, dead, live int64) {
	conn.ExpectQuery(optimizeStorageStatsQuery).
		WithArgs(table).
		WillReturnRows(pgxmock.NewRows([]string{"dead", "live"}).AddRow(dead, live))
}

func expectOptimizeStorageVacuum(conn pgxmock.PgxConnIface, stmt string) {
	conn.ExpectExec(stmt).
		WithArgs(pgx.QueryExecModeSimpleProtocol).
		WillReturnResult(pgxmock.NewResult("VACUUM", 0))
}
