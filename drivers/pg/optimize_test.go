package pg

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

// TestOptimizeStorage verifies optimization vacuums both graph storage parents in one statement.
func TestOptimizeStorage(t *testing.T) {
	t.Run("always vacuums node and edge", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectOptimizeStorageVacuum(conn, "VACUUM (ANALYZE) node, edge")

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})
}

func newOptimizeStorageMockConn(t *testing.T) pgxmock.PgxConnIface {
	t.Helper()

	conn, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)

	return conn
}

func expectOptimizeStorageVacuum(conn pgxmock.PgxConnIface, stmt string) {
	conn.ExpectExec(stmt).
		WithArgs(pgx.QueryExecModeSimpleProtocol).
		WillReturnResult(pgxmock.NewResult("VACUUM", 0))
}
