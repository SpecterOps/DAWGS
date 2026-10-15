package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

const optimizeStorageVacuumStatement = "VACUUM (ANALYZE) node, edge"

func TestOptimizeStorage(t *testing.T) {
	t.Run("always vacuums node and edge", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectInvalidReindexArtifacts(conn)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("drops invalid reindex artifacts before vacuuming", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectInvalidReindexArtifacts(conn,
			graphIndex{schema: "public", name: "node_1_pkey_ccnew", sizeBytes: 8192},
			graphIndex{schema: `odd"schema`, name: "edge_2_kind_index_ccold1", sizeBytes: 16384},
		)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."node_1_pkey_ccnew"`, nil)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "odd""schema"."edge_2_kind_index_ccold1"`, nil)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("continues past failures and joins every error", func(t *testing.T) {
		var (
			ctx       = context.Background()
			conn      = newOptimizeStorageMockConn(t)
			dropErr   = errors.New("drop failed")
			vacuumErr = errors.New("vacuum failed")
		)

		expectInvalidReindexArtifacts(conn,
			graphIndex{schema: "public", name: "node_1_pkey_ccnew"},
			graphIndex{schema: "public", name: "edge_1_pkey_ccnew"},
		)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."node_1_pkey_ccnew"`, dropErr)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."edge_1_pkey_ccnew"`, nil)
		conn.ExpectExec(optimizeStorageVacuumStatement).
			WithArgs(pgx.QueryExecModeSimpleProtocol).
			WillReturnError(vacuumErr)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, dropErr)
		require.ErrorIs(t, err, vacuumErr)
		require.ErrorContains(t, err, `"public"."node_1_pkey_ccnew"`)
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("vacuums when listing invalid reindex artifacts fails", func(t *testing.T) {
		var (
			ctx     = context.Background()
			conn    = newOptimizeStorageMockConn(t)
			listErr = errors.New("list failed")
		)

		conn.ExpectQuery(sqlSelectInvalidReindexArtifacts).WillReturnError(listErr)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, listErr)
		require.ErrorContains(t, err, "list invalid reindex artifacts")
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("vacuums when scanning invalid reindex artifacts fails", func(t *testing.T) {
		var (
			ctx     = context.Background()
			conn    = newOptimizeStorageMockConn(t)
			scanErr = errors.New("row failed")
		)

		conn.ExpectQuery(sqlSelectInvalidReindexArtifacts).WillReturnRows(
			pgxmock.NewRows([]string{"nspname", "relname", "size"}).
				AddRow("public", "node_1_pkey_ccnew", int64(0)).
				RowError(0, scanErr),
		)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)

		require.ErrorIs(t, optimizeStorage(ctx, conn), scanErr)
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("returns cancellation without issuing statements", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		conn := newOptimizeStorageMockConn(t)

		require.ErrorIs(t, optimizeStorage(ctx, conn), context.Canceled)
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("stops issuing statements once cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mock := newOptimizeStorageMockConn(t)
		conn := &cancelOnExecConn{optimizeStorageConn: mock, cancel: cancel}

		expectInvalidReindexArtifacts(mock,
			graphIndex{schema: "public", name: "node_1_pkey_ccnew"},
			graphIndex{schema: "public", name: "edge_1_pkey_ccnew"},
		)
		expectIndexStatement(mock, `DROP INDEX CONCURRENTLY IF EXISTS "public"."node_1_pkey_ccnew"`, nil)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, unwrapJoined(err), 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("reports cancellation during the final phase once", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mock := newOptimizeStorageMockConn(t)
		conn := &cancelOnExecConn{optimizeStorageConn: mock, cancel: cancel}

		expectInvalidReindexArtifacts(mock)
		expectOptimizeStorageVacuum(mock, optimizeStorageVacuumStatement)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, unwrapJoined(err), 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does not duplicate cancellation returned by the final phase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mock := newOptimizeStorageMockConn(t)
		conn := &cancelOnExecConn{optimizeStorageConn: mock, cancel: cancel}

		expectInvalidReindexArtifacts(mock)
		mock.ExpectExec(optimizeStorageVacuumStatement).
			WithArgs(pgx.QueryExecModeSimpleProtocol).
			WillReturnError(context.Canceled)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, optimizeStorageVacuumStatement)
		require.Len(t, unwrapJoined(err), 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestInvalidReindexArtifactQueryFilters(t *testing.T) {
	for _, fragment := range []string{
		"inh.inhparent in ('node'::regclass, 'edge'::regclass)",
		"a.amname = 'btree'",
		"not x.indisvalid",
		"i.relname ~ '_cc(new|old)[0-9]*$'",
	} {
		require.Contains(t, sqlSelectInvalidReindexArtifacts, fragment)
	}
}

// cancelOnExecConn cancels its context after each Exec to simulate cancellation arriving mid-pass.
type cancelOnExecConn struct {
	optimizeStorageConn
	cancel context.CancelFunc
}

func (s *cancelOnExecConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	defer s.cancel()
	return s.optimizeStorageConn.Exec(ctx, sql, arguments...)
}

func unwrapJoined(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return joined.Unwrap()
	}

	return []error{err}
}

func expectInvalidReindexArtifacts(conn pgxmock.PgxConnIface, indexes ...graphIndex) {
	rows := pgxmock.NewRows([]string{"nspname", "relname", "size"})
	for _, index := range indexes {
		rows.AddRow(index.schema, index.name, index.sizeBytes)
	}

	conn.ExpectQuery(sqlSelectInvalidReindexArtifacts).WillReturnRows(rows)
}

func expectIndexStatement(conn pgxmock.PgxConnIface, stmt string, err error) {
	expectation := conn.ExpectExec(stmt).WithArgs(pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		expectation.WillReturnError(err)
	} else {
		expectation.WillReturnResult(pgxmock.NewResult("DROP INDEX", 0))
	}
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
