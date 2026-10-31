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

// TestOptimizeStorage verifies optimization vacuums both graph storage parents in one statement.
func TestOptimizeStorage(t *testing.T) {
	t.Run("always vacuums node and edge", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectGraphIndexes(conn, sqlSelectInvalidReindexArtifacts)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)
		expectGraphIndexes(conn, sqlSelectBtreeIndexes)

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("drops invalid reindex artifacts, vacuums, then rebuilds btree indexes in listed order", func(t *testing.T) {
		ctx := context.Background()
		conn := newOptimizeStorageMockConn(t)

		expectGraphIndexes(conn, sqlSelectInvalidReindexArtifacts,
			graphIndex{schema: "public", name: "node_1_pkey_ccnew", sizeBytes: 8192},
			graphIndex{schema: `odd"schema`, name: "edge_2_kind_index_ccold1", sizeBytes: 16384},
		)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."node_1_pkey_ccnew"`, nil)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "odd""schema"."edge_2_kind_index_ccold1"`, nil)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)
		expectGraphIndexes(conn, sqlSelectBtreeIndexes,
			graphIndex{schema: "public", name: "edge_1_kind_index", sizeBytes: 8192},
			graphIndex{schema: `odd"schema`, name: "node_1_pkey", sizeBytes: 16384},
		)
		expectIndexStatement(conn, `REINDEX INDEX CONCURRENTLY "public"."edge_1_kind_index"`, nil)
		expectIndexStatement(conn, `REINDEX INDEX CONCURRENTLY "odd""schema"."node_1_pkey"`, nil)

		require.NoError(t, optimizeStorage(ctx, conn))
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("continues past failures and joins every error", func(t *testing.T) {
		var (
			ctx        = context.Background()
			conn       = newOptimizeStorageMockConn(t)
			dropErr    = errors.New("drop failed")
			vacuumErr  = errors.New("vacuum failed")
			reindexErr = errors.New("reindex failed")
		)

		expectGraphIndexes(conn, sqlSelectInvalidReindexArtifacts,
			graphIndex{schema: "public", name: "node_1_pkey_ccnew"},
			graphIndex{schema: "public", name: "edge_1_pkey_ccnew"},
		)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."node_1_pkey_ccnew"`, dropErr)
		expectIndexStatement(conn, `DROP INDEX CONCURRENTLY IF EXISTS "public"."edge_1_pkey_ccnew"`, nil)
		conn.ExpectExec(optimizeStorageVacuumStatement).
			WithArgs(pgx.QueryExecModeSimpleProtocol).
			WillReturnError(vacuumErr)
		expectGraphIndexes(conn, sqlSelectBtreeIndexes,
			graphIndex{schema: "public", name: "node_1_pkey"},
			graphIndex{schema: "public", name: "edge_1_pkey"},
		)
		expectIndexStatement(conn, `REINDEX INDEX CONCURRENTLY "public"."node_1_pkey"`, reindexErr)
		expectIndexStatement(conn, `REINDEX INDEX CONCURRENTLY "public"."edge_1_pkey"`, nil)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, dropErr)
		require.ErrorIs(t, err, vacuumErr)
		require.ErrorIs(t, err, reindexErr)
		require.ErrorContains(t, err, `"public"."node_1_pkey_ccnew"`)
		require.ErrorContains(t, err, `REINDEX INDEX CONCURRENTLY "public"."node_1_pkey"`)
		require.NoError(t, conn.ExpectationsWereMet())
	})

	t.Run("reports a failure to list btree indexes", func(t *testing.T) {
		var (
			ctx     = context.Background()
			conn    = newOptimizeStorageMockConn(t)
			listErr = errors.New("list failed")
		)

		expectGraphIndexes(conn, sqlSelectInvalidReindexArtifacts)
		expectOptimizeStorageVacuum(conn, optimizeStorageVacuumStatement)
		conn.ExpectQuery(sqlSelectBtreeIndexes).WillReturnError(listErr)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, listErr)
		require.ErrorContains(t, err, "list btree indexes")
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
		expectGraphIndexes(conn, sqlSelectBtreeIndexes)

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
		expectGraphIndexes(conn, sqlSelectBtreeIndexes)

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

		expectGraphIndexes(mock, sqlSelectInvalidReindexArtifacts,
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

		const reindexStatement = `REINDEX INDEX CONCURRENTLY "public"."node_1_pkey"`

		mock := newOptimizeStorageMockConn(t)
		conn := &cancelOnExecConn{optimizeStorageConn: mock, cancel: cancel, onStatement: reindexStatement}

		expectGraphIndexes(mock, sqlSelectInvalidReindexArtifacts)
		expectOptimizeStorageVacuum(mock, optimizeStorageVacuumStatement)
		expectGraphIndexes(mock, sqlSelectBtreeIndexes, graphIndex{schema: "public", name: "node_1_pkey"})
		expectIndexStatement(mock, reindexStatement, nil)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, unwrapJoined(err), 1)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("does not duplicate cancellation returned by the final phase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const reindexStatement = `REINDEX INDEX CONCURRENTLY "public"."node_1_pkey"`

		mock := newOptimizeStorageMockConn(t)
		conn := &cancelOnExecConn{optimizeStorageConn: mock, cancel: cancel, onStatement: reindexStatement}

		expectGraphIndexes(mock, sqlSelectInvalidReindexArtifacts)
		expectOptimizeStorageVacuum(mock, optimizeStorageVacuumStatement)
		expectGraphIndexes(mock, sqlSelectBtreeIndexes, graphIndex{schema: "public", name: "node_1_pkey"})
		expectIndexStatement(mock, reindexStatement, context.Canceled)

		err := optimizeStorage(ctx, conn)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, reindexStatement)
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

func TestBtreeIndexQueryFilters(t *testing.T) {
	for _, fragment := range []string{
		"inh.inhparent in ('node'::regclass, 'edge'::regclass)",
		"a.amname = 'btree'",
		"and x.indisvalid",
		"order by pg_relation_size(i.oid),",
	} {
		require.Contains(t, sqlSelectBtreeIndexes, fragment)
	}
}

// cancelOnExecConn cancels its context after each Exec, or only after onStatement when set, to simulate cancellation
// arriving mid-pass.
type cancelOnExecConn struct {
	optimizeStorageConn
	cancel      context.CancelFunc
	onStatement string
}

func (s *cancelOnExecConn) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if s.onStatement == "" || sql == s.onStatement {
		defer s.cancel()
	}

	return s.optimizeStorageConn.Exec(ctx, sql, arguments...)
}

func unwrapJoined(err error) []error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return joined.Unwrap()
	}

	return []error{err}
}

func expectGraphIndexes(conn pgxmock.PgxConnIface, sql string, indexes ...graphIndex) {
	rows := pgxmock.NewRows([]string{"nspname", "relname", "size"})
	for _, index := range indexes {
		rows.AddRow(index.schema, index.name, index.sizeBytes)
	}

	conn.ExpectQuery(sql).WillReturnRows(rows)
}

func expectIndexStatement(conn pgxmock.PgxConnIface, stmt string, err error) {
	expectation := conn.ExpectExec(stmt).WithArgs(pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		expectation.WillReturnError(err)
	} else {
		expectation.WillReturnResult(pgxmock.NewResult("OK", 0))
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
