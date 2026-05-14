package pg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errSentinel is a fixed error used by wiring tests to assert that a real
// error value (not a wrapped variant) propagates out of the listing /
// measuring / executing call sites.
var errSentinel = errors.New("sentinel")

func newFakeRows(values ...[]any) *fakeRows { return &fakeRows{values: values} }

// TestOptimize_PgstattupleNotInstalled_NoOp covers the early-exit path: when
// the pre-flight extension probe returns false, optimize must not invoke any
// of the four maintenance phases.
func TestOptimize_PgstattupleNotInstalled_NoOp(t *testing.T) {
	db := &fakeDriver{
		queryRowRules: []queryRowRule{{
			match:    "pg_extension",
			response: func() pgx.Row { return &fakeRow{values: []any{false}} },
		}},
	}
	require.NoError(t, optimize(context.Background(), db))
	assert.Empty(t, db.execCalls, "no exec should run when pgstattuple is missing")
}

// TestOptimize_PgstattupleCheckError_ReturnsError covers the only path that
// makes optimize return a non-nil error: the pre-flight probe itself failing.
func TestOptimize_PgstattupleCheckError_ReturnsError(t *testing.T) {
	db := &fakeDriver{
		queryRowRules: []queryRowRule{{
			match:    "pg_extension",
			response: func() pgx.Row { return &fakeRow{err: errSentinel} },
		}},
	}
	err := optimize(context.Background(), db)
	require.Error(t, err)
	assert.ErrorIs(t, err, errSentinel)
}

// TestOptimize_AllPhasesRunWhenInstalled covers the happy path of the
// dispatcher: when pgstattuple is installed each of the four list queries
// must be issued. Phase bodies are exercised by the per-phase tests below.
func TestOptimize_AllPhasesRunWhenInstalled(t *testing.T) {
	var queriedFragments []string
	listResponder := func(fragment string) func() (pgx.Rows, error) {
		return func() (pgx.Rows, error) {
			queriedFragments = append(queriedFragments, fragment)
			return newFakeRows(), nil
		}
	}
	db := &fakeDriver{
		queryRowRules: []queryRowRule{{
			match:    "pg_extension",
			response: func() pgx.Row { return &fakeRow{values: []any{true}} },
		}},
		queryRules: []queryRule{
			{match: "_ccnew", response: listResponder("orphans")},
			{match: "a.amname = 'gin'", response: listResponder("gin")},
			{match: "pg_stat_user_tables", response: listResponder("vacuum")},
			{match: "a.amname = 'btree'", response: listResponder("btree")},
		},
	}
	require.NoError(t, optimize(context.Background(), db))
	assert.Equal(t, []string{"orphans", "gin", "vacuum", "btree"}, queriedFragments,
		"all four phases must dispatch their list query in the documented order")
}

// TestCleanupOrphanedReindexArtifacts covers the orphan-cleanup phase wiring:
// list error short-circuits, empty list logs and exits, listed orphans each
// receive a DROP, and per-orphan exec failures do not abort the loop.
func TestCleanupOrphanedReindexArtifacts(t *testing.T) {
	t.Run("list error returns without exec", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match:    "_ccnew",
			response: func() (pgx.Rows, error) { return nil, errSentinel },
		}}}
		cleanupOrphanedReindexArtifacts(context.Background(), db)
		assert.Empty(t, db.execCalls)
	})

	t.Run("empty list issues no exec", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match:    "_ccnew",
			response: func() (pgx.Rows, error) { return newFakeRows(), nil },
		}}}
		cleanupOrphanedReindexArtifacts(context.Background(), db)
		assert.Empty(t, db.execCalls)
	})

	t.Run("each orphan dropped with sanitized identifier", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match: "_ccnew",
			response: func() (pgx.Rows, error) {
				return newFakeRows(
					[]any{"graph", "edge_1_pkey_ccnew"},
					[]any{"graph", "node_2_pkey_ccnew1"},
				), nil
			},
		}}}
		cleanupOrphanedReindexArtifacts(context.Background(), db)
		require.Len(t, db.execCalls, 2)
		for _, call := range db.execCalls {
			assert.True(t, strings.HasPrefix(call.sql, "drop index concurrently if exists "),
				"orphan cleanup must use DROP INDEX CONCURRENTLY IF EXISTS, got %q", call.sql)
		}
	})

	t.Run("loop continues past per-orphan exec error", func(t *testing.T) {
		var execAttempts int
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "_ccnew",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						[]any{"graph", "first_ccnew"},
						[]any{"graph", "second_ccnew"},
					), nil
				},
			}},
			execRules: []execRule{{
				match:    "first_ccnew",
				response: func() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, errSentinel },
			}},
		}
		cleanupOrphanedReindexArtifacts(context.Background(), db)
		for _, call := range db.execCalls {
			if strings.Contains(call.sql, "_ccnew") {
				execAttempts++
			}
		}
		assert.Equal(t, 2, execAttempts, "second orphan must be attempted after first fails")
	})
}

// indexRowValues mirrors the column order of sqlSelectGraphPartitionBtreeIndexes
// and sqlSelectGraphPartitionGinIndexes so the wiring tests below can register
// listing rows compactly.
func indexRowValues(oid uint32, schema, table, index string, sizeBytes int64) []any {
	return []any{oid, schema, table, index, sizeBytes}
}
