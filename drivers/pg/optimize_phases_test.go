package pg

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlushGinPendingLists exercises the GIN flush phase wiring: scan error,
// per-index measure error, threshold filtering, and exec error continuation.
func TestFlushGinPendingLists(t *testing.T) {
	t.Run("list error short-circuits", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match:    "a.amname = 'gin'",
			response: func() (pgx.Rows, error) { return nil, errSentinel },
		}}}
		flushGinPendingLists(context.Background(), db)
		assert.Empty(t, db.execCalls)
	})

	t.Run("measure error skips that index but flush continues", func(t *testing.T) {
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'gin'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "ng_idx_1", 1024),
						indexRowValues(2, "graph", "node_2", "ng_idx_2", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatginindex", response: func() pgx.Row { return &fakeRow{err: errSentinel} }},
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold), int64(99)}}
				}},
			},
		}
		flushGinPendingLists(context.Background(), db)
		require.Len(t, db.execCalls, 1, "only the second index, which scanned successfully and crossed the threshold, should be flushed")
		assert.True(t, strings.Contains(db.execCalls[0].sql, "gin_clean_pending_list"))
	})

	t.Run("only flagged candidates exec; below-threshold ignored", func(t *testing.T) {
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'gin'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "ng_idx_1", 1024),
						indexRowValues(2, "graph", "node_2", "ng_idx_2", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold - 1), int64(0)}}
				}},
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold + 100), int64(50)}}
				}},
			},
		}
		flushGinPendingLists(context.Background(), db)
		require.Len(t, db.execCalls, 1)
	})

	t.Run("loop continues past per-candidate exec error", func(t *testing.T) {
		var execAttempts int
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'gin'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "ng_idx_1", 1024),
						indexRowValues(2, "graph", "node_2", "ng_idx_2", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold), int64(1)}}
				}},
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold), int64(1)}}
				}},
			},
			execRules: []execRule{
				{match: "gin_clean_pending_list", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, errSentinel }},
				{match: "gin_clean_pending_list", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, nil }},
			},
		}
		flushGinPendingLists(context.Background(), db)
		assert.Equal(t, 2, execAttempts)
	})

	t.Run("ctx cancellation between candidates aborts remaining flushes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'gin'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "ng_idx_1", 1024),
						indexRowValues(2, "graph", "node_2", "ng_idx_2", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold), int64(0)}}
				}},
				{match: "pgstatginindex", response: func() pgx.Row {
					return &fakeRow{values: []any{int64(ginPendingPagesFlushThreshold), int64(0)}}
				}},
			},
		}
		flushGinPendingLists(ctx, db)
		assert.Empty(t, db.execCalls, "cancelled ctx must prevent any flush exec")
	})
}

// vacuumStatsValues mirrors the column order of sqlSelectGraphPartitionVacuumStats
// so the wiring tests can register stats rows compactly.
func vacuumStatsValues(schema, table string, live, dead, mod int64, lastAnalyzed *time.Time) []any {
	return []any{schema, table, live, dead, mod, lastAnalyzed}
}

// TestVacuumGraphPartitions exercises the vacuum phase wiring: scan error,
// threshold filtering, exec error continuation, and ctx cancellation.
func TestVacuumGraphPartitions(t *testing.T) {
	stale := time.Now().Add(-(analyzeStalenessWindow + time.Hour))

	t.Run("list error short-circuits", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match:    "pg_stat_user_tables",
			response: func() (pgx.Rows, error) { return nil, errSentinel },
		}}}
		vacuumGraphPartitions(context.Background(), db)
		assert.Empty(t, db.execCalls)
	})

	t.Run("only flagged partitions exec; healthy partitions ignored", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match: "pg_stat_user_tables",
			response: func() (pgx.Rows, error) {
				return newFakeRows(
					vacuumStatsValues("graph", "node_1", 1_000_000, 5_000, 1_000, &stale),
					vacuumStatsValues("graph", "edge_1", 40_000, vacuumDeadTupleAbsoluteFloor, 0, &stale),
				), nil
			},
		}}}
		vacuumGraphPartitions(context.Background(), db)
		require.Len(t, db.execCalls, 1)
		assert.True(t, strings.Contains(db.execCalls[0].sql, "vacuum (analyze)"))
		assert.True(t, strings.Contains(db.execCalls[0].sql, `"edge_1"`))
	})

	t.Run("loop continues past per-partition exec error", func(t *testing.T) {
		var execAttempts int
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "pg_stat_user_tables",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						vacuumStatsValues("graph", "node_1", 40_000, vacuumDeadTupleAbsoluteFloor, 0, &stale),
						vacuumStatsValues("graph", "edge_1", 40_000, vacuumDeadTupleAbsoluteFloor, 0, &stale),
					), nil
				},
			}},
			execRules: []execRule{
				{match: "vacuum (analyze)", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, errSentinel }},
				{match: "vacuum (analyze)", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, nil }},
			},
		}
		vacuumGraphPartitions(context.Background(), db)
		assert.Equal(t, 2, execAttempts)
	})

	t.Run("ctx cancellation between partitions aborts remaining vacuums", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		db := &fakeDriver{queryRules: []queryRule{{
			match: "pg_stat_user_tables",
			response: func() (pgx.Rows, error) {
				return newFakeRows(
					vacuumStatsValues("graph", "node_1", 40_000, vacuumDeadTupleAbsoluteFloor, 0, &stale),
					vacuumStatsValues("graph", "edge_1", 40_000, vacuumDeadTupleAbsoluteFloor, 0, &stale),
				), nil
			},
		}}}
		vacuumGraphPartitions(ctx, db)
		assert.Empty(t, db.execCalls)
	})
}

// TestReindexBloatedBtreeIndexes exercises the btree reindex phase wiring:
// scan error, per-index measure error, threshold filtering, smallest-first
// ordering, exec error continuation, and ctx cancellation.
func TestReindexBloatedBtreeIndexes(t *testing.T) {
	bloated := func() pgx.Row { return &fakeRow{values: []any{30.0, 0.0}} }
	healthy := func() pgx.Row { return &fakeRow{values: []any{85.0, 5.0}} }

	t.Run("list error short-circuits", func(t *testing.T) {
		db := &fakeDriver{queryRules: []queryRule{{
			match:    "a.amname = 'btree'",
			response: func() (pgx.Rows, error) { return nil, errSentinel },
		}}}
		reindexBloatedBtreeIndexes(context.Background(), db)
		assert.Empty(t, db.execCalls)
	})

	t.Run("measure error skips that index but reindex continues", func(t *testing.T) {
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'btree'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "node_1_pkey", 1024),
						indexRowValues(2, "graph", "node_2", "node_2_pkey", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatindex", response: func() pgx.Row { return &fakeRow{err: errSentinel} }},
				{match: "pgstatindex", response: bloated},
			},
		}
		reindexBloatedBtreeIndexes(context.Background(), db)
		require.Len(t, db.execCalls, 1)
		assert.True(t, strings.Contains(db.execCalls[0].sql, "reindex index concurrently"))
	})

	t.Run("only flagged candidates exec; healthy indexes ignored", func(t *testing.T) {
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'btree'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "node_1_pkey", 1024),
						indexRowValues(2, "graph", "node_2", "node_2_pkey", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatindex", response: healthy},
				{match: "pgstatindex", response: bloated},
			},
		}
		reindexBloatedBtreeIndexes(context.Background(), db)
		require.Len(t, db.execCalls, 1)
		assert.True(t, strings.Contains(db.execCalls[0].sql, `"node_2_pkey"`))
	})

	// candidates are processed smallest-first so cancellation mid-pass leaves
	// the maximum number of rebuilds completed; the larger size_bytes index
	// must trail the smaller one in the recorded exec order.
	t.Run("flagged candidates execute smallest-first", func(t *testing.T) {
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'btree'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "big_idx", 9999),
						indexRowValues(2, "graph", "node_2", "small_idx", 100),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatindex", response: bloated},
				{match: "pgstatindex", response: bloated},
			},
		}
		reindexBloatedBtreeIndexes(context.Background(), db)
		require.Len(t, db.execCalls, 2)
		assert.True(t, strings.Contains(db.execCalls[0].sql, `"small_idx"`),
			"smaller index must be reindexed first; got %q", db.execCalls[0].sql)
		assert.True(t, strings.Contains(db.execCalls[1].sql, `"big_idx"`))
	})

	t.Run("loop continues past per-candidate exec error", func(t *testing.T) {
		var execAttempts int
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'btree'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "node_1_pkey", 1024),
						indexRowValues(2, "graph", "node_2", "node_2_pkey", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatindex", response: bloated},
				{match: "pgstatindex", response: bloated},
			},
			execRules: []execRule{
				{match: "reindex index concurrently", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, errSentinel }},
				{match: "reindex index concurrently", response: func() (pgconn.CommandTag, error) { execAttempts++; return pgconn.CommandTag{}, nil }},
			},
		}
		reindexBloatedBtreeIndexes(context.Background(), db)
		assert.Equal(t, 2, execAttempts)
	})

	t.Run("ctx cancellation between candidates aborts remaining reindexes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		db := &fakeDriver{
			queryRules: []queryRule{{
				match: "a.amname = 'btree'",
				response: func() (pgx.Rows, error) {
					return newFakeRows(
						indexRowValues(1, "graph", "node_1", "node_1_pkey", 1024),
						indexRowValues(2, "graph", "node_2", "node_2_pkey", 2048),
					), nil
				},
			}},
			queryRowRules: []queryRowRule{
				{match: "pgstatindex", response: bloated},
				{match: "pgstatindex", response: bloated},
			},
		}
		reindexBloatedBtreeIndexes(ctx, db)
		assert.Empty(t, db.execCalls, "cancelled ctx must prevent any reindex exec")
	})
}
