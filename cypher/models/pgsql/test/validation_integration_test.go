//go:build manual_integration

package test

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

const (
	PGConnectionStringEV = "PG_CONNECTION_STRING"

	emptyFrontier = `insert into next_front (root_id, next_id, depth, satisfied, is_cycle, path)
select 1::int8, 1::int8, 1::int4, false, false, array []::int8[]
where false;`
)

func nextFrontValues(rows ...string) string {
	return fmt.Sprintf(
		"insert into next_front (root_id, next_id, depth, satisfied, is_cycle, path) values %s;",
		strings.Join(rows, ", "),
	)
}

func pairFilterValues(rows ...string) string {
	return fmt.Sprintf(
		"insert into traversal_pair_filter (root_id, terminal_id) values %s;",
		strings.Join(rows, ", "),
	)
}

func TestTranslationTestCases(t *testing.T) {
	var (
		testCtx, done   = context.WithCancel(context.Background())
		pgConnectionStr = os.Getenv(PGConnectionStringEV)
	)

	defer done()

	require.NotEmpty(t, pgConnectionStr)

	// pg.NewPool installs the AfterConnect and AfterRelease hooks that register
	// the composite types (nodecomposite, edgecomposite, pathcomposite) on every
	// pool connection. Using pgxpool.New directly omits these hooks; after
	// AssertSchema calls pool.Reset(), new connections would return composite
	// values as raw []uint8 instead of map[string]any, causing scan failures.
	if pgxPool, err := pg.NewPool(drivers.DatabaseConfiguration{Connection: pgConnectionStr}); err != nil {
		t.Fatalf("Failed opening database connection: %v", err)
	} else if connection, err := dawgs.Open(context.TODO(), pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pgxPool,
	}); err != nil {
		t.Fatalf("Failed opening database connection: %v", err)
	} else if pgConnection, typeOK := connection.(*pg.Driver); !typeOK {
		t.Fatalf("Invalid connection type: %T", connection)
	} else {
		defer connection.Close(testCtx)

		graphSchema := graph.Schema{
			Graphs: []graph.Graph{{
				Name: "test",
				Nodes: graph.Kinds{
					graph.StringKind("NodeKind1"),
					graph.StringKind("NodeKind2"),
				},
				Edges: graph.Kinds{
					graph.StringKind("EdgeKind1"),
					graph.StringKind("EdgeKind2"),
				},
			}},
			DefaultGraph: graph.Graph{
				Name: "test",
			},
		}

		if err := connection.AssertSchema(testCtx, graphSchema); err != nil {
			t.Fatalf("Failed asserting graph schema: %v", err)
		}

		var (
			casesRun     = 0
			cassesPassed = 0
		)

		if testCases, err := ReadTranslationTestCases(); err != nil {
			t.Fatal(err)
		} else {
			for _, testCase := range testCases {
				passed := t.Run(testCase.Name, func(t *testing.T) {
					defer func() {
						if err := recover(); err != nil {
							debug.PrintStack()
							t.Error(err)
						}
					}()

					testCase.AssertLive(testCtx, t, pgConnection)
				})

				if passed {
					cassesPassed += 1
				}

				casesRun += 1
			}
		}

		fmt.Printf("Validated %d test cases with %d passing\n", casesRun, cassesPassed)
	}
}

func TestBidirectionalASPHarnessOverloads(t *testing.T) {
	var (
		testCtx, done   = context.WithCancel(context.Background())
		pgConnectionStr = os.Getenv(PGConnectionStringEV)
	)

	defer done()

	require.NotEmpty(t, pgConnectionStr)

	pgxPool, err := pg.NewPool(drivers.DatabaseConfiguration{Connection: pgConnectionStr})
	require.NoError(t, err)
	defer pgxPool.Close()

	connection, err := dawgs.Open(context.TODO(), pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pgxPool,
	})
	require.NoError(t, err)
	defer connection.Close(testCtx)

	graphSchema := graph.Schema{
		Graphs: []graph.Graph{{
			Name: "test",
			Nodes: graph.Kinds{
				graph.StringKind("NodeKind1"),
				graph.StringKind("NodeKind2"),
			},
			Edges: graph.Kinds{
				graph.StringKind("EdgeKind1"),
				graph.StringKind("EdgeKind2"),
			},
		}},
		DefaultGraph: graph.Graph{
			Name: "test",
		},
	}

	require.NoError(t, connection.AssertSchema(testCtx, graphSchema))

	t.Run("array overload does not require pair filter", func(t *testing.T) {
		tx, err := pgxPool.Begin(testCtx)
		require.NoError(t, err)
		defer tx.Rollback(testCtx)

		var count int
		require.NoError(t, tx.QueryRow(testCtx,
			"select count(*) from bidirectional_asp_harness($1::text, $2::text, $3::text, $4::text, 1, array []::int8[], array []::int8[])",
			emptyFrontier,
			emptyFrontier,
			emptyFrontier,
			emptyFrontier,
		).Scan(&count))
		require.Equal(t, 0, count)
	})

	t.Run("pair filter constrains midpoint meets", func(t *testing.T) {
		tx, err := pgxPool.Begin(testCtx)
		require.NoError(t, err)
		defer tx.Rollback(testCtx)

		var (
			graphID int64
			kindID  int16
		)

		require.NoError(t, tx.QueryRow(testCtx, "select id from graph where name = 'test'").Scan(&graphID))
		require.NoError(t, tx.QueryRow(testCtx, "select id from kind where name = 'EdgeKind1'").Scan(&kindID))

		_, err = tx.Exec(testCtx,
			"insert into edge (graph_id, start_id, end_id, kind_id, properties) values ($1, 10, 10, $2, '{}'::jsonb) on conflict do nothing",
			graphID,
			kindID,
		)
		require.NoError(t, err)

		forwardPrimer := nextFrontValues(
			"(1::int8, 10::int8, 1::int4, false, false, array [101]::int8[])",
			"(3::int8, 10::int8, 1::int4, false, false, array [103]::int8[])",
		)
		backwardPrimer := nextFrontValues(
			"(2::int8, 10::int8, 1::int4, false, false, array [202]::int8[])",
			"(4::int8, 10::int8, 1::int4, false, false, array [204]::int8[])",
		)
		pairFilter := pairFilterValues("(1::int8, 2::int8)")

		rows, err := tx.Query(testCtx,
			"select root_id, next_id from bidirectional_asp_harness($1::text, $2::text, $3::text, $4::text, 4, ''::text, ''::text, $5::text) order by root_id, next_id",
			forwardPrimer,
			emptyFrontier,
			backwardPrimer,
			emptyFrontier,
			pairFilter,
		)
		require.NoError(t, err)
		defer rows.Close()

		var results [][2]int64
		for rows.Next() {
			var result [2]int64
			require.NoError(t, rows.Scan(&result[0], &result[1]))
			results = append(results, result)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, [][2]int64{{1, 2}}, results)
	})

	t.Run("pair-aware all shortest paths harness resolves all explicit pairs", func(t *testing.T) {
		tx, err := pgxPool.Begin(testCtx)
		require.NoError(t, err)
		defer tx.Rollback(testCtx)

		var (
			graphID int64
			kindID  int16
		)

		require.NoError(t, tx.QueryRow(testCtx, "select id from graph where name = 'test'").Scan(&graphID))
		require.NoError(t, tx.QueryRow(testCtx, "select id from kind where name = 'EdgeKind1'").Scan(&kindID))

		_, err = tx.Exec(testCtx,
			"insert into edge (graph_id, start_id, end_id, kind_id, properties) values ($1, 30, 30, $2, '{}'::jsonb) on conflict do nothing",
			graphID,
			kindID,
		)
		require.NoError(t, err)

		forwardPrimer := nextFrontValues(
			"(1::int8, 2::int8, 1::int4, true, false, array [102]::int8[])",
			"(1::int8, 2::int8, 1::int4, true, false, array [103]::int8[])",
			"(3::int8, 30::int8, 1::int4, false, false, array [330]::int8[])",
		)
		backwardPrimer := nextFrontValues(
			"(4::int8, 30::int8, 1::int4, false, false, array [304]::int8[])",
			"(4::int8, 30::int8, 1::int4, false, false, array [305]::int8[])",
		)
		pairFilter := pairFilterValues(
			"(1::int8, 2::int8)",
			"(3::int8, 4::int8)",
		)

		rows, err := tx.Query(testCtx,
			"select root_id, next_id, depth, path from bidirectional_asp_harness($1::text, $2::text, $3::text, $4::text, 4, ''::text, ''::text, $5::text) order by root_id, next_id, path",
			forwardPrimer,
			emptyFrontier,
			backwardPrimer,
			emptyFrontier,
			pairFilter,
		)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			rootID int64
			nextID int64
			depth  int32
			path   []int64
		}

		var results []result
		for rows.Next() {
			var next result
			require.NoError(t, rows.Scan(&next.rootID, &next.nextID, &next.depth, &next.path))
			results = append(results, next)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []result{{
			rootID: 1,
			nextID: 2,
			depth:  1,
			path:   []int64{102},
		}, {
			rootID: 1,
			nextID: 2,
			depth:  1,
			path:   []int64{103},
		}, {
			rootID: 3,
			nextID: 4,
			depth:  2,
			path:   []int64{330, 304},
		}, {
			rootID: 3,
			nextID: 4,
			depth:  2,
			path:   []int64{330, 305},
		}}, results)
	})

	t.Run("shortest path harnesses avoid output column ambiguity", func(t *testing.T) {
		frontier := nextFrontValues("(1::int8, 2::int8, 1::int4, false, false, array [101]::int8[])")

		var unidirectionalCount int
		require.NoError(t, pgxPool.QueryRow(testCtx,
			"select count(*) from unidirectional_sp_harness($1::text, $2::text, 1, array []::int8[], array []::int8[])",
			frontier,
			emptyFrontier,
		).Scan(&unidirectionalCount))
		require.Equal(t, 0, unidirectionalCount)

		var bidirectionalCount int
		require.NoError(t, pgxPool.QueryRow(testCtx,
			"select count(*) from bidirectional_sp_harness($1::text, $2::text, $3::text, $4::text, 1, array []::int8[], array []::int8[])",
			frontier,
			emptyFrontier,
			emptyFrontier,
			emptyFrontier,
		).Scan(&bidirectionalCount))
		require.Equal(t, 0, bidirectionalCount)
	})

	t.Run("pair-aware shortest path harness resolves all explicit pairs", func(t *testing.T) {
		tx, err := pgxPool.Begin(testCtx)
		require.NoError(t, err)
		defer tx.Rollback(testCtx)

		forwardPrimer := nextFrontValues(
			"(1::int8, 2::int8, 1::int4, true, false, array [102]::int8[])",
			"(3::int8, 30::int8, 1::int4, false, false, array [330]::int8[])",
		)
		backwardPrimer := nextFrontValues("(4::int8, 30::int8, 1::int4, false, false, array [304]::int8[])")
		pairFilter := pairFilterValues(
			"(1::int8, 2::int8)",
			"(3::int8, 4::int8)",
		)

		rows, err := tx.Query(testCtx,
			"select root_id, next_id, depth, path from bidirectional_sp_harness($1::text, $2::text, $3::text, $4::text, 4, ''::text, ''::text, $5::text) order by root_id, next_id",
			forwardPrimer,
			emptyFrontier,
			backwardPrimer,
			emptyFrontier,
			pairFilter,
		)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			rootID int64
			nextID int64
			depth  int32
			path   []int64
		}

		var results []result
		for rows.Next() {
			var next result
			require.NoError(t, rows.Scan(&next.rootID, &next.nextID, &next.depth, &next.path))
			results = append(results, next)
		}
		require.NoError(t, rows.Err())

		require.Equal(t, []result{{
			rootID: 1,
			nextID: 2,
			depth:  1,
			path:   []int64{102},
		}, {
			rootID: 3,
			nextID: 4,
			depth:  2,
			path:   []int64{330, 304},
		}}, results)
	})
}
