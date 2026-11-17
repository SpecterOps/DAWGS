//go:build manual_integration

package query

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

const (
	// compactDAGConnectionStringEnv selects the backend for the temporary SQL fixture.
	compactDAGConnectionStringEnv = "CONNECTION_STRING"

	// compactDAGStateLimitSetting controls how many records the fixture may admit.
	compactDAGStateLimitSetting = "dawgs.traversal_state_limit"

	// compactDAGByteLimitSetting controls the fixture's cumulative estimated record bytes.
	compactDAGByteLimitSetting = "dawgs.traversal_byte_limit"

	// compactDAGResourceSQLState identifies an exhausted or invalid execution budget.
	compactDAGResourceSQLState = "54000"

	// compactDAGArgumentSQLState identifies a shortest-path contract the kernel does not support.
	compactDAGArgumentSQLState = "22023"

	// compactDAGSnapshotSQLState identifies an invocation without a stable graph snapshot.
	compactDAGSnapshotSQLState = "25001"

	// compactDAGFixtureSetupSQL creates the temporary edge table and initializes independent admission budgets.
	compactDAGFixtureSetupSQL = `
		create temporary table edge (
			id bigint not null, graph_id integer not null,
			start_id bigint not null, end_id bigint not null, kind_id smallint not null,
			primary key (id, graph_id));
		select set_config('dawgs.traversal_token', 'compact-dag-v2-fixture', true);
		select set_config('dawgs.traversal_state_limit', '100000', true);
		select set_config('dawgs.traversal_byte_limit', '67108864', true);`

	// compactDAGSelectPathsSQL returns complete ordered witnesses for one open or explicitly bounded endpoint pair.
	compactDAGSelectPathsSQL = `select path from pg_temp.all_shortest_paths_dag_v2(1,$1,$2,1,$3,$4,$5,$6) order by path`

	// compactDAGDiamondFixtureSQL seeds tied paths, parallel kinds, cycles, and an edge in another graph.
	compactDAGDiamondFixtureSQL = `insert into edge values
		(1,1,10,11,1),(2,1,10,12,1),(3,1,11,13,1),
		(4,1,12,13,1),(5,1,11,13,2),(6,1,13,10,1),
		(7,2,10,13,1),(8,1,10,10,1);`

	// compactDAGLongChainFixtureSQL seeds a 129-relationship chain beyond the historical compact-selection ceiling.
	compactDAGLongChainFixtureSQL = `insert into edge select 10000+i,1,1000+i,1001+i,1 from generate_series(0,128) i`

	// compactDAGOneEdgeFixtureSQL seeds the one-hop graph used to count every admission phase.
	compactDAGOneEdgeFixtureSQL = `insert into edge values (1,1,10,11,1)`

	// compactDAGSetSettingSQL changes one transaction-local traversal setting without interpolating values.
	compactDAGSetSettingSQL = `select set_config($1,$2,true)`

	// compactDAGCountOneHopSQL fully consumes the one-hop compact search so late admission errors are observed.
	compactDAGCountOneHopSQL = `select count(*) from pg_temp.all_shortest_paths_dag_v2(1,10,11,1,0,false,array[]::smallint[],false)`

	// compactDAGExactBudgetFixtureSQL seeds one edge and admits exactly its seven states and 592 estimated bytes.
	compactDAGExactBudgetFixtureSQL = `
		insert into edge values (1,1,10,11,1);
		select set_config('dawgs.traversal_state_limit','7',true);
		select set_config('dawgs.traversal_byte_limit','592',true);`

	// compactDAGReadCountersSQL reads the cumulative counters shared by compact and legacy admission.
	compactDAGReadCountersSQL = `select current_setting('dawgs.traversal_states')::bigint,current_setting('dawgs.traversal_bytes')::bigint`

	// compactDAGNextQueryTokenSQL starts an independent logical query on the same physical session.
	compactDAGNextQueryTokenSQL = `select set_config('dawgs.traversal_token','compact-dag-v2-next-query',true)`

	// compactDAGRejectMinimumSQL calls the kernel with an unsupported zero minimum.
	compactDAGRejectMinimumSQL = `select count(*) from pg_temp.all_shortest_paths_dag_v2(1,10,11,0,0,false,array[]::smallint[],false)`

	// compactDAGRejectMaximumSQL calls the kernel with a negative explicit maximum.
	compactDAGRejectMaximumSQL = `select count(*) from pg_temp.all_shortest_paths_dag_v2(1,10,11,1,-1,true,array[]::smallint[],false)`

	// compactDAGRejectSelfSQL calls the kernel with equal endpoints that require separate cycle semantics.
	compactDAGRejectSelfSQL = `select count(*) from pg_temp.all_shortest_paths_dag_v2(1,10,10,1,0,false,array[]::smallint[],false)`

	// compactDAGRejectNegativeWorkSQL attempts to credit a negative state delta.
	compactDAGRejectNegativeWorkSQL = `select pg_temp.dawgs_admit_traversal_work_v2(-1,0,'test')`

	// compactDAGRejectMinimumStateSQL exercises the smallest int64 state delta before subtraction.
	compactDAGRejectMinimumStateSQL = `select pg_temp.dawgs_admit_traversal_work_v2(-9223372036854775808,0,'test')`

	// compactDAGRejectMinimumBytesSQL exercises the smallest int64 byte delta before subtraction.
	compactDAGRejectMinimumBytesSQL = `select pg_temp.dawgs_admit_traversal_work_v2(0,-9223372036854775808,'test')`

	// compactDAGRejectOversizedWorkSQL requests more states than the configured query budget.
	compactDAGRejectOversizedWorkSQL = `select pg_temp.dawgs_admit_traversal_work_v2(9223372036854775807,0,'test')`

	// compactDAGSharedAdmissionSQL charges legacy trail work followed by compact endpoint work under one token.
	compactDAGSharedAdmissionSQL = `
		select pg_temp.dawgs_admit_traversal_v1(2,77);
		select pg_temp.dawgs_admit_traversal_work_v2(2,64,'endpoint');`

	// compactDAGLimitSharedStatesSQL caps the shared budget at the work already admitted.
	compactDAGLimitSharedStatesSQL = `select set_config('dawgs.traversal_state_limit','3',true)`

	// compactDAGChargeLegacySQL requests one more legacy state after compact admission filled the budget.
	compactDAGChargeLegacySQL = `select pg_temp.dawgs_admit_traversal_v1(0,78)`

	// compactDAGInt64BoundarySQL admits exactly the largest representable state and byte totals.
	compactDAGInt64BoundarySQL = `
		select set_config('dawgs.traversal_state_limit','9223372036854775807',true);
		select set_config('dawgs.traversal_byte_limit','9223372036854775807',true);
		select pg_temp.dawgs_admit_traversal_work_v2(9223372036854775807,9223372036854775807,'boundary');`

	// compactDAGChargePastBoundarySQL requests work beyond counters that already equal int64 maximum.
	compactDAGChargePastBoundarySQL = `select pg_temp.dawgs_admit_traversal_work_v2(1,1,'overflow')`

	// compactDAGReadonlyFixtureSQL seeds two hops and creates every workspace before entering READ ONLY.
	compactDAGReadonlyFixtureSQL = `
		insert into edge values (1,1,10,11,1),(2,1,11,12,1);
		select pg_temp.ensure_all_shortest_dag_v2_workspace();`

	// compactDAGClearEdgesSQL replaces only the session-local randomized edge fixture.
	compactDAGClearEdgesSQL = "truncate table edge"

	// compactDAGSetQueryTokenSQL isolates each independent oracle comparison's admission counters.
	compactDAGSetQueryTokenSQL = "select set_config('dawgs.traversal_token',$1,true)"
)

// compactDAGFixture keeps the lifetime-bound context and transaction for a temporary SQL graph.
type compactDAGFixture struct {
	// ctx expires all operations if the fixture exceeds its test deadline.
	ctx context.Context

	// tx owns the temporary definitions and rolls their changes back during cleanup.
	tx pgx.Tx
}

// compactDAGFunctionSQL extracts one complete, uniquely named function so the
// integration fixture can install it in pg_temp without changing public DDL.
func compactDAGFunctionSQL(t *testing.T, name string) string {
	t.Helper()

	start := strings.Index(sqlSchemaUp, "create or replace function public."+name+"(")
	require.NotEqual(t, -1, start, name)

	var (
		function  = sqlSchemaUp[start:]
		bodyStart = strings.Index(function, "$$")
	)
	require.NotEqual(t, -1, bodyStart, name)

	bodyEnd := strings.Index(function[bodyStart+2:], "$$")
	require.NotEqual(t, -1, bodyEnd, name)

	var (
		bodyBoundary = bodyEnd + bodyStart + 4
		end          = strings.Index(function[bodyBoundary:], ";")
	)
	require.NotEqual(t, -1, end, name)

	return strings.ReplaceAll(function[:bodyBoundary+end+1], "public.", "pg_temp.")
}

// compactDAGSQLFixture uses only temporary tables/functions and rolls back all
// fixture changes, including when CONNECTION_STRING selects a populated graph.
func compactDAGSQLFixture(t *testing.T) compactDAGFixture {
	t.Helper()

	connection := os.Getenv(compactDAGConnectionStringEnv)
	if connection == "" {
		t.Skip("CONNECTION_STRING is not set")
	}
	normalized := strings.ToLower(connection)
	if !strings.HasPrefix(normalized, "postgres://") && !strings.HasPrefix(normalized, "postgresql://") {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := tx.Rollback(context.Background())
		if !errors.Is(err, pgx.ErrTxClosed) {
			require.NoError(t, err)
		}
	})
	_, err = tx.Exec(ctx, compactDAGFixtureSetupSQL)
	require.NoError(t, err)
	for _, name := range []string{
		"shortest_path_self_endpoint_error", "ensure_shortest_dag_workspace", "reset_shortest_dag_workspace",
		"dawgs_admit_traversal_v1", "dawgs_admit_traversal_work_v2",
		"ensure_all_shortest_dag_v2_workspace", "all_shortest_paths_dag_v2",
	} {
		_, err := tx.Exec(ctx, compactDAGFunctionSQL(t, name))
		require.NoError(t, err, name)
	}

	return compactDAGFixture{
		ctx: ctx,
		tx:  tx,
	}
}

// compactDAGPaths consumes the full row stream and returns edge-ID sequences in SQL order.
func compactDAGPaths(t *testing.T, ctx context.Context, tx pgx.Tx, source, target, maximum int64, bounded, inbound bool, kinds []int16) [][]int64 {
	t.Helper()

	rows, err := tx.Query(ctx, compactDAGSelectPathsSQL, source, target, maximum, bounded, kinds, inbound)
	require.NoError(t, err)
	defer rows.Close()

	paths := make([][]int64, 0)
	for rows.Next() {
		var path []int64
		require.NoError(t, rows.Scan(&path))

		paths = append(paths, path)
	}
	require.NoError(t, rows.Err())

	return paths
}

// TestCompactDAGV2CompleteLayers checks parallel predecessor retention, both
// directions, finite semantics, open-bound encoding and cyclic no-path search.
func TestCompactDAGV2CompleteLayers(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGDiamondFixtureSQL)
	require.NoError(t, err)
	all := [][]int64{{1, 3}, {1, 5}, {2, 4}}
	require.Equal(t, all, compactDAGPaths(t, ctx, tx, 10, 13, 0, false, false, []int16{}))
	require.Equal(t, all, compactDAGPaths(t, ctx, tx, 10, 13, -7, false, false, []int16{}), "open numeric maximum is unused")
	require.Equal(t, all, compactDAGPaths(t, ctx, tx, 10, 13, 9223372036854775807, true, false, []int16{}))
	require.Empty(t, compactDAGPaths(t, ctx, tx, 10, 13, 1, true, false, []int16{}))
	require.Empty(t, compactDAGPaths(t, ctx, tx, 10, 13, 0, true, false, []int16{}))
	require.Equal(t, [][]int64{{1, 3}, {2, 4}}, compactDAGPaths(t, ctx, tx, 10, 13, 0, false, false, []int16{1}))
	require.Equal(t, [][]int64{{3, 1}, {4, 2}, {5, 1}}, compactDAGPaths(t, ctx, tx, 13, 10, 0, false, true, []int16{}))
	require.Empty(t, compactDAGPaths(t, ctx, tx, 10, 999, 0, false, false, []int16{}))
	require.Equal(t, all, compactDAGPaths(t, ctx, tx, 10, 13, 0, false, false, []int16{}), "a no-path call leaves no stale workspace")
}

// TestCompactDAGV2OpenLongChain proves that no selector-style 64-hop bound is
// encoded in the kernel and verifies the complete ordered relationship array.
func TestCompactDAGV2OpenLongChain(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGLongChainFixtureSQL)
	require.NoError(t, err)
	expected := make([]int64, 129)
	for index := range expected {
		expected[index] = 10000 + int64(index)
	}
	require.Equal(t, [][]int64{expected}, compactDAGPaths(t, ctx, tx, 1000, 1129, 0, false, false, []int16{}))
	require.Empty(t, compactDAGPaths(t, ctx, tx, 1000, 1129, 128, true, false, []int16{}))
}

// TestCompactDAGV2ResourcePhases verifies every materialization phase is
// charged, including the one-hop case and final function result storage.
func TestCompactDAGV2ResourcePhases(t *testing.T) {
	for _, testCase := range []struct {
		// name identifies the admission phase whose first rejected record is tested.
		name string

		// setting selects the transaction-local state or byte budget to reduce.
		setting string

		// value allows exactly the records preceding the expected rejection.
		value string

		// phase is the kernel stage reported in the PostgreSQL error detail.
		phase string
	}{
		{
			name:    "discovery",
			setting: compactDAGStateLimitSetting,
			value:   "1",
			phase:   "asp_discovery",
		},
		{
			name:    "predecessor",
			setting: compactDAGStateLimitSetting,
			value:   "2",
			phase:   "asp_predecessor",
		},
		{
			name:    "seen",
			setting: compactDAGStateLimitSetting,
			value:   "3",
			phase:   "asp_seen",
		},
		{
			name:    "enumeration",
			setting: compactDAGStateLimitSetting,
			value:   "4",
			phase:   "asp_enumeration",
		},
		{
			name:    "output",
			setting: compactDAGStateLimitSetting,
			value:   "6",
			phase:   "asp_output",
		},
		{
			name:    "bytes",
			setting: compactDAGByteLimitSetting,
			value:   "591",
			phase:   "asp_output",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var (
				fixture = compactDAGSQLFixture(t)
				ctx     = fixture.ctx
				tx      = fixture.tx
			)

			_, err := tx.Exec(ctx, compactDAGOneEdgeFixtureSQL)
			require.NoError(t, err)
			_, err = tx.Exec(ctx, compactDAGSetSettingSQL, testCase.setting, testCase.value)
			require.NoError(t, err)
			var count int64
			err = tx.QueryRow(ctx, compactDAGCountOneHopSQL).Scan(&count)
			var pgError *pgconn.PgError
			require.ErrorAs(t, err, &pgError)
			require.Equal(t, compactDAGResourceSQLState, pgError.Code)
			require.Contains(t, pgError.Detail, "phase="+testCase.phase)
		})
	}
}

// TestCompactDAGV2ExactBudgetAndTokenReuse verifies N succeeds, N+1 work fails
// rather than returning an incomplete result, and independent tokens reset.
func TestCompactDAGV2ExactBudgetAndTokenReuse(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGExactBudgetFixtureSQL)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{1}}, compactDAGPaths(t, ctx, tx, 10, 11, 0, false, false, []int16{}))
	var (
		states int64
		bytes  int64
	)

	require.NoError(t, tx.QueryRow(ctx, compactDAGReadCountersSQL).Scan(&states, &bytes))
	require.EqualValues(t, 7, states)
	require.EqualValues(t, 592, bytes)

	savepoint, err := tx.Begin(ctx)
	require.NoError(t, err)
	var count int64
	err = savepoint.QueryRow(ctx, compactDAGCountOneHopSQL).Scan(&count)
	var pgError *pgconn.PgError
	require.ErrorAs(t, err, &pgError)
	require.Equal(t, compactDAGResourceSQLState, pgError.Code)
	require.NoError(t, savepoint.Rollback(ctx))
	_, err = tx.Exec(ctx, compactDAGNextQueryTokenSQL)
	require.NoError(t, err)
	require.Equal(t, [][]int64{{1}}, compactDAGPaths(t, ctx, tx, 10, 11, 0, false, false, []int16{}))
}

// TestCompactDAGV2RejectsUnsupportedContracts keeps direct SQL callers from
// acquiring an incoherent graph snapshot or bypassing shortest-path semantics.
func TestCompactDAGV2RejectsUnsupportedContracts(t *testing.T) {
	for _, testCase := range []struct {
		// name identifies the unsupported SQL contract supplied by a direct caller.
		name string

		// query invokes the kernel or admission helper with the invalid argument.
		query string

		// state is the PostgreSQL error code required for this rejection.
		state string
	}{
		{
			name:  "minimum",
			query: compactDAGRejectMinimumSQL,
			state: compactDAGArgumentSQLState,
		},
		{
			name:  "negative maximum",
			query: compactDAGRejectMaximumSQL,
			state: compactDAGArgumentSQLState,
		},
		{
			name:  "equal endpoints",
			query: compactDAGRejectSelfSQL,
			state: compactDAGArgumentSQLState,
		},
		{
			name:  "negative delta",
			query: compactDAGRejectNegativeWorkSQL,
			state: compactDAGResourceSQLState,
		},
		{
			name:  "minimum state delta",
			query: compactDAGRejectMinimumStateSQL,
			state: compactDAGResourceSQLState,
		},
		{
			name:  "minimum byte delta",
			query: compactDAGRejectMinimumBytesSQL,
			state: compactDAGResourceSQLState,
		},
		{
			name:  "addition overflow",
			query: compactDAGRejectOversizedWorkSQL,
			state: compactDAGResourceSQLState,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var (
				fixture = compactDAGSQLFixture(t)
				ctx     = fixture.ctx
				tx      = fixture.tx
			)

			_, err := tx.Exec(ctx, testCase.query)
			var pgError *pgconn.PgError
			require.ErrorAs(t, err, &pgError)
			require.Equal(t, testCase.state, pgError.Code)
		})
	}
	t.Run("read committed", func(t *testing.T) {
		var (
			fixture = compactDAGSQLFixture(t)
			ctx     = fixture.ctx
			tx      = fixture.tx
		)
		// Setup commands necessarily establish the fixture snapshot already;
		// COMMIT preserves its session-local functions for a new RC transaction.
		conn := tx.Conn()
		require.NoError(t, tx.Commit(ctx))
		rc, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		require.NoError(t, err)
		defer rc.Rollback(ctx)
		_, err = rc.Exec(ctx, compactDAGCountOneHopSQL)
		var pgError *pgconn.PgError
		require.ErrorAs(t, err, &pgError)
		require.Equal(t, compactDAGSnapshotSQLState, pgError.Code)
	})
}

// TestCompactDAGV2SharesLegacyAdmission verifies a compact stage cannot reset
// work already charged by a generic traversal in the same logical query.
func TestCompactDAGV2SharesLegacyAdmission(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGSharedAdmissionSQL)
	require.NoError(t, err)
	var (
		states int64
		bytes  int64
	)

	require.NoError(t, tx.QueryRow(ctx, compactDAGReadCountersSQL).Scan(&states, &bytes))
	require.EqualValues(t, 3, states)
	require.EqualValues(t, 208, bytes)
	_, err = tx.Exec(ctx, compactDAGLimitSharedStatesSQL)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, compactDAGChargeLegacySQL)
	var pgError *pgconn.PgError
	require.ErrorAs(t, err, &pgError)
	require.Equal(t, compactDAGResourceSQLState, pgError.Code)
}

// TestCompactDAGV2AdmissionInt64Boundary checks the limit without overflowing
// arithmetic when an otherwise valid configured budget reaches int8 maximum.
func TestCompactDAGV2AdmissionInt64Boundary(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGInt64BoundarySQL)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, compactDAGChargePastBoundarySQL)
	var pgError *pgconn.PgError
	require.ErrorAs(t, err, &pgError)
	require.Equal(t, compactDAGResourceSQLState, pgError.Code)
	require.Contains(t, pgError.Detail, "phase=overflow")
}

// TestCompactDAGV2ReusesReadonlyWorkspace checks both the initial reset and
// repeated discovery/enumeration resets after the driver prepares temp tables.
func TestCompactDAGV2ReusesReadonlyWorkspace(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)

	_, err := tx.Exec(ctx, compactDAGReadonlyFixtureSQL)
	require.NoError(t, err)
	conn := tx.Conn()
	require.NoError(t, tx.Commit(ctx))
	readonly, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	require.NoError(t, err)
	defer readonly.Rollback(ctx)
	for range 2 {
		require.Equal(t, [][]int64{{1, 2}}, compactDAGPaths(t, ctx, readonly, 10, 12, 0, false, false, []int16{}))
	}
}

// compactDAGOracleEdge records one independently generated directed relationship.
type compactDAGOracleEdge struct {
	// id distinguishes parallel relationships in the expected path multiset.
	id int64

	// start is the physical outbound endpoint of this relationship.
	start int64

	// end is the physical inbound endpoint of this relationship.
	end int64

	// graph identifies graph-scoping decoys that must never enter the search.
	graph int32

	// kind selects one of the parallel relationship-kind alternatives.
	kind int16
}

// compactDAGExhaustiveShortest is independent of BFS and the SQL workspace:
// enumerate every node-simple path by DFS, then retain minimum-length paths.
// For distinct endpoints and no path predicates a shortest path is node-simple,
// so visited-node exclusion proves termination without a query hop limit.
func compactDAGExhaustiveShortest(edges []compactDAGOracleEdge, source, target int64, inbound, typed bool) [][]int64 {
	var (
		paths   = make([][]int64, 0)
		minimum int
		visited = map[int64]bool{source: true}
		visit   func(int64, []int64)
	)
	visit = func(node int64, path []int64) {
		if node == target {
			if minimum == 0 || len(path) < minimum {
				minimum = len(path)
				paths = paths[:0]
			}
			if len(path) == minimum {
				paths = append(paths, slices.Clone(path))
			}
			return
		}
		if minimum != 0 && len(path) >= minimum {
			return
		}
		for _, edge := range edges {
			if edge.graph != 1 || (typed && edge.kind != 1) {
				continue
			}
			var (
				start = edge.start
				end   = edge.end
			)
			if inbound {
				start, end = end, start
			}
			if start != node || visited[end] {
				continue
			}
			visited[end] = true
			visit(end, append(path, edge.id))
			delete(visited, end)
		}
	}
	visit(source, nil)
	slices.SortFunc(paths, func(left, right []int64) int { return slices.Compare(left, right) })

	return paths
}

// TestCompactDAGV2MatchesIndependentSmallGraphOracle compares complete ordered
// edge-ID multisets against exhaustive DFS on deterministic directed graphs,
// including parallel kinds, cycles, and graph-scoping decoys. Every kernel call
// uses an open maximum and receives a fresh logical query budget token.
func TestCompactDAGV2MatchesIndependentSmallGraphOracle(t *testing.T) {
	var (
		fixture = compactDAGSQLFixture(t)
		ctx     = fixture.ctx
		tx      = fixture.tx
	)
	var (
		random       = rand.New(rand.NewSource(718394261))
		noPath       int
		multiHopTies int
		longest      int
	)
	for graphIndex := range 32 {
		var (
			edges  = make([]compactDAGOracleEdge, 0)
			nextID = int64(1)
		)
		for start := range 5 {
			for end := range 5 {
				if random.Intn(100) >= 34 {
					continue
				}
				kind := int16(1 + random.Intn(2))
				edges = append(edges, compactDAGOracleEdge{
					id:    nextID,
					graph: 1,
					start: int64(100 + start),
					end:   int64(100 + end),
					kind:  kind,
				})
				nextID++
				if random.Intn(100) < 35 {
					edges = append(edges, compactDAGOracleEdge{
						id:    nextID,
						graph: 1,
						start: int64(100 + start),
						end:   int64(100 + end),
						kind:  3 - kind,
					})
					nextID++
				}
			}
		}
		var (
			source = int64(100 + random.Intn(5))
			target = int64(100 + random.Intn(4))
		)
		if target >= source {
			target++
		}
		// These direct edges would incorrectly win if graph isolation leaked.
		edges = append(edges,
			compactDAGOracleEdge{
				id:    900001,
				graph: 2,
				start: source,
				end:   target,
				kind:  1,
			},
			compactDAGOracleEdge{
				id:    900002,
				graph: 2,
				start: target,
				end:   source,
				kind:  1,
			},
		)
		_, err := tx.Exec(ctx, compactDAGClearEdgesSQL)
		require.NoError(t, err)
		rows := make([][]any, len(edges))
		for index, edge := range edges {
			rows[index] = []any{edge.id, edge.graph, edge.start, edge.end, edge.kind}
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"edge"}, []string{"id", "graph_id", "start_id", "end_id", "kind_id"}, pgx.CopyFromRows(rows))
		require.NoError(t, err)
		for _, inbound := range []bool{false, true} {
			for _, typed := range []bool{false, true} {
				name := fmt.Sprintf("graph=%d/inbound=%t/typed=%t", graphIndex, inbound, typed)
				t.Run(name, func(t *testing.T) {
					_, err := tx.Exec(ctx, compactDAGSetQueryTokenSQL, name)
					require.NoError(t, err)
					kinds := []int16{}
					if typed {
						kinds = []int16{1}
					}
					var (
						expected = compactDAGExhaustiveShortest(edges, source, target, inbound, typed)
						actual   = compactDAGPaths(t, ctx, tx, source, target, 0, false, inbound, kinds)
					)
					require.Equal(t, expected, actual, "source=%d target=%d edges=%+v", source, target, edges)
					if len(expected) == 0 {
						noPath++
					} else {
						longest = max(longest, len(expected[0]))
						if len(expected) > 1 && len(expected[0]) > 1 {
							multiHopTies++
						}
					}
				})
			}
		}
	}

	require.Positive(t, noPath, "oracle matrix must exercise disconnected endpoint pairs")
	require.Positive(t, multiHopTies, "oracle matrix must exercise multi-hop shortest-path multiplicity")
	require.GreaterOrEqual(t, longest, 3, "oracle matrix must exercise more than shallow special cases")
}
