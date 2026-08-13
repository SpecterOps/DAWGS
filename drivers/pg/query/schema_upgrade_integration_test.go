// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration && integration

package query

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/stretchr/testify/require"
)

// TestSchemaUpgradeRemovesLegacyPathMaterializerOverloads verifies an upgrade drops obsolete unscoped path functions while retaining graph-scoped signatures.
func TestSchemaUpgradeRemovesLegacyPathMaterializerOverloads(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		drop function public.nodes_to_path(int4, int8[]);
		drop function public.edges_to_path(int4, int8[]);
		drop function public.ordered_edges_to_path(int4, nodeComposite, edgeComposite[], nodeComposite[]);
		create function public.nodes_to_path(nodes variadic int8[]) returns pathComposite language sql immutable strict as $$
			select row(array[]::nodeComposite[], array[]::edgeComposite[])::pathComposite
		$$;
		create function public.edges_to_path(path variadic int8[]) returns pathComposite language sql immutable strict as $$
			select row(array[]::nodeComposite[], array[]::edgeComposite[])::pathComposite
		$$;
		create function public.ordered_edges_to_path(root nodeComposite, edges edgeComposite[], known_nodes nodeComposite[]) returns pathComposite language sql immutable strict as $$
			select row(array[root]::nodeComposite[], edges)::pathComposite
		$$;
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)

	var legacyNodes, legacyEdges, legacyOrdered, scopedNodes, scopedEdges, scopedOrdered bool
	err = pool.QueryRow(ctx, `select
		to_regprocedure('public.nodes_to_path(bigint[])') is not null,
		to_regprocedure('public.edges_to_path(bigint[])') is not null,
		to_regprocedure('public.ordered_edges_to_path(nodecomposite,edgecomposite[],nodecomposite[])') is not null,
		to_regprocedure('public.nodes_to_path(integer,bigint[])') is not null,
		to_regprocedure('public.edges_to_path(integer,bigint[])') is not null,
		to_regprocedure('public.ordered_edges_to_path(integer,nodecomposite,edgecomposite[],nodecomposite[])') is not null
	`).Scan(&legacyNodes, &legacyEdges, &legacyOrdered, &scopedNodes, &scopedEdges, &scopedOrdered)
	require.NoError(t, err)
	require.False(t, legacyNodes)
	require.False(t, legacyEdges)
	require.False(t, legacyOrdered)
	require.True(t, scopedNodes)
	require.True(t, scopedEdges)
	require.True(t, scopedOrdered)
}

// TestBidirectionalAllShortestPathCapBoundaries proves that every candidate
// admission gate is exact at N, fails closed at N-1, and preserves the full
// ASP-A1 multiset on fallback. The fixture reconverges through two middle
// nodes so equal-depth, relationship-distinct predecessor rows are required
// to produce all six shortest paths.
func TestBidirectionalAllShortestPathCapBoundaries(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	connectionHandle, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer connectionHandle.Release()

	_, err = connectionHandle.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)
	_, err = connectionHandle.Exec(ctx, `
		create temporary table edge
		(
			id int8 not null,
			graph_id int4 not null,
			start_id int8 not null,
			end_id int8 not null,
			kind_id int2 not null,
			properties jsonb not null
		) on commit preserve rows;
		insert into edge(id, graph_id, start_id, end_id, kind_id, properties) values
			(101, 1, 1, 2, 1, '{}'), (102, 1, 1, 3, 1, '{}'), (103, 1, 1, 4, 1, '{}'),
			(104, 1, 2, 5, 1, '{}'), (105, 1, 2, 6, 1, '{}'),
			(106, 1, 3, 5, 1, '{}'), (107, 1, 3, 6, 1, '{}'),
			(108, 1, 4, 5, 1, '{}'), (109, 1, 4, 6, 1, '{}'),
			(110, 1, 5, 9, 1, '{}'), (111, 1, 6, 9, 1, '{}');
	`)
	require.NoError(t, err)

	tx, err := connectionHandle.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	readPaths := func(query string, args ...any) []string {
		rows, queryErr := tx.Query(ctx, query, args...)
		require.NoError(t, queryErr)
		defer rows.Close()
		paths := []string{}
		for rows.Next() {
			var path string
			require.NoError(t, rows.Scan(&path))
			paths = append(paths, path)
		}
		require.NoError(t, rows.Err())
		return paths
	}

	exact := readPaths(`
		select path::text
		from public.all_shortest_paths_dag(1, 1, 9, 1, 8, array[]::int2[], false)
		order by path`)
	require.Len(t, exact, 6)

	// limits configures each guarded resource dimension exercised by the helper.
	type limits struct {
		// state retains the state while limits is assembled or evaluated.
		state int64
		// frontier retains the frontier while limits is assembled or evaluated.
		frontier int64
		// predecessor retains the predecessor while limits is assembled or evaluated.
		predecessor int64
		// enumeration retains the enumeration while limits is assembled or evaluated.
		enumeration int64
		// outputBytes retains the output bytes while limits is assembled or evaluated.
		outputBytes int64
	}

	// diagnostic decodes the runtime receipt returned by the guarded helper.
	type diagnostic struct {
		// RuntimeBranch supplies the runtime branch input to the diagnostic contract.
		RuntimeBranch string `json:"runtime_branch"`
		// Overflowed indicates whether overflowed applies.
		Overflowed bool `json:"overflowed"`
		// FallbackExecuted indicates whether fallback executed applies.
		FallbackExecuted bool `json:"fallback_executed"`
		// Counters supplies the counters input to the diagnostic contract.
		Counters struct {
			// SeenPeak supplies the seen peak input to the Counters contract.
			SeenPeak int64 `json:"seen_peak"`
			// FrontierPeak supplies the frontier peak input to the Counters contract.
			FrontierPeak int64 `json:"frontier_peak"`
			// PredecessorPeak supplies the predecessor peak input to the Counters contract.
			PredecessorPeak int64 `json:"predecessor_peak"`
			// OutputPaths identifies the filesystem output paths.
			OutputPaths int64 `json:"output_paths"`
			// OutputBytes supplies the output bytes input to the Counters contract.
			OutputBytes int64 `json:"output_bytes"`
		} `json:"counters"`
	}
	const candidateQuery = `
		select path::text
		from public.all_shortest_paths_b1_strict_alternating(
			1, 1, 9, 1, 8, array[]::int2[], false, $1, $2, $3, $4, $5)
		order by path`
	runCandidate := func(invocationID string, caps limits) ([]string, diagnostic) {
		_, execErr := tx.Exec(ctx, "select public.begin_bidirectional_all_shortest_path_diagnostic_v1($1)", invocationID)
		require.NoError(t, execErr)
		paths := readPaths(candidateQuery, caps.state, caps.frontier, caps.predecessor, caps.enumeration, caps.outputBytes)
		var raw string
		require.NoError(t, tx.QueryRow(ctx,
			"select public.read_bidirectional_all_shortest_path_diagnostic_v1($1)::text", invocationID).Scan(&raw))
		var report diagnostic
		require.NoError(t, json.Unmarshal([]byte(raw), &report))
		_, execErr = tx.Exec(ctx, "select public.clear_bidirectional_all_shortest_path_diagnostic_v1($1)", invocationID)
		require.NoError(t, execErr)
		return paths, report
	}

	large := limits{
		state:       1_000_000,
		frontier:    1_000_000,
		predecessor: 1_000_000,
		enumeration: 1_000_000,
		outputBytes: 1 << 30,
	}
	for _, scheduler := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// query retains the query while anonymous record is assembled or evaluated.
		query string
	}{
		{
			name:  "B1 strict alternating",
			query: candidateQuery,
		},
		{
			name: "B2 smaller level",
			query: `
			select path::text
			from public.all_shortest_paths_b2_smaller_current_level(
				1, 1, 9, 1, 8, array[]::int2[], false, $1, $2, $3, $4, $5)
			order by path`,
		},
	} {
		t.Run(scheduler.name+" retains the exact multiset", func(t *testing.T) {
			paths := readPaths(scheduler.query, large.state, large.frontier, large.predecessor, large.enumeration, large.outputBytes)
			require.Equal(t, exact, paths)
		})
	}

	baselinePaths, baseline := runCandidate("asp-cap-baseline", large)
	require.Equal(t, exact, baselinePaths)
	require.Equal(t, "bidirectional_search", baseline.RuntimeBranch)
	require.False(t, baseline.Overflowed)
	require.False(t, baseline.FallbackExecuted)
	require.Positive(t, baseline.Counters.SeenPeak)
	require.Positive(t, baseline.Counters.FrontierPeak)
	require.Positive(t, baseline.Counters.PredecessorPeak)
	require.Equal(t, int64(len(exact)), baseline.Counters.OutputPaths)
	require.Positive(t, baseline.Counters.OutputBytes)

	boundaries := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// get retains the get while anonymous record is assembled or evaluated.
		get func(limits) int64
		// set retains the set while anonymous record is assembled or evaluated.
		set func(*limits, int64)
	}{
		{
			name: "state",
			get:  func(_ limits) int64 { return baseline.Counters.SeenPeak },
			set:  func(value *limits, limit int64) { value.state = limit },
		},
		{
			name: "frontier",
			get:  func(_ limits) int64 { return baseline.Counters.FrontierPeak },
			set:  func(value *limits, limit int64) { value.frontier = limit },
		},
		{
			name: "predecessor",
			get:  func(_ limits) int64 { return baseline.Counters.PredecessorPeak },
			set:  func(value *limits, limit int64) { value.predecessor = limit },
		},
		{
			name: "enumeration",
			get:  func(_ limits) int64 { return baseline.Counters.OutputPaths },
			set:  func(value *limits, limit int64) { value.enumeration = limit },
		},
		{
			name: "output bytes",
			get:  func(_ limits) int64 { return baseline.Counters.OutputBytes },
			set:  func(value *limits, limit int64) { value.outputBytes = limit },
		},
	}
	for _, boundary := range boundaries {
		boundary := boundary
		n := boundary.get(large)
		for _, delta := range []int64{-1, 0, 1} {
			delta := delta
			name := boundary.name + map[int64]string{-1: " N-1", 0: " N", 1: " N+1"}[delta]
			t.Run(name, func(t *testing.T) {
				caps := large
				boundary.set(&caps, n+delta)
				paths, report := runCandidate("asp-cap-"+boundary.name+map[int64]string{-1: "-minus", 0: "-exact", 1: "-plus"}[delta], caps)
				require.Equal(t, exact, paths, "candidate and fallback must preserve the complete ordered multiset")
				if delta < 0 {
					require.Equal(t, "exact_a1_fallback", report.RuntimeBranch)
					require.True(t, report.Overflowed)
					require.True(t, report.FallbackExecuted)
				} else {
					require.Equal(t, "bidirectional_search", report.RuntimeBranch)
					require.False(t, report.Overflowed)
					require.False(t, report.FallbackExecuted)
				}
			})
		}
	}

	require.NoError(t, tx.Rollback(ctx))
}

// TestBidirectionalShortestPathLowerBoundAndWitnesses exercises a graph where
// strict alternation encounters a length-five meeting before the unique
// length-four route. Returning the shorter route proves the queue-head
// lower-bound check continued beyond the first intersection. The tie and
// inbound assertions separately validate the one-witness contract: minimum
// depth, relationship uniqueness, and logical source-to-target edge order.
func TestBidirectionalShortestPathLowerBoundAndWitnesses(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	connectionHandle, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer connectionHandle.Release()
	_, err = connectionHandle.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)
	_, err = connectionHandle.Exec(ctx, `
		create temporary table edge
		(
			id int8 not null,
			graph_id int4 not null,
			start_id int8 not null,
			end_id int8 not null,
			kind_id int2 not null,
			properties jsonb not null
		) on commit preserve rows;
		-- Graph 2: the low-ID length-five branch meets first under B1. Two
		-- target-side dead ends delay acceptance of the unique length-four arm.
		insert into edge(id, graph_id, start_id, end_id, kind_id, properties) values
			(201, 2, 1000, 1001, 1, '{}'), (203, 2, 1001, 1002, 1, '{}'),
			(205, 2, 1002, 1003, 1, '{}'), (207, 2, 1003, 1004, 1, '{}'),
			(209, 2, 1004, 1999, 1, '{}'),
			(202, 2, 1000, 1100, 1, '{}'), (204, 2, 1100, 1101, 1, '{}'),
			(206, 2, 1101, 1102, 1, '{}'), (999, 2, 1102, 1999, 1, '{}'),
			(210, 2, 1200, 1999, 1, '{}'), (211, 2, 1201, 1999, 1, '{}'),
			-- Graph 3: two equally short, relationship-disjoint witnesses.
			(301, 3, 2000, 2001, 1, '{}'), (302, 3, 2001, 2002, 1, '{}'),
			(303, 3, 2002, 2999, 1, '{}'),
			(304, 3, 2000, 2101, 1, '{}'), (305, 3, 2101, 2102, 1, '{}'),
			(306, 3, 2102, 2999, 1, '{}');
	`)
	require.NoError(t, err)

	tx, err := connectionHandle.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	// result captures the depth and path returned by one helper invocation.
	type result struct {
		// depth retains the depth while result is assembled or evaluated.
		depth int32
		// path retains the path while result is assembled or evaluated.
		path []int64
	}
	run := func(function string, graphID, sourceID, targetID int64, inbound bool) result {
		query := `select depth, path from public.` + function + `(
			$1::int4, $2::int8, $3::int8, 1, 8, array[]::int2[], $4, 100000, 100000, 100000)`
		var value result
		require.NoError(t, tx.QueryRow(ctx, query, graphID, sourceID, targetID, inbound).Scan(&value.depth, &value.path))
		return value
	}

	_, err = tx.Exec(ctx, "select public.begin_bidirectional_shortest_path_diagnostic_v1('sp-adversarial-b1')")
	require.NoError(t, err)
	b1 := run("shortest_path_b1_strict_alternating", 2, 1000, 1999, false)
	require.Equal(t, int32(4), b1.depth)
	require.Equal(t, []int64{202, 204, 206, 999}, b1.path)
	var raw string
	require.NoError(t, tx.QueryRow(ctx,
		"select public.read_bidirectional_shortest_path_diagnostic_v1('sp-adversarial-b1')::text").Scan(&raw))
	var report struct {
		// RuntimeBranch supplies the runtime branch input to the anonymous record contract.
		RuntimeBranch string `json:"runtime_branch"`
		// Counters supplies the counters input to the anonymous record contract.
		Counters struct {
			// MeetingCandidates supplies the meeting candidates input to the Counters contract.
			MeetingCandidates int64 `json:"meeting_candidates"`
			// FrozenDistance supplies the frozen distance input to the Counters contract.
			FrozenDistance int32 `json:"frozen_distance"`
			// WitnessRows records the number of witness rows.
			WitnessRows int64 `json:"witness_rows"`
		} `json:"counters"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &report))
	require.Equal(t, "bidirectional_search", report.RuntimeBranch)
	require.GreaterOrEqual(t, report.Counters.MeetingCandidates, int64(2), "the longer and shorter intersections must both be observed")
	require.Equal(t, int32(4), report.Counters.FrozenDistance)
	require.Equal(t, int64(1), report.Counters.WitnessRows)
	_, err = tx.Exec(ctx, "select public.clear_bidirectional_shortest_path_diagnostic_v1('sp-adversarial-b1')")
	require.NoError(t, err)

	for _, scheduler := range []string{
		"shortest_path_b1_strict_alternating",
		"shortest_path_b2_smaller_current_level",
	} {
		t.Run(scheduler+" unique and inbound witnesses", func(t *testing.T) {
			outbound := run(scheduler, 2, 1000, 1999, false)
			require.Equal(t, int32(4), outbound.depth)
			require.Equal(t, []int64{202, 204, 206, 999}, outbound.path)
			require.Len(t, outbound.path, int(outbound.depth))

			inbound := run(scheduler, 2, 1999, 1000, true)
			require.Equal(t, int32(4), inbound.depth)
			require.Equal(t, []int64{999, 206, 204, 202}, inbound.path)
			require.Len(t, inbound.path, int(inbound.depth))

			tie := run(scheduler, 3, 2000, 2999, false)
			require.Equal(t, int32(3), tie.depth)
			require.Len(t, tie.path, int(tie.depth))
			require.Contains(t, [][]int64{{301, 302, 303}, {304, 305, 306}}, tie.path)
			relationships := map[int64]struct{}{}
			for _, edgeID := range tie.path {
				relationships[edgeID] = struct{}{}
			}
			require.Len(t, relationships, len(tie.path), "a shortest witness may not repeat a relationship")
		})
	}

	require.NoError(t, tx.Rollback(ctx))
}
