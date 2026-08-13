// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// outboundShortestPathQuery is the canonical bound-endpoint path query shared by reference-arm tests.
const outboundShortestPathQuery = "MATCH p = shortestPath((s)-[*0..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"

// TestSupplementalPostgresReadHelpersPropagateTransactionOptions verifies that
// reference timing, precomputation, and plan capture all retain the caller's
// stable-snapshot transaction contract.
func TestSupplementalPostgresReadHelpersPropagateTransactionOptions(t *testing.T) {
	database := &referenceTransactionOptionTestDatabase{expectedDriverConfig: "stable-snapshot"}
	transactionOption := func(config *graph.TransactionConfig) {
		config.DriverConfig = "stable-snapshot"
	}

	rowCount, _, err := measureRawPostgres(context.Background(), database, "select value", nil, 0, 1, transactionOption)
	require.NoError(t, err)
	require.Equal(t, int64(1), rowCount)

	values, err := readReferenceRow(context.Background(), database, "select value", nil, transactionOption)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, values)

	plan, planJSON, _, err := explainRawPostgres(context.Background(), database, "select value", nil, transactionOption)
	require.NoError(t, err)
	require.NotEmpty(t, plan)
	require.NotEmpty(t, planJSON)

	require.Equal(t, []bool{true, true, true, true}, database.transactionOptionsApplied)
}

// referenceTransactionOptionTestDatabase records transaction configuration and
// supplies the narrow raw-query surface used by supplemental reference helpers.
type referenceTransactionOptionTestDatabase struct {
	// Database supplies the database input to the referenceTransactionOptionTestDatabase contract.
	graph.Database
	// expectedDriverConfig retains the expected driver config while referenceTransactionOptionTestDatabase is assembled or evaluated.
	expectedDriverConfig any
	// transactionOptionsApplied retains the transaction options applied while referenceTransactionOptionTestDatabase is assembled or evaluated.
	transactionOptionsApplied []bool
}

// ReadTransaction applies the supplied options before executing a synthetic raw transaction.
func (s *referenceTransactionOptionTestDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	config := &graph.TransactionConfig{}
	for _, option := range options {
		option(config)
	}
	s.transactionOptionsApplied = append(s.transactionOptionsApplied, config.DriverConfig == s.expectedDriverConfig)
	return delegate(&referenceTransactionOptionTestTransaction{})
}

// referenceTransactionOptionTestTransaction returns one scalar row or one valid plan document.
type referenceTransactionOptionTestTransaction struct {
	// Transaction supplies the transaction input to the referenceTransactionOptionTestTransaction contract.
	graph.Transaction
}

// Raw returns the minimal row shape expected by the helper under test.
func (s *referenceTransactionOptionTestTransaction) Raw(statement string, _ map[string]any) graph.Result {
	if strings.Contains(statement, "FORMAT JSON") {
		return &referenceTransactionOptionTestResult{rows: [][]any{{`[{"Plan":{"Node Type":"Result","Actual Rows":1,"Actual Loops":1}}]`}}}
	}
	if strings.HasPrefix(statement, "EXPLAIN ") {
		return &referenceTransactionOptionTestResult{rows: [][]any{{"Result"}}}
	}
	return &referenceTransactionOptionTestResult{rows: [][]any{{int64(1)}}}
}

// referenceTransactionOptionTestResult iterates a fixed set of raw rows.
type referenceTransactionOptionTestResult struct {
	// Result supplies the result input to the referenceTransactionOptionTestResult contract.
	graph.Result
	// rows retains the rows while referenceTransactionOptionTestResult is assembled or evaluated.
	rows [][]any
	// index retains the index while referenceTransactionOptionTestResult is assembled or evaluated.
	index int
}

// Next advances to the next fixed row.
func (s *referenceTransactionOptionTestResult) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

// Values returns the current fixed row.
func (s *referenceTransactionOptionTestResult) Values() []any {
	return s.rows[s.index-1]
}

// Error reports a successful fixed result.
func (s *referenceTransactionOptionTestResult) Error() error {
	return nil
}

// Close satisfies graph.Result.
func (s *referenceTransactionOptionTestResult) Close() {}

// TestShortestReferenceSpecsAreGraphScopedAndSeparateRawFromFullOutput verifies the complete arm inventory, graph partition predicates, precomputed hydration inputs, and full-comparator metadata.
func TestShortestReferenceSpecsAreGraphScopedAndSeparateRawFromFullOutput(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(2), "max_depth": int32(15)}
	specs := buildShortestReferenceSpecs(ScaleCase{
		Name:   "one_shortest_path_bound_pair",
		Cypher: outboundShortestPathQuery,
	}, params, []int64{1, 2, 3}, []int64{10, 11}, graph.DirectionOutbound)

	require.Len(t, specs, 14)
	require.Equal(t, "round_trip", specs[0].name)
	require.Equal(t, int32(42), specs[1].parameters["graph_id"])
	require.Equal(t, "minimum_graph_access", specs[2].name)
	require.Contains(t, specs[3].sql, "e.graph_id = @graph_id")
	require.Contains(t, specs[3].boundary, "ordered node/edge IDs")
	require.Equal(t, []int64{10, 11}, specs[4].parameters["edge_ids"])
	require.Equal(t, []int64{1, 2, 3}, specs[6].parameters["node_ids"])

	s3u := specs[referenceSpecIndex(specs, "s3_unidirectional_trail_cte")]
	require.True(t, s3u.fullComparator)
	require.Equal(t, "complete_reference_s1_array_cte", s3u.legacyName)
	require.Equal(t, "SP-S3-U-NE", s3u.architecture)
	require.Contains(t, s3u.sql, "ordered_edge_ids_to_path")

	s3b := specs[referenceSpecIndex(specs, "s3_bidirectional_trail_cte")]
	require.Equal(t, "candidate_s2_bidirectional_cte", s3b.legacyName)
	require.Equal(t, "SP-S3-B", s3b.architecture)
	require.True(t, s3b.fullComparator)
	require.Contains(t, s3b.sql, "forward join backward")
	require.Contains(t, s3b.sql, "e.graph_id = @graph_id")
	require.Contains(t, s3b.sql, "edge_id = any(backward.edge_ids)")
}

// TestShortestDistanceReferenceCarriesNoTrailOrPredecessorState verifies that distance-only recursion stores just the frontier node and depth, avoiding node and edge trail arrays.
func TestShortestDistanceReferenceCarriesNoTrailOrPredecessorState(t *testing.T) {
	specs := buildShortestReferenceSpecs(ScaleCase{
		Name: "shortest_distance_bound_pair",
		Expected: ExpectedResult{
			ResultKind: "scalar",
		},
	}, map[string]any{}, nil, nil, graph.DirectionOutbound)
	reference := specs[referenceSpecIndex(specs, "s3_unidirectional_trail_cte")]

	require.Equal(t, "distance frontier node and depth only; no path or predecessor state", reference.stateShape)
	require.Contains(t, reference.sql, "search(node_id, depth)")
	require.NotContains(t, reference.sql, "node_ids")
	require.NotContains(t, reference.sql, "edge_ids")
}

// TestCompactBidirectionalReferencesExposeMatchedDistanceAndWitnessBoundaries
// verifies the four frozen arms share caps while preserving observation shape.
func TestCompactBidirectionalReferencesExposeMatchedDistanceAndWitnessBoundaries(t *testing.T) {
	params := map[string]any{
		"graph_id": int32(42), "start_id": int64(1), "end_id": int64(3),
		"min_depth": int32(1), "max_depth": int32(8), "edge_kind_ids": []int16{1},
	}
	distance := buildShortestReferenceSpecs(ScaleCase{
		Expected: ExpectedResult{ResultKind: "scalar"},
	}, params, nil, nil, graph.DirectionOutbound)
	for _, name := range []string{"sp_b1_strict_alternating_distance", "sp_b2_smaller_frontier_distance"} {
		spec := distance[referenceSpecIndex(distance, name)]
		require.True(t, spec.fullComparator)
		require.Equal(t, "distance scalar", spec.observationShape)
		require.Equal(t, int64(100_000), spec.parameters["state_limit"])
		require.Equal(t, int64(100_000), spec.parameters["frontier_limit"])
		require.Equal(t, int64(100_000), spec.parameters["predecessor_limit"])
		require.Contains(t, spec.sql, "select depth, path as edge_ids")
		require.NotContains(t, spec.sql, "ordered_edge_ids_to_path")
	}

	witness := buildShortestReferenceSpecs(ScaleCase{
		Name:     "one_shortest_path_bound_pair",
		Expected: ExpectedResult{ResultKind: "path_set"},
	}, params, nil, nil, graph.DirectionInbound)
	for _, name := range []string{"sp_b1_strict_alternating_witness_m0", "sp_b2_smaller_frontier_witness_m0"} {
		spec := witness[referenceSpecIndex(witness, name)]
		require.True(t, spec.fullComparator)
		require.Equal(t, "public_observation", spec.observationShape)
		require.Contains(t, spec.sql, "@edge_kind_ids, true")
		require.Contains(t, spec.sql, "terminal.id = edge.start_id")
	}
}

// TestCanonicalSourceDistanceReferenceSwapsInboundEndpointsAndPhysicalDirection verifies that the inbound-only canonical arm searches from the logical terminal using reversed physical adjacency.
func TestCanonicalSourceDistanceReferenceSwapsInboundEndpointsAndPhysicalDirection(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(10), "end_id": int64(20), "min_depth": int32(1), "max_depth": int32(8), "edge_kind_ids": []int16{1}}
	testCase := ScaleCase{
		Name: "hidden_fanin",
		Expected: ExpectedResult{
			ResultKind: "scalar",
		},
	}
	inbound := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionInbound)
	canonical := inbound[referenceSpecIndex(inbound, "s4_canonical_source_distance")]
	require.Equal(t, "SP-I1-C-D", canonical.architecture)
	require.Equal(t, int64(20), canonical.parameters["start_id"])
	require.Equal(t, int64(10), canonical.parameters["end_id"])
	require.Contains(t, canonical.sql, "e.start_id = search.node_id")
	require.Contains(t, canonical.sql, "select e.end_id")
	require.NotContains(t, canonical.sql, "edge_ids")
	require.True(t, canonical.fullComparator)

	outbound := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionOutbound)
	require.Equal(t, -1, referenceSpecIndexOrMissing(outbound, "s4_canonical_source_distance"))
}

// TestShortestS1DistancePrototypeIsDistinctBoundedAndFallsBack verifies S1 metadata, its state guard and SQL fallback, and propagation of inbound traversal direction.
func TestShortestS1DistancePrototypeIsDistinctBoundedAndFallsBack(t *testing.T) {
	minDepth, maxDepth := 1, 8
	params := map[string]any{
		"graph_id": int32(1), "start_id": int64(10), "end_id": int64(20),
		"min_depth": int32(1), "max_depth": int32(8), "edge_kind_ids": []int16{2},
	}
	testCase := ScaleCase{
		Name: "distance",
		Expected: ExpectedResult{
			ResultKind: "scalar",
		},
		Shape: WorkloadShape{
			MinDepth: &minDepth,
			MaxDepth: &maxDepth,
		},
	}
	specs := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionOutbound)
	s1 := specs[referenceSpecIndex(specs, "s1_array_bfs_distance")]

	require.Equal(t, "SP-S1", s1.architecture)
	require.Equal(t, "typed_plpgsql_array_bfs_distance_v1", s1.implementationID)
	require.True(t, s1.fullComparator)
	require.Equal(t, int32(100_000), s1.parameters["state_limit"])
	require.Contains(t, s1.sql, "graphbench_s1_distance_bfs")
	require.Contains(t, s1.sql, "where (select overflow from s1)")
	require.Contains(t, s1.sql, shortestDistanceReferenceSearchForDirection(graph.DirectionOutbound))

	inbound := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionInbound)
	require.Contains(t, inbound[referenceSpecIndex(inbound, "s1_array_bfs_distance")].sql, "@edge_kind_ids, true, @state_limit")
}

// TestShortestS1DistancePrototypeRejectsUnsupportedShapes verifies that S1 is omitted for minimum depth above one, path results, and identical bound endpoints.
func TestShortestS1DistancePrototypeRejectsUnsupportedShapes(t *testing.T) {
	minDepth, maxDepth := 2, 8
	params := map[string]any{"start_id": int64(10), "end_id": int64(20)}
	distance := ScaleCase{
		Expected: ExpectedResult{
			ResultKind: "scalar",
		},
		Shape: WorkloadShape{
			MinDepth: &minDepth,
			MaxDepth: &maxDepth,
		},
	}
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(distance, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))

	minDepth = 1
	path := ScaleCase{
		Expected: ExpectedResult{
			ResultKind: "path_set",
		},
		Shape: WorkloadShape{
			MinDepth: &minDepth,
			MaxDepth: &maxDepth,
		},
	}
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(path, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))

	params["end_id"] = int64(10)
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(distance, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))
}

// TestShortestPathReferencesCompareM0AndM1WithMinimalSearchState verifies exact M0/M1 comparator arms while preserving their edge-only versus node-and-edge hydration boundaries.
func TestShortestPathReferencesCompareM0AndM1WithMinimalSearchState(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(3), "max_depth": int32(4)}
	specs := buildShortestReferenceSpecs(
		ScaleCase{
			Name:   "one_shortest_path_bound_pair",
			Cypher: outboundShortestPathQuery,
		},
		params,
		[]int64{1, 2, 3},
		[]int64{10, 11},
		graph.DirectionOutbound,
	)

	m0 := specs[referenceSpecIndex(specs, "s3_unidirectional_cte_m0_directed")]
	m1 := specs[referenceSpecIndex(specs, "s3_unidirectional_cte_m1_ordered_ids")]
	require.Equal(t, "SP-S3-U-E+MAT-M0", m0.architecture)
	require.Equal(t, "SP-S3-U-NE+MAT-M1", m1.architecture)
	require.True(t, m0.fullComparator)
	require.True(t, m1.fullComparator)
	require.Equal(t, "exact_public_observation", m0.semanticValidation)
	require.Equal(t, "exact_public_observation", m1.semanticValidation)
	require.Contains(t, m0.sql, shortestEdgeReferenceSearch(graph.DirectionOutbound))
	require.Contains(t, m1.sql, shortestReferenceSearch())
	require.NotContains(t, m0.sql, "node_ids")
	require.NotContains(t, m0.sql, "ordered_edge_ids_to_path")
	require.NotContains(t, m1.sql, "ordered_edge_ids_to_path")
	require.Contains(t, m0.sql, "terminal.id = edge.end_id")
	require.Contains(t, m1.sql, "unnest(shortest.node_ids) with ordinality")
	require.Contains(t, m0.sql, "edge.graph_id = @graph_id")
	require.Contains(t, m1.sql, "node.graph_id = @graph_id")
}

// TestCanonicalWitnessReferenceUsesCompactDiscoveryAndRestoresInboundPathOrder verifies distance-only discovery, separate witness reconstruction, swapped inbound endpoints, and restoration of public path order.
func TestCanonicalWitnessReferenceUsesCompactDiscoveryAndRestoresInboundPathOrder(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(10), "end_id": int64(20), "min_depth": int32(1), "max_depth": int32(8), "edge_kind_ids": []int16{1}}
	testCase := ScaleCase{
		Name: "path",
		Expected: ExpectedResult{
			ResultKind: "path_set",
		},
	}
	inbound := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionInbound)
	witness := inbound[referenceSpecIndex(inbound, "s4_canonical_source_witness_m0")]
	require.Equal(t, "SP-I1-C-WE+MAT-M0", witness.architecture)
	require.Equal(t, int64(20), witness.parameters["search_start_id"])
	require.Equal(t, int64(10), witness.parameters["search_end_id"])
	require.Contains(t, witness.sql, "distance(node_id, depth)")
	require.Contains(t, witness.sql, "witness(node_id, depth, edge_ids)")
	require.Contains(t, witness.sql, "e.start_id = distance.node_id")
	require.Contains(t, witness.sql, "order by reversed.ordinal desc")
	require.Contains(t, witness.sql, "terminal.id = edge.start_id")
	require.NotContains(t, witness.sql, "distance(node_id, depth, edge_ids)")
	require.True(t, witness.fullComparator)

	outbound := buildShortestReferenceSpecs(testCase, params, nil, nil, graph.DirectionOutbound)
	outboundWitness := outbound[referenceSpecIndex(outbound, "s4_canonical_source_witness_m0")]
	require.Equal(t, int64(10), outboundWitness.parameters["search_start_id"])
	require.NotContains(t, outboundWitness.sql, "reversed.ordinal")
}

// TestAllShortestDAGReferenceRetainsEveryShortestDepthPredecessor verifies that all-shortest search records every depth-minimal predecessor and reconstructs paths in both physical directions without LIMIT-based tie loss.
func TestAllShortestDAGReferenceRetainsEveryShortestDepthPredecessor(t *testing.T) {
	outbound := allShortestDAGSearch(graph.DirectionOutbound)
	require.Contains(t, outbound, "distance(node_id, depth)")
	require.Contains(t, outbound, "predecessor(node_id, depth, predecessor_id, edge_id)")
	require.Contains(t, outbound, "paths(node_id, depth, edge_ids)")
	require.Contains(t, outbound, "e.start_id = prior.node_id and e.end_id = paths.node_id")
	require.Contains(t, outbound, "paths.depth <= target.depth")
	require.NotContains(t, outbound, "limit 1\n  ) predecessor")

	inbound := allShortestDAGSearch(graph.DirectionInbound)
	require.Contains(t, inbound, "e.end_id = distance.node_id")
	require.Contains(t, inbound, "e.end_id = prior.node_id and e.start_id = paths.node_id")
}

// TestShortestReferenceIdentitiesAndInboundMinimalState verifies normalized arm identities and inbound M0 SQL that recurses over edge trails without carrying node arrays.
func TestShortestReferenceIdentitiesAndInboundMinimalState(t *testing.T) {
	specs := buildShortestReferenceSpecs(
		ScaleCase{
			Name:   "one_shortest_path_bound_pair",
			Cypher: "MATCH p = shortestPath((s)<-[*1..4]-(e)) RETURN p",
			Expected: ExpectedResult{
				ResultKind: "path_set",
			},
		},
		map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(3), "max_depth": int32(4)},
		[]int64{1, 2, 3}, []int64{10, 11}, graph.DirectionInbound,
	)
	for idx := range specs {
		specs[idx] = normalizedReferenceSpec(specs[idx])
	}
	require.NoError(t, validateReferenceSpecs(specs))
	m0 := specs[referenceSpecIndex(specs, "s3_unidirectional_cte_m0_directed")]
	require.Contains(t, m0.sql, "e.end_id = search.node_id")
	require.Contains(t, m0.sql, "terminal.id = edge.start_id")
	require.NotContains(t, m0.sql, "node_ids")
}

// TestShortestPathMaterializerOnlyReferencesExcludeSearch verifies that M0/M1 hydration-only arms consume precomputed exact inputs and neither their timing nor validation SQL performs recursive search.
func TestShortestPathMaterializerOnlyReferencesExcludeSearch(t *testing.T) {
	specs := buildShortestReferenceSpecs(
		ScaleCase{
			Name:   "one_shortest_path_bound_pair",
			Cypher: outboundShortestPathQuery,
		},
		map[string]any{},
		[]int64{1, 2},
		[]int64{10},
		graph.DirectionOutbound,
	)

	m0 := specs[referenceSpecIndex(specs, "m0_directed_hydration_only")]
	m1 := specs[referenceSpecIndex(specs, "m1_ordered_ids_hydration_only")]
	require.False(t, m0.fullComparator)
	require.False(t, m1.fullComparator)
	require.NotContains(t, m0.sql, "with recursive")
	require.NotContains(t, m1.sql, "with recursive")
	require.Equal(t, "precomputed_exact_path_inputs", m0.semanticValidation)
	require.Equal(t, "precomputed_exact_path_inputs", m1.semanticValidation)
	require.NotEmpty(t, m0.validationSQL)
	require.NotEmpty(t, m1.validationSQL)
	require.NotContains(t, m0.validationSQL, "with recursive")
	require.NotContains(t, m1.validationSQL, "with recursive")
}

// TestShortestReferencesPreserveZeroLengthPathInputs verifies non-nil empty edge arrays, singleton node arrays, minimum-depth predicates, and bidirectional acceptance of zero-edge paths.
func TestShortestReferencesPreserveZeroLengthPathInputs(t *testing.T) {
	zeroEdges, err := referenceInt64Slice([]int64{})
	require.NoError(t, err)
	require.NotNil(t, zeroEdges)

	params := map[string]any{
		"graph_id":      int32(42),
		"start_id":      int64(1),
		"end_id":        int64(1),
		"min_depth":     int32(0),
		"max_depth":     int32(4),
		"edge_kind_ids": []int16{},
	}
	specs := buildShortestReferenceSpecs(
		ScaleCase{
			Name:   "zero_shortest_path",
			Cypher: outboundShortestPathQuery,
			Expected: ExpectedResult{
				ResultKind: "path_set",
			},
		},
		params,
		[]int64{1},
		zeroEdges,
		graph.DirectionOutbound,
	)

	require.Contains(t, shortestReferenceSearch(), "depth >= @min_depth")
	require.Contains(t, shortestDistanceReferenceSearch(), "depth >= @min_depth")
	require.Equal(t, zeroEdges, specs[referenceSpecIndex(specs, "m0_directed_hydration_only")].parameters["edge_ids"])
	require.Equal(t, []int64{1}, specs[referenceSpecIndex(specs, "m1_ordered_ids_hydration_only")].parameters["node_ids"])
	require.Contains(t, specs[referenceSpecIndex(specs, "s3_bidirectional_trail_cte")].sql, "between @min_depth and @max_depth")
}

// TestShortestMaterializersRequireProvablyOutboundPattern verifies direction parsing and withholds ordered outbound hydration arms only for directionless patterns.
func TestShortestMaterializersRequireProvablyOutboundPattern(t *testing.T) {
	for _, testCase := range []struct {
		// name identifies the direction case in subtest diagnostics.
		name string

		// query is the pattern whose relationship direction is classified.
		query string

		// outbound is true when parsing must select physical outbound traversal.
		outbound bool

		// supported is true when directional reference materializers must be available.
		supported bool
	}{
		{
			name:      "outbound",
			query:     "MATCH p = shortestPath((s)-[*1..4]->(e)) RETURN p",
			outbound:  true,
			supported: true,
		},
		{
			name:      "inbound",
			query:     "MATCH p = shortestPath((s)<-[*1..4]-(e)) RETURN p",
			supported: true,
		},
		{
			name:  "directionless",
			query: "MATCH p = shortestPath((s)-[*1..4]-(e)) RETURN p",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			direction, err := shortestReferenceDirection(testCase.query)
			require.NoError(t, err)
			require.Equal(t, testCase.outbound, direction == graph.DirectionOutbound)

			specs := buildShortestReferenceSpecs(
				ScaleCase{
					Cypher: testCase.query,
					Expected: ExpectedResult{
						ResultKind: "path_set",
					},
				},
				map[string]any{},
				[]int64{1, 2},
				[]int64{10},
				direction,
			)
			if testCase.supported {
				require.NotEqual(t, -1, referenceSpecIndexOrMissing(specs, "s3_unidirectional_cte_m0_directed"))
			} else {
				require.Equal(t, -1, referenceSpecIndexOrMissing(specs, "s3_unidirectional_cte_m0_directed"))
				require.Equal(t, -1, referenceSpecIndexOrMissing(specs, "m1_ordered_ids_hydration_only"))
			}
		})
	}
}

// TestShortestReferenceEndpointParametersFollowPatternRootOrder verifies that endpoint bindings follow left-to-right pattern roles rather than arrow direction or variable spelling.
func TestShortestReferenceEndpointParametersFollowPatternRootOrder(t *testing.T) {
	for _, testCase := range []struct {
		// name identifies the endpoint-order case in subtest diagnostics.
		name string

		// query contains the bound variables whose pattern positions are resolved.
		query string

		// root is the parameter attached to the left pattern endpoint.
		root string

		// terminal is the parameter attached to the right pattern endpoint.
		terminal string
	}{
		{
			name:     "outbound",
			query:    `MATCH p = shortestPath((s)-[:Traverse*1..8]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`,
			root:     "start_id",
			terminal: "end_id",
		},
		{
			name:     "inbound same symbols",
			query:    `MATCH p = shortestPath((s)<-[:Traverse*1..8]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`,
			root:     "start_id",
			terminal: "end_id",
		},
		{
			name:     "inbound reversed symbols",
			query:    `MATCH p = shortestPath((e)<-[:Traverse*1..8]-(s)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`,
			root:     "end_id",
			terminal: "start_id",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, terminal, err := shortestReferenceEndpointParameters(testCase.query)
			require.NoError(t, err)
			require.Equal(t, testCase.root, root)
			require.Equal(t, testCase.terminal, terminal)
		})
	}
}

// TestAlternativeOneShortestPathTieIsSemanticallyValid verifies acceptance of an equal-length valid tie and rejection of longer, wrong-kind, or unmapped alternatives.
func TestAlternativeOneShortestPathTieIsSemanticallyValid(t *testing.T) {
	testCase := ScaleCase{
		Cypher: outboundShortestPathQuery,
		Expected: ExpectedResult{
			ResultKind: "path_set",
		},
		Shape: WorkloadShape{
			EdgeKinds: []string{"Edge"},
		},
	}
	public := []string{`[{"nodes":[{"identity":"start"},{"identity":"left"},{"identity":"end"}],"relationships":[{"start":"start","end":"left","kind":"Edge"},{"start":"left","end":"end","kind":"Edge"}]}]`}
	alternative := []string{`[{"nodes":[{"identity":"start"},{"identity":"right"},{"identity":"end"}],"relationships":[{"start":"start","end":"right","kind":"Edge"},{"start":"right","end":"end","kind":"Edge"}]}]`}
	longer := []string{`[{"nodes":[{"identity":"start"},{"identity":"right"},{"identity":"other"},{"identity":"end"}],"relationships":[{"start":"start","end":"right","kind":"Edge"},{"start":"right","end":"other","kind":"Edge"},{"start":"other","end":"end","kind":"Edge"}]}]`}
	wrongKind := []string{`[{"nodes":[{"identity":"start"},{"identity":"right"},{"identity":"end"}],"relationships":[{"start":"start","end":"right","kind":"Wrong"},{"start":"right","end":"end","kind":"Wrong"}]}]`}
	unmapped := []string{`[{"nodes":[{"identity":"start"},{"identity":"unmapped-node:42"},{"identity":"end"}],"relationships":[{"start":"start","end":"unmapped-node:42","kind":"Edge"},{"start":"unmapped-node:42","end":"end","kind":"Edge"}]}]`}

	require.True(t, validAlternativeShortestPathObservation(testCase, public, alternative))
	require.False(t, validAlternativeShortestPathObservation(testCase, public, longer))
	require.False(t, validAlternativeShortestPathObservation(testCase, public, wrongKind))
	require.False(t, validAlternativeShortestPathObservation(testCase, public, unmapped))
}

// TestReferenceSpecsAlternateOrderByRound verifies fallback odd/even forward-reverse execution ordering without mutating the declared arm sequence.
func TestReferenceSpecsAlternateOrderByRound(t *testing.T) {
	specs := []postgresReferenceSpec{{name: "first"}, {name: "second"}}
	require.Equal(t, []postgresReferenceSpec{{name: "first"}, {name: "second"}}, referenceSpecsForRound(specs, 1))
	require.Equal(t, []postgresReferenceSpec{{name: "second"}, {name: "first"}}, referenceSpecsForRound(specs, 2))
	require.Equal(t, "first", specs[0].name)
}

// TestThreeArmReferenceSpecsUseCarryoverBalancedSchedule verifies the doubled
// Williams design balances both execution position and directed carryover.
func TestThreeArmReferenceSpecsUseCarryoverBalancedSchedule(t *testing.T) {
	specs := []postgresReferenceSpec{{name: "A"}, {name: "B"}, {name: "C"}}
	expected := [][]string{
		{"A", "B", "C"},
		{"B", "C", "A"},
		{"C", "A", "B"},
		{"C", "B", "A"},
		{"A", "C", "B"},
		{"B", "A", "C"},
	}
	positions := map[string][3]int{}
	carryover := map[[2]string]int{}
	for round, want := range expected {
		got := referenceSpecNames(referenceSpecsForRound(specs, round+1))
		require.Equal(t, want, got)
		for position, arm := range got {
			counts := positions[arm]
			counts[position]++
			positions[arm] = counts
			if position > 0 {
				carryover[[2]string{got[position-1], arm}]++
			}
		}
	}
	require.Equal(t, expected[0], referenceSpecNames(referenceSpecsForRound(specs, 7)))
	for _, arm := range []string{"A", "B", "C"} {
		require.Equal(t, [3]int{2, 2, 2}, positions[arm])
	}
	for _, pair := range [][2]string{{"A", "B"}, {"A", "C"}, {"B", "A"}, {"B", "C"}, {"C", "A"}, {"C", "B"}} {
		require.Equal(t, 2, carryover[pair], pair)
	}
	require.Equal(t, "A", specs[0].name)
}

// TestFiveArmReferenceSpecsUsePredeclaredBalancedSchedule verifies selected rows of the ten-round five-arm schedule and its periodic repetition.
func TestFiveArmReferenceSpecsUsePredeclaredBalancedSchedule(t *testing.T) {
	specs := []postgresReferenceSpec{{name: "T1"}, {name: "T2"}, {name: "T3"}, {name: "T4"}, {name: "T5"}}
	require.Equal(t, []string{"T1", "T2", "T5", "T3", "T4"}, referenceSpecNames(referenceSpecsForRound(specs, 1)))
	require.Equal(t, []string{"T4", "T3", "T5", "T2", "T1"}, referenceSpecNames(referenceSpecsForRound(specs, 6)))
	require.Equal(t, []string{"T1", "T2", "T5", "T3", "T4"}, referenceSpecNames(referenceSpecsForRound(specs, 11)))
}

// referenceSpecNames returns reference names in their declared execution order.
func referenceSpecNames(specs []postgresReferenceSpec) []string {
	names := make([]string, len(specs))
	for idx, spec := range specs {
		names[idx] = spec.name
	}
	return names
}

// TestAllShortestPathCaseUsesDistinctFullMultisetDAGReferences verifies that
// stored A1, inline I1, and both exact two-sided candidates retain distinct
// treatment identities.
func TestAllShortestPathCaseUsesDistinctFullMultisetDAGReferences(t *testing.T) {
	runner := &postgresSQLRunner{}
	specs, err := runner.referenceSpecs(context.Background(), ScaleCase{
		Category: "generated_shortest_path",
		Cypher:   "MATCH p = allShortestPaths((s)-[:Traverse*1..2]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p",
	}, map[string]any{"start_id": int64(1), "end_id": int64(2)})

	require.NoError(t, err)
	require.Len(t, specs, 4)
	require.Equal(t, []string{
		"asp_a1_stored_helper_m0",
		"asp_i1_inline_predecessor_dag_m0",
		"asp_b1_bidirectional_dag_strict_m0",
		"asp_b2_bidirectional_dag_smaller_frontier_m0",
	}, referenceSpecNames(specs))
	require.Equal(t, []string{"ASP-A1-DAG", "ASP-I1-U-DAG+MAT-M0", "ASP-B1-DAG-ALT-NODE", "ASP-B2-DAG-MIN-LEVEL"}, []string{
		specs[0].architecture, specs[1].architecture, specs[2].architecture, specs[3].architecture,
	})
	for _, spec := range specs {
		require.True(t, validPostgresReferenceArm(spec.name), spec.name)
		require.True(t, spec.fullComparator)
		require.Equal(t, "complete all-shortest path multiset", spec.observationShape)
		require.Equal(t, "exact_public_observation", spec.semanticValidation)
		require.Contains(t, spec.sql, "pathComposite")
	}
	for _, spec := range specs[2:] {
		require.Equal(t, int64(100_000), spec.parameters["state_limit"])
		require.Equal(t, int64(100_000), spec.parameters["frontier_limit"])
		require.Equal(t, int64(100_000), spec.parameters["predecessor_limit"])
		require.Equal(t, int64(100_000), spec.parameters["enumeration_limit"])
		require.Equal(t, int64(64*1024*1024), spec.parameters["output_bytes_limit"])
		require.Contains(t, spec.sql, "@enumeration_limit, @output_bytes_limit")
	}
	require.Contains(t, specs[0].sql, "all_shortest_paths_dag")
	require.Contains(t, specs[1].sql, "with recursive validated")
	require.Contains(t, specs[2].sql, "all_shortest_paths_b1_strict_alternating")
	require.Contains(t, specs[3].sql, "all_shortest_paths_b2_smaller_current_level")
}

// TestAllShortestBidirectionalReferencesStayInsideNarrowEnvelope verifies
// min-zero, over-depth, and equal endpoints retain only the exact A1 control.
func TestAllShortestBidirectionalReferencesStayInsideNarrowEnvelope(t *testing.T) {
	runner := &postgresSQLRunner{}
	minimumZero, maximumFour, maximumSixtyFive := 0, 4, 65
	for _, test := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// shape retains the shape while anonymous record is assembled or evaluated.
		shape WorkloadShape
		// params retains the params while anonymous record is assembled or evaluated.
		params map[string]any
	}{
		{
			name: "zero minimum",
			shape: WorkloadShape{
				MinDepth: &minimumZero,
				MaxDepth: &maximumFour,
			},
			params: map[string]any{"start_id": int64(1), "end_id": int64(2)},
		},
		{
			name:   "maximum sixty five",
			shape:  WorkloadShape{MaxDepth: &maximumSixtyFive},
			params: map[string]any{"start_id": int64(1), "end_id": int64(2)},
		},
		{
			name:   "equal endpoints",
			shape:  WorkloadShape{MaxDepth: &maximumFour},
			params: map[string]any{"start_id": int64(1), "end_id": int64(1)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			specs, err := runner.referenceSpecs(context.Background(), ScaleCase{
				Category: "generated_shortest_path",
				Cypher:   "MATCH p = allShortestPaths((s)-[:Traverse*0..65]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p",
				Shape:    test.shape,
			}, test.params)
			require.NoError(t, err)
			require.Len(t, specs, 1)
			require.Equal(t, "ASP-A1-DAG", specs[0].architecture)
		})
	}
}

// TestFixedSuffixExpansionReferenceSpecsAvoidAmbiguousArrayContainmentOperators verifies all seventeen arms use explicit membership predicates and retain each strategy's defining recursive SQL shape.
func TestFixedSuffixExpansionReferenceSpecsAvoidAmbiguousArrayContainmentOperators(t *testing.T) {
	specs := buildFixedSuffixExpansionReferenceSpecs(ScaleCase{
		Name: "fixed_suffix_expansion_endpoint_ids",
	}, map[string]any{"graph_id": int32(42)})

	require.Len(t, specs, 17)
	for _, spec := range specs {
		require.NotContains(t, spec.sql, " @> ")
	}
	require.Contains(t, specs[1].sql, "= any(n.kind_ids)")
	require.Contains(t, specs[referenceSpecIndex(specs, "suffix_seeded_reverse_ordered_ids")].sql, "array_prepend(e.id, reverse_trails.edge_ids)")
	require.Contains(t, specs[referenceSpecIndex(specs, "suffix_seeded_reverse_ordered_ids")].sql, "union all")
	require.Contains(t, specs[referenceSpecIndex(specs, "backward_viability_forward_ordered_ids")].sql, "viable(node_id, reverse_distance)")
	require.Contains(t, specs[referenceSpecIndex(specs, "factored_suffix_forward_ordered_ids")].sql, "suffix_rows")
}

// TestFixedSuffixHydrationPrecomputeIsSelectionAware verifies that default and explicit hydration-only selections request precomputed path inputs.
func TestFixedSuffixHydrationPrecomputeIsSelectionAware(t *testing.T) {
	require.True(t, referenceHydrationRequested(nil))
	require.True(t, referenceHydrationRequested([]string{"hydration_only"}))
}

// TestGeneratedFixedSuffixExpansionReferencesUseDeclaredDepthAndObservation verifies propagation of maximum depth and selection of ID-row versus fully hydrated path output SQL.
func TestGeneratedFixedSuffixExpansionReferencesUseDeclaredDepthAndObservation(t *testing.T) {
	minDepth, maxDepth := 0, 16
	runner := &postgresSQLRunner{}
	testCase := ScaleCase{
		Name:     "generated_fixed_suffix_expansion_endpoint_d16_f1000",
		Category: "generated_fixed_suffix_expansion",
		Expected: ExpectedResult{ResultKind: "id_rows"},
		Shape: WorkloadShape{
			MinDepth: &minDepth,
			MaxDepth: &maxDepth,
		},
	}
	// Reference routing occurs before kind mapping; the generated category is
	// asserted separately from the SQL builder so this remains a unit test.
	require.NotNil(t, runner)
	specs := buildFixedSuffixExpansionReferenceSpecs(testCase, map[string]any{"min_depth": int32(0), "max_depth": int32(16)})
	require.Contains(t, specs[referenceSpecIndex(specs, "complete_reference")].sql, "select head_id, terminal_id")
	require.NotContains(t, specs[referenceSpecIndex(specs, "complete_reference")].sql, "ordered_edge_ids_to_path")
	require.Equal(t, int32(16), specs[referenceSpecIndex(specs, "suffix_seeded_reverse_ordered_ids")].parameters["max_depth"])

	testCase.Observes.Paths = true
	testCase.Expected.ResultKind = "path_set"
	pathSpecs := buildFixedSuffixExpansionReferenceSpecs(testCase, map[string]any{"min_depth": int32(0), "max_depth": int32(16)})
	require.Contains(t, pathSpecs[referenceSpecIndex(pathSpecs, "suffix_seeded_reverse_complete")].sql, "ordered_edge_ids_to_path")
}

// TestParseConfigValidatesPostgresReferenceArmSelector verifies ordered arm selection, implicit reference enablement, and rejection of unknown or duplicate arm names.
func TestParseConfigValidatesPostgresReferenceArmSelector(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-reference-arms", "suffix_seeded_reverse_ordered_ids,factored_suffix_forward_complete"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresReferences)
	require.Equal(t, []string{"suffix_seeded_reverse_ordered_ids", "factored_suffix_forward_complete"}, cfg.PostgresReferenceArms)

	_, err = parseConfig([]string{"-postgres-reference-arms", "does_not_exist"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unknown PostgreSQL reference arm")
	_, err = parseConfig([]string{"-postgres-reference-arms", "round_trip,round_trip"}, func(string) string { return "" })
	require.ErrorContains(t, err, "duplicate PostgreSQL reference arm")
}

// TestRequestedReferenceArmCannotDisappearFromCase verifies that an explicitly requested arm must be available for the particular workload shape.
func TestRequestedReferenceArmCannotDisappearFromCase(t *testing.T) {
	_, err := selectReferenceSpecs([]postgresReferenceSpec{{name: "available"}}, []string{"missing"})
	require.ErrorContains(t, err, `requested PostgreSQL reference arm "missing" is unavailable`)
}

// TestReferenceIdentityRejectsUndeclaredDuplicateSQL verifies that normalized duplicate SQL requires an explicit A/A alias linking the second arm to the first.
func TestReferenceIdentityRejectsUndeclaredDuplicateSQL(t *testing.T) {
	specs := []postgresReferenceSpec{
		normalizedReferenceSpec(postgresReferenceSpec{
			name:             "one",
			architecture:     "SP-S1",
			stateShape:       "state",
			observationShape: "ordered_ids",
			sql:              "select 1",
		}),
		normalizedReferenceSpec(postgresReferenceSpec{
			name:             "two",
			architecture:     "SP-S2",
			stateShape:       "state",
			observationShape: "ordered_ids",
			sql:              " select  1 ",
		}),
	}
	require.ErrorContains(t, validateReferenceSpecs(specs), "without a declared A/A alias")

	specs[1].aaAliasOf = "one"
	require.NoError(t, validateReferenceSpecs(specs))
}

// TestReferenceIdentityRejectsImplementationShapeDrift verifies that a shared implementation ID cannot describe different state shapes or SQL bodies.
func TestReferenceIdentityRejectsImplementationShapeDrift(t *testing.T) {
	specs := []postgresReferenceSpec{
		normalizedReferenceSpec(postgresReferenceSpec{
			name:             "one",
			architecture:     "SP-S1",
			implementationID: "same",
			stateShape:       "edge IDs",
			observationShape: "ordered_ids",
			sql:              "select 1",
		}),
		normalizedReferenceSpec(postgresReferenceSpec{
			name:             "two",
			architecture:     "SP-S1",
			implementationID: "same",
			stateShape:       "node and edge IDs",
			observationShape: "ordered_ids",
			sql:              "select 2",
		}),
	}
	require.ErrorContains(t, validateReferenceSpecs(specs), "changes state, observation, or SQL identity")
}

// TestFixedSuffixExpansionRootReuseIsExplicitAAAlias verifies that root-reuse arms declare their byte-equivalent ordered-ID and complete-reference counterparts.
func TestFixedSuffixExpansionRootReuseIsExplicitAAAlias(t *testing.T) {
	specs := buildFixedSuffixExpansionReferenceSpecs(ScaleCase{
		Name: "fixed_suffix_expansion_endpoint_ids",
	}, map[string]any{"graph_id": int32(42)})
	for idx := range specs {
		specs[idx] = normalizedReferenceSpec(specs[idx])
	}
	require.NoError(t, validateReferenceSpecs(specs))
	require.Equal(t, "search_ordered_ids", specs[referenceSpecIndex(specs, "root_reuse_ordered_ids")].aaAliasOf)
	require.Equal(t, "complete_reference", specs[referenceSpecIndex(specs, "root_reuse_complete")].aaAliasOf)
}

// TestFixedSuffixExpansionOrderedIDReferencesValidateAgainstCanonicalObservation verifies that every ordered-ID strategy uses the canonical search SQL and parameters for semantic validation.
func TestFixedSuffixExpansionOrderedIDReferencesValidateAgainstCanonicalObservation(t *testing.T) {
	specs := buildFixedSuffixExpansionReferenceSpecs(ScaleCase{
		Name: "fixed_suffix_expansion_endpoint_ids",
	}, map[string]any{"graph_id": int32(42)})
	canonical := specs[referenceSpecIndex(specs, "search_ordered_ids")]

	for _, name := range []string{
		"search_ordered_ids",
		"factored_suffix_forward_ordered_ids",
		"suffix_seeded_reverse_ordered_ids",
		"backward_viability_forward_ordered_ids",
	} {
		spec := specs[referenceSpecIndex(specs, name)]
		require.Equal(t, "exact_ordered_ids", spec.semanticValidation)
		require.Equal(t, canonical.sql, spec.validationSQL)
		require.Equal(t, canonical.parameters, spec.validationParams)
	}
}

// TestReferenceInt64SliceAcceptsDriverArrayRepresentations verifies normalization of int64, int32, and mixed driver arrays while rejecting nonnumeric elements with their index.
func TestReferenceInt64SliceAcceptsDriverArrayRepresentations(t *testing.T) {
	require.Equal(t, []int64{1, 2}, mustReferenceInt64Slice(t, []int64{1, 2}))
	require.Equal(t, []int64{3, 4}, mustReferenceInt64Slice(t, []int32{3, 4}))
	require.Equal(t, []int64{5, 6}, mustReferenceInt64Slice(t, []any{int64(5), int32(6)}))
	_, err := referenceInt64Slice([]any{"not-an-id"})
	require.ErrorContains(t, err, "array item 0")
}

// mustReferenceInt64Slice converts a reference value to integers and fails the test on invalid input.
func mustReferenceInt64Slice(t *testing.T, value any) []int64 {
	t.Helper()
	result, err := referenceInt64Slice(value)
	require.NoError(t, err)
	return result
}
