// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const outboundShortestPathQuery = "MATCH p = shortestPath((s)-[*0..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p"

func TestShortestReferenceSpecsAreGraphScopedAndSeparateRawFromFullOutput(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(2), "max_depth": int32(15)}
	specs := buildShortestReferenceSpecs(ScaleCase{Name: "one_shortest_path_bound_pair", Cypher: outboundShortestPathQuery}, params, []int64{1, 2, 3}, []int64{10, 11}, graph.DirectionOutbound)

	require.Len(t, specs, 11)
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

func TestShortestDistanceReferenceCarriesNoTrailOrPredecessorState(t *testing.T) {
	specs := buildShortestReferenceSpecs(ScaleCase{Name: "shortest_distance_bound_pair", Expected: ExpectedResult{ResultKind: "scalar"}}, map[string]any{}, nil, nil, graph.DirectionOutbound)
	reference := specs[len(specs)-2]

	require.Equal(t, "distance frontier node and depth only; no path or predecessor state", reference.stateShape)
	require.Contains(t, reference.sql, "search(node_id, depth)")
	require.NotContains(t, reference.sql, "node_ids")
	require.NotContains(t, reference.sql, "edge_ids")
}

func TestShortestS1DistancePrototypeIsDistinctBoundedAndFallsBack(t *testing.T) {
	minDepth, maxDepth := 1, 8
	params := map[string]any{
		"graph_id": int32(1), "start_id": int64(10), "end_id": int64(20),
		"min_depth": int32(1), "max_depth": int32(8), "edge_kind_ids": []int16{2},
	}
	testCase := ScaleCase{
		Name: "distance", Expected: ExpectedResult{ResultKind: "scalar"},
		Shape: WorkloadShape{MinDepth: &minDepth, MaxDepth: &maxDepth},
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

func TestShortestS1DistancePrototypeRejectsUnsupportedShapes(t *testing.T) {
	minDepth, maxDepth := 2, 8
	params := map[string]any{"start_id": int64(10), "end_id": int64(20)}
	distance := ScaleCase{Expected: ExpectedResult{ResultKind: "scalar"}, Shape: WorkloadShape{MinDepth: &minDepth, MaxDepth: &maxDepth}}
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(distance, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))

	minDepth = 1
	path := ScaleCase{Expected: ExpectedResult{ResultKind: "path_set"}, Shape: WorkloadShape{MinDepth: &minDepth, MaxDepth: &maxDepth}}
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(path, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))

	params["end_id"] = int64(10)
	require.Equal(t, -1, referenceSpecIndexOrMissing(buildShortestReferenceSpecs(distance, params, nil, nil, graph.DirectionOutbound), "s1_array_bfs_distance"))
}

func TestShortestPathReferencesCompareM0AndM1WithMinimalSearchState(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(3), "max_depth": int32(4)}
	specs := buildShortestReferenceSpecs(
		ScaleCase{Name: "one_shortest_path_bound_pair", Cypher: outboundShortestPathQuery},
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

func TestShortestReferenceIdentitiesAndInboundMinimalState(t *testing.T) {
	specs := buildShortestReferenceSpecs(
		ScaleCase{Name: "one_shortest_path_bound_pair", Cypher: "MATCH p = shortestPath((s)<-[*1..4]-(e)) RETURN p", Expected: ExpectedResult{ResultKind: "path_set"}},
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

func TestShortestPathMaterializerOnlyReferencesExcludeSearch(t *testing.T) {
	specs := buildShortestReferenceSpecs(
		ScaleCase{Name: "one_shortest_path_bound_pair", Cypher: outboundShortestPathQuery},
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
		ScaleCase{Name: "zero_shortest_path", Cypher: outboundShortestPathQuery, Expected: ExpectedResult{ResultKind: "path_set"}},
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

func TestShortestMaterializersRequireProvablyOutboundPattern(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		query     string
		outbound  bool
		supported bool
	}{
		{name: "outbound", query: "MATCH p = shortestPath((s)-[*1..4]->(e)) RETURN p", outbound: true, supported: true},
		{name: "inbound", query: "MATCH p = shortestPath((s)<-[*1..4]-(e)) RETURN p", supported: true},
		{name: "directionless", query: "MATCH p = shortestPath((s)-[*1..4]-(e)) RETURN p"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			direction, err := shortestReferenceDirection(testCase.query)
			require.NoError(t, err)
			require.Equal(t, testCase.outbound, direction == graph.DirectionOutbound)

			specs := buildShortestReferenceSpecs(
				ScaleCase{Cypher: testCase.query, Expected: ExpectedResult{ResultKind: "path_set"}},
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

func TestShortestReferenceEndpointParametersFollowPatternRootOrder(t *testing.T) {
	for _, testCase := range []struct {
		name, query, root, terminal string
	}{
		{name: "outbound", query: `MATCH p = shortestPath((s)-[:Traverse*1..8]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`, root: "start_id", terminal: "end_id"},
		{name: "inbound same symbols", query: `MATCH p = shortestPath((s)<-[:Traverse*1..8]-(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`, root: "start_id", terminal: "end_id"},
		{name: "inbound reversed symbols", query: `MATCH p = shortestPath((e)<-[:Traverse*1..8]-(s)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)`, root: "end_id", terminal: "start_id"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root, terminal, err := shortestReferenceEndpointParameters(testCase.query)
			require.NoError(t, err)
			require.Equal(t, testCase.root, root)
			require.Equal(t, testCase.terminal, terminal)
		})
	}
}

func TestAlternativeOneShortestPathTieIsSemanticallyValid(t *testing.T) {
	testCase := ScaleCase{Cypher: outboundShortestPathQuery, Expected: ExpectedResult{ResultKind: "path_set"}, Shape: WorkloadShape{EdgeKinds: []string{"Edge"}}}
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

func TestReferenceSpecsAlternateOrderByRound(t *testing.T) {
	specs := []postgresReferenceSpec{{name: "first"}, {name: "second"}, {name: "third"}}
	require.Equal(t, []postgresReferenceSpec{{name: "first"}, {name: "second"}, {name: "third"}}, referenceSpecsForRound(specs, 1))
	require.Equal(t, []postgresReferenceSpec{{name: "third"}, {name: "second"}, {name: "first"}}, referenceSpecsForRound(specs, 2))
	require.Equal(t, "first", specs[0].name)
}

func TestFiveArmReferenceSpecsUsePredeclaredBalancedSchedule(t *testing.T) {
	specs := []postgresReferenceSpec{{name: "T1"}, {name: "T2"}, {name: "T3"}, {name: "T4"}, {name: "T5"}}
	require.Equal(t, []string{"T1", "T2", "T5", "T3", "T4"}, referenceSpecNames(referenceSpecsForRound(specs, 1)))
	require.Equal(t, []string{"T4", "T3", "T5", "T2", "T1"}, referenceSpecNames(referenceSpecsForRound(specs, 6)))
	require.Equal(t, []string{"T1", "T2", "T5", "T3", "T4"}, referenceSpecNames(referenceSpecsForRound(specs, 11)))
}

func referenceSpecNames(specs []postgresReferenceSpec) []string {
	names := make([]string, len(specs))
	for idx, spec := range specs {
		names[idx] = spec.name
	}
	return names
}

func TestAllShortestPathCaseDoesNotUseSingletonReferences(t *testing.T) {
	runner := &postgresSQLRunner{}
	specs, err := runner.referenceSpecs(context.Background(), ScaleCase{
		Category: "generated_shortest_path",
		Cypher:   "MATCH p = allShortestPaths((s)-[*1..2]->(e)) RETURN p",
	}, nil)

	require.NoError(t, err)
	require.Empty(t, specs)
}

func TestADCSReferenceSpecsAvoidAmbiguousArrayContainmentOperators(t *testing.T) {
	specs := buildADCSReferenceSpecs(ScaleCase{Name: "adcs_p1_endpoint_ids"}, map[string]any{"graph_id": int32(42)})

	require.Len(t, specs, 17)
	for _, spec := range specs {
		require.NotContains(t, spec.sql, " @> ")
	}
	require.Contains(t, specs[1].sql, "= any(n.kind_ids)")
	require.Contains(t, specs[referenceSpecIndex(specs, "a3_suffix_seeded_reverse_ordered_ids")].sql, "array_prepend(e.id, reverse_trails.edge_ids)")
	require.Contains(t, specs[referenceSpecIndex(specs, "a3_suffix_seeded_reverse_ordered_ids")].sql, "union all")
	require.Contains(t, specs[referenceSpecIndex(specs, "a4_viability_forward_ordered_ids")].sql, "viable(node_id, reverse_distance)")
	require.Contains(t, specs[referenceSpecIndex(specs, "a2_factored_suffix_forward_ordered_ids")].sql, "suffix_rows")
}

func TestGeneratedADCSReferencesUseDeclaredDepthAndObservation(t *testing.T) {
	minDepth, maxDepth := 0, 16
	runner := &postgresSQLRunner{}
	testCase := ScaleCase{
		Name: "generated_adcs_endpoint_d16_f1000", Category: "generated_adcs",
		Expected: ExpectedResult{ResultKind: "id_rows"},
		Shape:    WorkloadShape{MinDepth: &minDepth, MaxDepth: &maxDepth},
	}
	// Reference routing occurs before kind mapping; the generated category is
	// asserted separately from the SQL builder so this remains a unit test.
	require.NotNil(t, runner)
	specs := buildADCSReferenceSpecs(testCase, map[string]any{"min_depth": int32(0), "max_depth": int32(16)})
	require.Contains(t, specs[referenceSpecIndex(specs, "complete_reference")].sql, "select ca_id, domain_id")
	require.NotContains(t, specs[referenceSpecIndex(specs, "complete_reference")].sql, "ordered_edge_ids_to_path")
	require.Equal(t, int32(16), specs[referenceSpecIndex(specs, "a3_suffix_seeded_reverse_ordered_ids")].parameters["max_depth"])

	testCase.Observes.Paths = true
	testCase.Expected.ResultKind = "path_set"
	pathSpecs := buildADCSReferenceSpecs(testCase, map[string]any{"min_depth": int32(0), "max_depth": int32(16)})
	require.Contains(t, pathSpecs[referenceSpecIndex(pathSpecs, "a3_suffix_seeded_reverse_complete")].sql, "ordered_edge_ids_to_path")
}

func TestParseConfigValidatesPostgresReferenceArmSelector(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-reference-arms", "a3_suffix_seeded_reverse_ordered_ids,a2_factored_suffix_forward_complete"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.PostgresReferences)
	require.Equal(t, []string{"a3_suffix_seeded_reverse_ordered_ids", "a2_factored_suffix_forward_complete"}, cfg.PostgresReferenceArms)

	_, err = parseConfig([]string{"-postgres-reference-arms", "does_not_exist"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unknown PostgreSQL reference arm")
	_, err = parseConfig([]string{"-postgres-reference-arms", "round_trip,round_trip"}, func(string) string { return "" })
	require.ErrorContains(t, err, "duplicate PostgreSQL reference arm")
}

func TestRequestedReferenceArmCannotDisappearFromCase(t *testing.T) {
	_, err := selectReferenceSpecs([]postgresReferenceSpec{{name: "available"}}, []string{"missing"})
	require.ErrorContains(t, err, `requested PostgreSQL reference arm "missing" is unavailable`)
}

func TestReferenceIdentityRejectsUndeclaredDuplicateSQL(t *testing.T) {
	specs := []postgresReferenceSpec{
		normalizedReferenceSpec(postgresReferenceSpec{name: "one", architecture: "SP-S1", stateShape: "state", observationShape: "ordered_ids", sql: "select 1"}),
		normalizedReferenceSpec(postgresReferenceSpec{name: "two", architecture: "SP-S2", stateShape: "state", observationShape: "ordered_ids", sql: " select  1 "}),
	}
	require.ErrorContains(t, validateReferenceSpecs(specs), "without a declared A/A alias")

	specs[1].aaAliasOf = "one"
	require.NoError(t, validateReferenceSpecs(specs))
}

func TestReferenceIdentityRejectsImplementationShapeDrift(t *testing.T) {
	specs := []postgresReferenceSpec{
		normalizedReferenceSpec(postgresReferenceSpec{name: "one", architecture: "SP-S1", implementationID: "same", stateShape: "edge IDs", observationShape: "ordered_ids", sql: "select 1"}),
		normalizedReferenceSpec(postgresReferenceSpec{name: "two", architecture: "SP-S1", implementationID: "same", stateShape: "node and edge IDs", observationShape: "ordered_ids", sql: "select 2"}),
	}
	require.ErrorContains(t, validateReferenceSpecs(specs), "changes state, observation, or SQL identity")
}

func TestADCSHistoricalA1AIsExplicitAAAlias(t *testing.T) {
	specs := buildADCSReferenceSpecs(ScaleCase{Name: "adcs_p1_endpoint_ids"}, map[string]any{"graph_id": int32(42)})
	for idx := range specs {
		specs[idx] = normalizedReferenceSpec(specs[idx])
	}
	require.NoError(t, validateReferenceSpecs(specs))
	require.Equal(t, "search_ordered_ids", specs[referenceSpecIndex(specs, "a1a_root_reuse_ordered_ids")].aaAliasOf)
	require.Equal(t, "complete_reference", specs[referenceSpecIndex(specs, "a1a_root_reuse_complete")].aaAliasOf)
}

func TestADCSOrderedIDReferencesValidateAgainstCanonicalObservation(t *testing.T) {
	specs := buildADCSReferenceSpecs(ScaleCase{Name: "adcs_p1_endpoint_ids"}, map[string]any{"graph_id": int32(42)})
	canonical := specs[referenceSpecIndex(specs, "search_ordered_ids")]

	for _, name := range []string{
		"search_ordered_ids",
		"a2_factored_suffix_forward_ordered_ids",
		"a3_suffix_seeded_reverse_ordered_ids",
		"a4_viability_forward_ordered_ids",
	} {
		spec := specs[referenceSpecIndex(specs, name)]
		require.Equal(t, "exact_ordered_ids", spec.semanticValidation)
		require.Equal(t, canonical.sql, spec.validationSQL)
		require.Equal(t, canonical.parameters, spec.validationParams)
	}
}

func TestReferenceInt64SliceAcceptsDriverArrayRepresentations(t *testing.T) {
	require.Equal(t, []int64{1, 2}, mustReferenceInt64Slice(t, []int64{1, 2}))
	require.Equal(t, []int64{3, 4}, mustReferenceInt64Slice(t, []int32{3, 4}))
	require.Equal(t, []int64{5, 6}, mustReferenceInt64Slice(t, []any{int64(5), int32(6)}))
	_, err := referenceInt64Slice([]any{"not-an-id"})
	require.ErrorContains(t, err, "array item 0")
}

func mustReferenceInt64Slice(t *testing.T, value any) []int64 {
	t.Helper()
	result, err := referenceInt64Slice(value)
	require.NoError(t, err)
	return result
}
