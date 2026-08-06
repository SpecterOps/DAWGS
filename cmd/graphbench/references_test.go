// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShortestReferenceSpecsAreGraphScopedAndSeparateRawFromFullOutput(t *testing.T) {
	params := map[string]any{"graph_id": int32(42), "start_id": int64(1), "end_id": int64(2), "max_depth": int32(15)}
	specs := buildShortestReferenceSpecs(ScaleCase{Name: "one_shortest_path_bound_pair"}, params, []int64{10, 11})

	require.Len(t, specs, 7)
	require.Equal(t, "round_trip", specs[0].name)
	require.Equal(t, int32(42), specs[1].parameters["graph_id"])
	require.Equal(t, "minimum_graph_access", specs[2].name)
	require.Contains(t, specs[3].sql, "e.graph_id = @graph_id")
	require.Contains(t, specs[3].boundary, "ordered node/edge IDs")
	require.Equal(t, []int64{10, 11}, specs[4].parameters["edge_ids"])
	require.True(t, specs[5].fullComparator)
	require.Equal(t, "s3_unidirectional_trail_cte", specs[5].name)
	require.Equal(t, "complete_reference_s1_array_cte", specs[5].legacyName)
	require.Equal(t, "S3-U", specs[5].architecture)
	require.Contains(t, specs[5].sql, "ordered_edge_ids_to_path")
	require.Equal(t, "s3_bidirectional_trail_cte", specs[6].name)
	require.Equal(t, "candidate_s2_bidirectional_cte", specs[6].legacyName)
	require.Equal(t, "S3-B", specs[6].architecture)
	require.True(t, specs[6].fullComparator)
	require.Contains(t, specs[6].sql, "forward join backward")
	require.Contains(t, specs[6].sql, "e.graph_id = @graph_id")
	require.Contains(t, specs[6].sql, "edge_id = any(backward.edge_ids)")
}

func TestShortestDistanceReferenceCarriesNoTrailOrPredecessorState(t *testing.T) {
	specs := buildShortestReferenceSpecs(ScaleCase{Name: "shortest_distance_bound_pair", Expected: ExpectedResult{ResultKind: "scalar"}}, map[string]any{}, nil)
	reference := specs[len(specs)-2]

	require.Equal(t, "distance frontier node and depth only; no path or predecessor state", reference.stateShape)
	require.Contains(t, reference.sql, "search(node_id, depth)")
	require.NotContains(t, reference.sql, "node_ids")
	require.NotContains(t, reference.sql, "edge_ids")
}

func TestADCSReferenceSpecsAvoidAmbiguousArrayContainmentOperators(t *testing.T) {
	specs := buildADCSReferenceSpecs(ScaleCase{Name: "adcs_p1_endpoint_ids"}, map[string]any{"graph_id": int32(42)})

	require.Len(t, specs, 5)
	for _, spec := range specs {
		require.NotContains(t, spec.sql, " @> ")
	}
	require.Contains(t, specs[1].sql, "= any(n.kind_ids)")
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
