// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// TestExpandIntoReferencesShareExactRelationshipBoundary verifies all three study arms preserve rows, cross-kind matches, and duplicate input multiplicity.
func TestExpandIntoReferencesShareExactRelationshipBoundary(t *testing.T) {
	params := map[string]any{
		"graph_id": int32(7), "start_ids": []int64{1, 2, 1}, "end_id": int64(3), "edge_kind_ids": []int16{4, 5},
	}
	specs := buildExpandIntoReferenceSpecs(params, "outbound")
	require.Len(t, specs, 3)
	for idx := range specs {
		specs[idx] = normalizedReferenceSpec(specs[idx])
	}
	require.NoError(t, validateReferenceSpecs(specs))

	pairJoin := specs[referenceSpecIndex(specs, "expand_into_pair_join")]
	require.True(t, pairJoin.fullComparator)
	require.Contains(t, pairJoin.sql, "unnest(@start_ids::int8[]) with ordinality")
	require.Contains(t, pairJoin.sql, "matched.start_id = input_pairs.start_id and matched.end_id = input_pairs.end_id")
	require.Contains(t, pairJoin.sql, "matched.kind_id = any(@edge_kind_ids::int2[])")

	lowerDegree := specs[referenceSpecIndex(specs, "expand_into_lower_degree_scan")]
	require.Contains(t, lowerDegree.sql, "degrees as materialized")
	require.Contains(t, lowerDegree.sql, "degrees.start_degree <= degrees.end_degree")
	require.Contains(t, lowerDegree.sql, "degrees.end_degree < degrees.start_degree")
	require.Contains(t, lowerDegree.sql, "union all")

	cache := specs[referenceSpecIndex(specs, "expand_into_pair_cache")]
	require.Contains(t, cache.sql, "select distinct start_id, end_id from input_pairs")
	require.Contains(t, cache.sql, "pair_matches")
	require.Contains(t, cache.observationShape, "duplicate outer-row multiplicity")
	for _, spec := range specs {
		require.Equal(t, "exact_public_observation", spec.semanticValidation)
		require.Equal(t, params, spec.parameters)
	}
}

// TestExpandIntoReferencesPreserveInboundAndDirectionlessPairs verifies every
// study arm uses the same physical pair semantics and does not double-count a
// directionless self-loop.
func TestExpandIntoReferencesPreserveInboundAndDirectionlessPairs(t *testing.T) {
	inbound := buildExpandIntoReferenceSpecs(map[string]any{}, "inbound")
	require.Contains(t, inbound[0].sql, "matched.end_id = input_pairs.start_id")
	require.Contains(t, inbound[1].sql, "outbound.end_id = input_pairs.start_id")
	require.Contains(t, inbound[1].sql, "inbound.start_id = input_pairs.end_id")
	require.Contains(t, inbound[2].sql, "matched.end_id = distinct_pairs.start_id")

	directionless := buildExpandIntoReferenceSpecs(map[string]any{}, "directionless")
	for _, spec := range directionless {
		require.Contains(t, spec.sql, " or (")
		require.NotContains(t, spec.sql, "union all\n    select matched")
	}
	require.Contains(t, directionless[1].sql, "degrees.start_degree <= degrees.end_degree")
	require.Contains(t, directionless[1].sql, "degrees.end_degree < degrees.start_degree")
}

// TestExpandIntoScaleCasesParse verifies the shared three-way study corpus remains valid Cypher input.
func TestExpandIntoScaleCasesParse(t *testing.T) {
	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	found := 0
	stateClasses := map[string]bool{}
	for _, testCase := range corpus.Cases {
		if testCase.Category != "expand_into_one_hop" {
			continue
		}
		found++
		stateClasses[testCase.Shape.ExpectedStateClass] = true
		_, err := frontend.ParseCypher(frontend.NewContext(), testCase.Cypher)
		require.NoError(t, err, testCase.Name)
	}
	require.Equal(t, 11, found)
	require.True(t, stateClasses["source_lower_degree"])
	require.True(t, stateClasses["target_lower_degree"])
}

// TestExpandIntoReferenceArmsAreDeclared verifies command-line selection accepts every three-way study arm.
func TestExpandIntoReferenceArmsAreDeclared(t *testing.T) {
	for _, name := range []string{"expand_into_pair_join", "expand_into_lower_degree_scan", "expand_into_pair_cache"} {
		require.True(t, validPostgresReferenceArm(name), name)
	}
}
