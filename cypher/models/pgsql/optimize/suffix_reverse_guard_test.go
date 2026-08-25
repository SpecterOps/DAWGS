// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package optimize

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/specterops/dawgs/cypher/frontend"
)

const suffixReverseGuardPathQuery = `
	MATCH (root:ExpansionRoot)
	WHERE root.root_key = $root_key
	MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
	RETURN path
`

func optimizeSuffixReverseGuardQuery(t *testing.T, query string) ExpansionSearchStrategyDecision {
	t.Helper()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	return plan.LoweringPlan.ExpansionSearchStrategy[0]
}

// TestFixedSuffixObservationDistinguishesFullPathFromEndpoint verifies the
// static fact consumed by suffix-reverse-guard-v1 is available before tooling
// overrides are applied.
func TestFixedSuffixObservationDistinguishesFullPathFromEndpoint(t *testing.T) {
	fullPath := optimizeSuffixReverseGuardQuery(t, suffixReverseGuardPathQuery)
	require.True(t, fullPath.StructurallyEligible)
	require.True(t, fullPath.StaticallyEligible)
	require.Equal(t, ExpansionSearchObservationFullPath, fullPath.ObservationMode)

	endpoint := optimizeSuffixReverseGuardQuery(t, `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN id(terminal)
	`)
	require.True(t, endpoint.StructurallyEligible)
	require.True(t, endpoint.StaticallyEligible)
	require.Equal(t, ExpansionSearchObservationEndpointIDs, endpoint.ObservationMode)
}

// TestSuffixReverseGuardConstantsAreIndependent verifies the feasibility lane
// cannot silently inherit orientation-v2's policy or state limit identity.
func TestSuffixReverseGuardConstantsAreIndependent(t *testing.T) {
	require.Equal(t, ExpansionSearchPolicy("suffix-reverse-guard-v1"), ExpansionSearchPolicySuffixReverseGuardV1)
	require.Equal(t, "fixed-suffix-path-static-v1", ExpansionSearchSelectorFixedSuffixPathV1)
	require.Positive(t, ExpansionSearchSuffixReverseGuardSuffixRowLimit)
	require.Positive(t, ExpansionSearchSuffixReverseGuardStateLimit)
	require.NotEqual(t, ExpansionSearchPolicyOrientationProbeV2, ExpansionSearchPolicySuffixReverseGuardV1)
	require.NotEqual(t, ExpansionSearchOrientationStateLimit, ExpansionSearchSuffixReverseGuardStateLimit)
}
