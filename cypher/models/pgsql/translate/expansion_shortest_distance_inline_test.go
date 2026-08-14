// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

const guardedDistanceToolQuery = `
	MATCH p = shortestPath((s)<-[:MemberOf*1..32]-(e))
	WHERE id(s) = $start_id AND id(e) = $end_id
	RETURN length(p)
`

// TestGuardedDistanceToolCapsDriveBothAdmissionSentinels verifies reduced-cap
// fallback can be exercised diagnostically without relaxing production caps.
func TestGuardedDistanceToolCapsDriveBothAdmissionSentinels(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), guardedDistanceToolQuery)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), query, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor:    optimize.ShortestPathExecutorI2GuardedDistance,
		GuardedDistanceStateLimit:    10,
		GuardedDistanceFrontierLimit: 10,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)

	decision := translation.Optimization.LoweringPlan.ShortestPathExecutor[0]
	require.Equal(t, int64(10), decision.StateLimit)
	require.Equal(t, int64(10), decision.FrontierLimit)
	require.Contains(t, formatted, "limit 11")
	require.Contains(t, formatted, "offset 10 limit 1")
	require.Contains(t, formatted, "having count(*)::int8 > 10")
	require.Contains(t, formatted, "shortest_path_compact(")
}

// TestGuardedDistanceToolCapsAreIsolated rejects partial, negative, and
// unrelated overrides before they can mutate an optimized plan.
func TestGuardedDistanceToolCapsAreIsolated(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), guardedDistanceToolQuery)
	require.NoError(t, err)
	plan, err := optimize.Optimize(query)
	require.NoError(t, err)

	for _, options := range []ToolOptions{
		{GuardedDistanceStateLimit: 10, GuardedDistanceFrontierLimit: 10},
		{ForceShortestPathExecutor: optimize.ShortestPathExecutorI2GuardedDistance, GuardedDistanceStateLimit: 10},
		{ForceShortestPathExecutor: optimize.ShortestPathExecutorI2GuardedDistance, GuardedDistanceStateLimit: -1, GuardedDistanceFrontierLimit: 10},
	} {
		planCopy := plan
		require.Error(t, applyToolOptions(&planCopy, options))
	}
}
