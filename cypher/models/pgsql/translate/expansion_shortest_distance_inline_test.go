// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"context"
	"strings"
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

func TestGuardedDistanceV2ConsolidatesAdmissionAndDominatesEqualFrontierCap(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), guardedDistanceToolQuery)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), query, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor:    optimize.ShortestPathExecutorI2GuardedDistanceV2,
		GuardedDistanceStateLimit:    10,
		GuardedDistanceFrontierLimit: 10,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.NotContains(t, formatted, "group by sp_i2_distance_bounded.depth")
	require.Contains(t, formatted, "true as frontier_guard_dominated")
	require.Less(t, strings.Index(formatted, "sp_i2_admission as materialized"), strings.Index(formatted, "sp_i2_target(depth) as materialized"))
	require.Contains(t, formatted, "else 'SP-I2-C-D-V2' end")
}

func TestGuardedDistanceV2RetainsOneIndependentFrontierCheckForUnequalCaps(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), guardedDistanceToolQuery)
	require.NoError(t, err)
	translation, err := TranslateForTool(context.Background(), query, optimizerSafetyKindMapper(), map[string]any{
		"start_id": int64(1), "end_id": int64(2),
	}, DefaultGraphID, ToolOptions{
		ForceShortestPathExecutor:    optimize.ShortestPathExecutorI2GuardedDistanceV2E1,
		GuardedDistanceStateLimit:    20,
		GuardedDistanceFrontierLimit: 10,
	})
	require.NoError(t, err)
	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(formatted, "group by sp_i2_distance_bounded.depth"))
	require.Contains(t, formatted, "false as frontier_guard_dominated")
	require.Contains(t, formatted, "limit 21")
	require.Contains(t, formatted, "having count(*)::int8 > 10")
	require.Contains(t, formatted, "else 'SP-I2-C-D-V2-E1' end")
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
