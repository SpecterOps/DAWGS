// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParsePostgresPlanJSONMetricsWalksStructuredNodes verifies extraction of root timings, buffer use, recursive cardinality, labeled CTE rows, index probes, and provenance from nested plan JSON.
func TestParsePostgresPlanJSONMetricsWalksStructuredNodes(t *testing.T) {
	raw := json.RawMessage(`[{
  "Plan": {
    "Node Type": "Recursive Union", "Plan Rows": 12, "Plan Width": 64,
    "Actual Rows": 19, "Actual Loops": 1, "Shared Hit Blocks": 40,
    "Plans": [
      {"Node Type":"CTE Scan", "CTE Name":"roots", "Alias":"roots", "Actual Rows":1, "Actual Loops":1},
      {"Node Type":"Index Only Scan", "Relation Name":"edge_1", "Alias":"e", "Index Name":"edge_1_end_id_kind_id_idx", "Index Cond":"(end_id = reverse_trails.node_id)", "Actual Rows":1, "Actual Loops":18, "Shared Hit Blocks":36},
      {"Node Type":"Index Scan", "Relation Name":"node_1", "Alias":"boundary", "Actual Rows":2, "Actual Loops":1}
    ]
  },
  "Planning Time": 1.25,
  "Execution Time": 2.5
}]`)

	metrics, err := parsePostgresPlanJSONMetrics(raw)
	require.NoError(t, err)
	require.Equal(t, 1.25, *metrics.PlanningMS)
	require.Equal(t, 2.5, *metrics.ExecutionMS)
	require.Equal(t, int64(40), metrics.Buffers.SharedHit)
	require.Equal(t, int64(19), metrics.RecursiveRows)
	require.Equal(t, int64(18), metrics.ReverseEdgeProbes)
	require.Equal(t, int64(1), metrics.RootRows)
	require.Equal(t, int64(1), metrics.BoundaryLookupLoops)
	require.Len(t, metrics.PlanNodes, 4)
	require.Equal(t, int64(1), metrics.PlanNodes[0].PlanNodeID)
	require.Zero(t, metrics.PlanNodes[0].ParentPlanNodeID)
	for idx := 1; idx < len(metrics.PlanNodes); idx++ {
		require.Equal(t, int64(idx+1), metrics.PlanNodes[idx].PlanNodeID)
		require.Equal(t, int64(1), metrics.PlanNodes[idx].ParentPlanNodeID)
	}
	require.Equal(t, "measured_plan_json", metrics.PlanNodes[0].Provenance)
	require.Equal(t, "plan_derived_index_loops", metrics.Provenance["reverse_edge_probes"])
}

// TestParsePostgresPlanJSONMetricsRejectsMissingPlan verifies that timing metadata alone is not accepted as a PostgreSQL execution plan.
func TestParsePostgresPlanJSONMetricsRejectsMissingPlan(t *testing.T) {
	_, err := parsePostgresPlanJSONMetrics(json.RawMessage(`[{"Planning Time":1}]`))
	require.ErrorContains(t, err, "missing its root Plan")
}

// TestParsePostgresPlanJSONMetricsRetainsDirectPlanParentage verifies parse postgres plan json metrics retains direct plan parentage behavior.
func TestParsePostgresPlanJSONMetricsRetainsDirectPlanParentage(t *testing.T) {
	raw := json.RawMessage(`[{
  "Plan": {"Node Type":"Append","Actual Rows":1,"Actual Loops":1,"Plans":[
    {"Node Type":"Nested Loop","Parent Relationship":"InitPlan","Subplan Name":"CTE asp_i1_candidate_rows","Actual Rows":1,"Actual Loops":1,"Plans":[
      {"Node Type":"CTE Scan","Parent Relationship":"Outer","CTE Name":"asp_i1_candidate_marker","Actual Rows":1,"Actual Loops":1},
      {"Node Type":"Result","Parent Relationship":"Inner","Actual Rows":1,"Actual Loops":1,"Plans":[
        {"Node Type":"Function Scan","Parent Relationship":"Outer","Function Name":"shortest_path_compact","Actual Rows":1,"Actual Loops":1}
      ]}
    ]}
  ]}
}]`)

	metrics, err := parsePostgresPlanJSONMetrics(raw)
	require.NoError(t, err)
	require.Len(t, metrics.PlanNodes, 5)
	require.Equal(t, int64(2), metrics.PlanNodes[1].PlanNodeID)
	require.Equal(t, int64(1), metrics.PlanNodes[1].ParentPlanNodeID)
	require.Equal(t, int64(2), metrics.PlanNodes[2].ParentPlanNodeID)
	require.Equal(t, "Outer", metrics.PlanNodes[2].ParentRelationship)
	require.Equal(t, int64(2), metrics.PlanNodes[3].ParentPlanNodeID)
	require.Equal(t, "Inner", metrics.PlanNodes[3].ParentRelationship)
	require.Equal(t, int64(4), metrics.PlanNodes[4].ParentPlanNodeID)
}

// TestParsePostgresPlanJSONMetricsAttributesLabeledS4State verifies that repeated frontier loops and labeled witness, meeting, and hydration nodes populate their dedicated counters.
func TestParsePostgresPlanJSONMetricsAttributesLabeledS4State(t *testing.T) {
	raw := json.RawMessage(`[{"Plan":{"Node Type":"Result","Actual Rows":1,"Actual Loops":1,"Plans":[
		{"Node Type":"CTE Scan","CTE Name":"forward_frontier","Actual Rows":3,"Actual Loops":2},
		{"Node Type":"CTE Scan","CTE Name":"selected_witness","Actual Rows":4,"Actual Loops":1},
		{"Node Type":"CTE Scan","CTE Name":"shortest_meeting","Actual Rows":1,"Actual Loops":1},
		{"Node Type":"Subquery Scan","Alias":"m0_hydrated","Actual Rows":5,"Actual Loops":1}
	]}}]`)
	metrics, err := parsePostgresPlanJSONMetrics(raw)
	require.NoError(t, err)
	require.Equal(t, int64(6), metrics.FrontierRows)
	require.Equal(t, int64(4), metrics.WitnessRows)
	require.Equal(t, int64(1), metrics.MeetingRows)
	require.Equal(t, int64(5), metrics.HydrationRows)
	require.Equal(t, "plan_derived_labeled_state_rows", metrics.Provenance["witness_rows"])
}

// TestParsePostgresPlanJSONMetricsAttributesEndpointGuardState verifies endpoint/state guard overflow detection and fallback attribution from labeled seeded-search CTEs.
func TestParsePostgresPlanJSONMetricsAttributesEndpointGuardState(t *testing.T) {
	raw := json.RawMessage(`[{"Plan":{"Node Type":"Result","Actual Rows":1,"Actual Loops":1,"Plans":[
		{"Node Type":"CTE Scan","CTE Name":"s4_endpoint_seeded_endpoints","Actual Rows":33,"Actual Loops":1},
		{"Node Type":"CTE Scan","CTE Name":"s4_endpoint_seeded_states","Actual Rows":4097,"Actual Loops":1},
		{"Node Type":"CTE Scan","CTE Name":"s4_endpoint_seeded_incumbent","Actual Rows":10,"Actual Loops":1}
	]}}]`)
	metrics, err := parsePostgresPlanJSONMetrics(raw)
	require.NoError(t, err)
	require.Equal(t, int64(33), metrics.EndpointProbeRows)
	require.Equal(t, int64(4097), metrics.ReverseStateProbeRows)
	require.True(t, metrics.EndpointGuardOverflow)
	require.True(t, metrics.StateGuardOverflow)
	require.True(t, metrics.ExpansionFallbackExecuted)
}
