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
	require.Equal(t, "measured_plan_json", metrics.PlanNodes[0].Provenance)
	require.Equal(t, "plan_derived_index_loops", metrics.Provenance["reverse_edge_probes"])
}

func TestParsePostgresPlanJSONMetricsRejectsMissingPlan(t *testing.T) {
	_, err := parsePostgresPlanJSONMetrics(json.RawMessage(`[{"Planning Time":1}]`))
	require.ErrorContains(t, err, "missing its root Plan")
}

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
