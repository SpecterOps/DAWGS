// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// parsePostgresPlanJSONMetrics extracts only fields PostgreSQL exposes. Every
// derived counter remains explicitly plan-derived; fixture expectations and
// benchmark-only state diagnostics are recorded elsewhere.
func parsePostgresPlanJSONMetrics(raw json.RawMessage) (PostgresPlanMetrics, error) {
	var documents []map[string]any
	if err := json.Unmarshal(raw, &documents); err != nil {
		return PostgresPlanMetrics{}, fmt.Errorf("decode PostgreSQL JSON plan: %w", err)
	}
	if len(documents) != 1 {
		return PostgresPlanMetrics{}, fmt.Errorf("PostgreSQL JSON plan has %d documents, expected 1", len(documents))
	}

	metrics := PostgresPlanMetrics{Provenance: map[string]string{}}
	metrics.PlanningMS = jsonFloatPointer(documents[0]["Planning Time"])
	metrics.ExecutionMS = jsonFloatPointer(documents[0]["Execution Time"])
	if metrics.PlanningMS != nil {
		metrics.Provenance["planning_ms"] = "measured_plan_json"
	}
	if metrics.ExecutionMS != nil {
		metrics.Provenance["execution_ms"] = "measured_plan_json"
	}
	plan, ok := documents[0]["Plan"].(map[string]any)
	if !ok {
		return PostgresPlanMetrics{}, fmt.Errorf("PostgreSQL JSON plan is missing its root Plan object")
	}
	walkPostgresPlanNode(plan, &metrics)
	if len(metrics.PlanNodes) > 0 {
		metrics.Buffers = metrics.PlanNodes[0].Buffers
		metrics.Provenance["buffers"] = "measured_plan_json_root_inclusive"
	}
	return metrics, nil
}

// walkPostgresPlanNode flattens one EXPLAIN node into aggregate metrics, then recursively visits child plans and CTE subplans.
func walkPostgresPlanNode(node map[string]any, metrics *PostgresPlanMetrics) {
	metric := PostgresPlanNodeMetric{
		NodeType:           jsonString(node["Node Type"]),
		ParentRelationship: jsonString(node["Parent Relationship"]),
		CTEName:            jsonString(node["CTE Name"]),
		RelationName:       jsonString(node["Relation Name"]),
		Alias:              jsonString(node["Alias"]),
		IndexName:          jsonString(node["Index Name"]),
		PlanRows:           jsonInt64(node["Plan Rows"]),
		PlanWidth:          jsonInt64(node["Plan Width"]),
		ActualRows:         jsonInt64(node["Actual Rows"]),
		ActualLoops:        jsonInt64(node["Actual Loops"]),
		ActualTotalMS:      jsonFloat64(node["Actual Total Time"]),
		Buffers:            postgresJSONBuffers(node),
		Provenance:         "measured_plan_json",
	}
	metrics.PlanNodes = append(metrics.PlanNodes, metric)

	rows := metric.ActualRows * metric.ActualLoops
	lowerIdentity := strings.ToLower(strings.Join([]string{metric.NodeType, metric.CTEName, metric.RelationName, metric.Alias, metric.IndexName, jsonString(node["Index Cond"])}, " "))
	if strings.Contains(lowerIdentity, "endpoint_seeded_endpoints") && rows > metrics.EndpointProbeRows {
		metrics.EndpointProbeRows = rows
		metrics.EndpointGuardOverflow = rows >= 33
		metrics.Provenance["endpoint_probe_rows"] = "plan_derived_endpoint_seed_cte_rows"
	}
	if strings.Contains(lowerIdentity, "endpoint_seeded_states") && rows > metrics.ReverseStateProbeRows {
		metrics.ReverseStateProbeRows = rows
		metrics.StateGuardOverflow = rows >= 4097
		metrics.Provenance["reverse_state_probe_rows"] = "plan_derived_reverse_state_probe_cte_rows"
	}
	if strings.Contains(lowerIdentity, "endpoint_seeded_incumbent") && metric.ActualLoops > 0 {
		metrics.ExpansionFallbackExecuted = true
		metrics.Provenance["expansion_fallback_executed"] = "plan_derived_incumbent_cte_scan_loops"
	}
	if strings.Contains(lowerIdentity, "recursive union") {
		metrics.RecursiveRows += rows
		metrics.RecursiveLoops += metric.ActualLoops
		metrics.Provenance["recursive_rows"] = "measured_plan_json"
		metrics.Provenance["recursive_loops"] = "measured_plan_json"
	}
	for identity, target := range map[string]*int64{
		"frontier": &metrics.FrontierRows,
		"witness":  &metrics.WitnessRows,
		"meeting":  &metrics.MeetingRows,
	} {
		if strings.Contains(lowerIdentity, identity) {
			*target += rows
			metrics.Provenance[identity+"_rows"] = "plan_derived_labeled_state_rows"
		}
	}
	if strings.Contains(lowerIdentity, "hydrated") || strings.Contains(lowerIdentity, "materializ") {
		metrics.HydrationRows += rows
		metrics.Provenance["hydration_rows"] = "plan_derived_labeled_state_rows"
	}
	if metric.CTEName == "roots" || (strings.Contains(lowerIdentity, " roots") && strings.Contains(lowerIdentity, "cte scan")) {
		metrics.RootRows += rows
		metrics.Provenance["root_rows"] = "measured_plan_json"
	}
	if strings.Contains(lowerIdentity, "edge") && strings.Contains(lowerIdentity, "start_id") {
		metrics.ForwardEdgeProbes += metric.ActualLoops
		metrics.Provenance["forward_edge_probes"] = "plan_derived_index_loops"
	}
	if strings.Contains(lowerIdentity, "edge") && strings.Contains(lowerIdentity, "end_id") {
		metrics.ReverseEdgeProbes += metric.ActualLoops
		metrics.Provenance["reverse_edge_probes"] = "plan_derived_index_loops"
	}
	if metric.RelationName == "node" || strings.HasPrefix(metric.RelationName, "node_") {
		switch {
		case strings.Contains(strings.ToLower(metric.Alias), "root"):
			metrics.RootLookupLoops += metric.ActualLoops
			metrics.Provenance["root_lookup_loops"] = "plan_derived_alias_loops"
		case strings.Contains(strings.ToLower(metric.Alias), "boundary") || strings.Contains(strings.ToLower(metric.Alias), "next"):
			metrics.BoundaryLookupLoops += metric.ActualLoops
			metrics.Provenance["boundary_lookup_loops"] = "plan_derived_alias_loops"
		default:
			metrics.HydrationLoops += metric.ActualLoops
			metrics.Provenance["hydration_loops"] = "plan_derived_node_relation_loops"
		}
	}
	metrics.WALRecords += jsonInt64(node["WAL Records"])
	metrics.WALBytes += jsonInt64(node["WAL Bytes"])

	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkPostgresPlanNode(childNode, metrics)
		}
	}
}

// postgresJSONBuffers converts optional JSON buffer counters to integer metrics.
func postgresJSONBuffers(node map[string]any) Buffers {
	return Buffers{
		SharedHit:     jsonInt64(node["Shared Hit Blocks"]),
		SharedRead:    jsonInt64(node["Shared Read Blocks"]),
		SharedDirtied: jsonInt64(node["Shared Dirtied Blocks"]),
		SharedWritten: jsonInt64(node["Shared Written Blocks"]),
		LocalHit:      jsonInt64(node["Local Hit Blocks"]),
		LocalRead:     jsonInt64(node["Local Read Blocks"]),
		LocalDirtied:  jsonInt64(node["Local Dirtied Blocks"]),
		LocalWritten:  jsonInt64(node["Local Written Blocks"]),
		TempRead:      jsonInt64(node["Temp Read Blocks"]),
		TempWritten:   jsonInt64(node["Temp Written Blocks"]),
	}
}

// jsonFloatPointer decodes a JSON number as an optional floating-point value.
func jsonFloatPointer(value any) *float64 {
	if value == nil {
		return nil
	}
	parsed := jsonFloat64(value)
	return &parsed
}

// jsonFloat64 decodes a JSON number as a floating-point value, returning zero when absent or invalid.
func jsonFloat64(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case json.Number:
		parsed, _ := typed.Float64()
		return parsed
	default:
		return 0
	}
}

// jsonInt64 decodes a JSON number as an integer, returning zero when absent or invalid.
func jsonInt64(value any) int64 { return int64(jsonFloat64(value)) }

// jsonString decodes a JSON string, returning an empty string for other values.
func jsonString(value any) string {
	valueString, _ := value.(string)
	return valueString
}
