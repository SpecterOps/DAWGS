// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

const (
	// postgresTraversalTelemetryOff reserves the stable protocol value used to recognize postgres traversal telemetry off across artifacts and executions.
	postgresTraversalTelemetryOff = "off"

	// postgresTraversalTelemetrySummary reserves the stable protocol value used to recognize postgres traversal telemetry summary across artifacts and executions.
	postgresTraversalTelemetrySummary = "summary"

	// postgresTraversalTelemetryDiagnostic reserves the stable protocol value used to recognize postgres traversal telemetry diagnostic across artifacts and executions.
	postgresTraversalTelemetryDiagnostic = "diagnostic"

	// postgresTraversalPlanReplaySource reserves the stable protocol value used to recognize postgres traversal plan replay source across artifacts and executions.
	postgresTraversalPlanReplaySource = "postgres_explain_analyze_json_timing_off"

	// postgresBidirectionalDiagnosticSource reserves the stable protocol value used to recognize postgres bidirectional diagnostic source across artifacts and executions.
	postgresBidirectionalDiagnosticSource = "public.read_bidirectional_shortest_path_diagnostic_v1"

	// postgresBidirectionalAllShortestDiagnosticSource reserves the stable protocol value used to recognize postgres bidirectional all shortest diagnostic source across artifacts and executions.
	postgresBidirectionalAllShortestDiagnosticSource = "public.read_bidirectional_all_shortest_path_diagnostic_v1"
)

// buildPostgresCaseTraversalTelemetry binds optimizer, emitted SQL, and
// separately replayed plan evidence into one validated traversal identity.
// A nil result means the statement has no unambiguous traversal target.
func buildPostgresCaseTraversalTelemetry(
	optimization translate.OptimizationSummary,
	metrics PostgresPlanMetrics,
	connectionID string,
	level TraversalTelemetryLevel,
) (*TraversalExecutionTelemetry, error) {
	outcome, ok := singleTraversalOutcome(optimization.TargetOutcomes)
	if !ok {
		return nil, nil
	}

	summary, family, err := traversalSummaryFromOutcome(outcome, metrics)
	if err != nil {
		return nil, err
	}
	telemetry := newPostgresTraversalTelemetry(summary, family, metrics, connectionID, level)
	if functionBackedTraversal(metrics) && isBidirectionalTelemetryIdentity(summary) {
		markTraversalSummaryUnavailable(&telemetry, "outer Function Scan does not expose the invocation-local runtime branch")
	}
	if telemetry.Diagnostic != nil && functionBackedTraversal(metrics) && (family == TraversalTelemetryFamilySP || family == TraversalTelemetryFamilyASP) {
		telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
		telemetry.Diagnostic.IncompleteReasons = []string{"outer Function Scan does not expose invocation-local traversal work counters"}
	}
	if err := telemetry.Validate(); err != nil {
		return nil, err
	}
	return &telemetry, nil
}

// buildPostgresReferenceTraversalTelemetry binds an explicit reference
// architecture and implementation to its own untimed JSON EXPLAIN replay.
func buildPostgresReferenceTraversalTelemetry(
	reference PostgresReferenceResult,
	parameters map[string]any,
	connectionID string,
	level TraversalTelemetryLevel,
) (*TraversalExecutionTelemetry, error) {
	if strings.TrimSpace(reference.Architecture) == "" || strings.TrimSpace(reference.ImplementationID) == "" || reference.PostgresMetrics == nil {
		return nil, nil
	}
	if !isTraversalReferenceArchitecture(reference.Architecture) {
		return nil, nil
	}

	family := traversalFamilyForIdentity(reference.Architecture, "")
	fallback := false
	overflow := false
	planned := []string{reference.Architecture}
	fallbackIdentity := bidirectionalFallbackIdentity(reference.Architecture)
	if fallbackIdentity != "" && fallbackIdentity != reference.Architecture {
		planned = append(planned, fallbackIdentity)
	}
	summary := TraversalExecutionSummary{
		RequestedIdentity:       reference.Architecture,
		PlannedIdentities:       planned,
		EmittedIdentity:         reference.ImplementationID,
		RuntimeIdentity:         reference.Architecture,
		AppliedIdentity:         reference.Architecture,
		SelectorVersion:         "explicit-reference-v1",
		SchedulerVersion:        schedulerForIdentity(reference.Architecture, ""),
		ObservationMode:         reference.ObservationShape,
		Caps:                    referenceTraversalCaps(parameters),
		RuntimeOutcomeAvailable: traversalTelemetryPointer(true),
		RuntimeBranch:           "explicit_reference",
		Overflow:                &overflow,
		FallbackExecuted:        &fallback,
		Provenance: map[string]string{
			"requested_identity":        "reference.architecture",
			"planned_identities":        "reference.architecture",
			"emitted_identity":          "reference.implementation_id",
			"runtime_identity":          postgresTraversalPlanReplaySource + ".reference_statement",
			"applied_identity":          "reference.architecture",
			"selector_version":          "reference.explicit_selection",
			"scheduler_version":         "reference.architecture",
			"observation_mode":          "reference.observation_shape",
			"runtime_outcome_available": postgresTraversalPlanReplaySource + ".reference_statement",
			"runtime_branch":            postgresTraversalPlanReplaySource + ".reference_statement",
			"overflow":                  postgresTraversalPlanReplaySource + ".visible_guards",
			"fallback_executed":         postgresTraversalPlanReplaySource + ".visible_branches",
		},
	}
	for name := range summary.Caps {
		summary.Provenance["caps."+name] = "reference.parameters." + traversalCapParameterName(name)
	}

	telemetry := newPostgresTraversalTelemetry(summary, family, *reference.PostgresMetrics, connectionID, level)
	if functionBackedTraversal(*reference.PostgresMetrics) && isBidirectionalTelemetryIdentity(summary) {
		markTraversalSummaryUnavailable(&telemetry, "outer Function Scan does not expose the invocation-local runtime branch")
	}
	if functionBackedTraversal(*reference.PostgresMetrics) && (family == TraversalTelemetryFamilySP || family == TraversalTelemetryFamilyASP) {
		if telemetry.Diagnostic != nil {
			telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
			telemetry.Diagnostic.IncompleteReasons = []string{"outer Function Scan does not expose invocation-local traversal work counters"}
		}
	}
	if err := telemetry.Validate(); err != nil {
		return nil, err
	}
	return &telemetry, nil
}

// isTraversalReferenceArchitecture reports whether is traversal reference architecture.
func isTraversalReferenceArchitecture(identity string) bool {
	return strings.HasPrefix(identity, "SP-") ||
		strings.HasPrefix(identity, "ASP-") ||
		strings.HasPrefix(identity, "EXPANSION-") ||
		strings.HasPrefix(identity, "EXPAND-INTO-") ||
		strings.HasPrefix(identity, "MAT-") ||
		identity == "hydration"
}

// newPostgresTraversalTelemetry records either an optimizer/plan-derived
// summary or partial SQL-visible diagnostic evidence. It never converts
// absent executor counters into fabricated zero values.
func newPostgresTraversalTelemetry(
	summary TraversalExecutionSummary,
	family TraversalTelemetryFamily,
	metrics PostgresPlanMetrics,
	connectionID string,
	level TraversalTelemetryLevel,
) TraversalExecutionTelemetry {
	telemetry := TraversalExecutionTelemetry{
		SchemaVersion: TraversalExecutionTelemetrySchemaVersion,
		Level:         level,
		Summary:       summary,
	}
	if level == TraversalTelemetryLevelDiagnostic {
		telemetry.Diagnostic = &TraversalExecutionDiagnostic{
			InvocationID:     newRunUUID(),
			ConnectionID:     connectionID,
			TimedSample:      traversalTelemetryPointer(false),
			RequiredFamilies: traversalRequiredFamilies(summary, family),
			CounterStatus:    TraversalTelemetryCounterStatusPlanPartial,
			IncompleteReasons: []string{
				"JSON EXPLAIN exposes SQL plan work but not every qualification counter in the declared family",
			},
			PlanReplay: postgresTraversalPlanReplay(metrics),
			Provenance: map[string]string{},
		}
	}
	return telemetry
}

// singleTraversalOutcome supports benchmark evidence processing for single traversal outcome.
func singleTraversalOutcome(outcomes []translate.TargetLoweringOutcome) (translate.TargetLoweringOutcome, bool) {
	var shortest, expansion []translate.TargetLoweringOutcome
	for _, outcome := range outcomes {
		if outcome.TargetKind != "" && outcome.TargetKind != "traversal" {
			continue
		}
		if outcome.Family == "SP" || outcome.Family == "ASP" {
			shortest = append(shortest, outcome)
		} else if strings.Contains(outcome.Family, "expansion") {
			expansion = append(expansion, outcome)
		}
	}
	// Shortest-path execution is the public traversal boundary even when its
	// underlying variable step also produced ordinary-expansion analysis.
	// Analysis-only endpoint/predicate outcomes must never make telemetry
	// ambiguous or replace the executor identity.
	if len(shortest) == 1 {
		return shortest[0], true
	}
	if len(shortest) != 0 || len(expansion) != 1 {
		return translate.TargetLoweringOutcome{}, false
	}
	return expansion[0], true
}

// traversalSummaryFromOutcome supports benchmark evidence processing for traversal summary from outcome.
func traversalSummaryFromOutcome(outcome translate.TargetLoweringOutcome, metrics PostgresPlanMetrics) (TraversalExecutionSummary, TraversalTelemetryFamily, error) {
	requested := outcome.Candidate
	if requested == "" {
		requested = outcome.Selected
	}
	applied := outcome.Applied
	if applied == "" {
		applied = outcome.Fallback
	}
	if requested == "" || applied == "" {
		return TraversalExecutionSummary{}, "", fmt.Errorf("traversal target outcome has no requested or applied identity")
	}

	planned := append([]string(nil), outcome.PlannedCandidates...)
	for _, identity := range []string{requested, applied, outcome.Fallback} {
		if identity != "" && !slices.Contains(planned, identity) {
			planned = append(planned, identity)
		}
	}
	emitted := outcome.EmittedPolicy
	if emitted == "" {
		if len(outcome.EmittedCandidates) > 1 {
			emitted = strings.Join(outcome.EmittedCandidates, "+")
		} else if len(outcome.EmittedCandidates) == 1 {
			emitted = outcome.EmittedCandidates[0]
		} else {
			emitted = applied
		}
	}

	runtimeIdentity, runtimeBranch, fallbackExecuted, overflow := runtimeTraversalIdentity(outcome, metrics, requested, applied)
	wouldSelectIdentity := ""
	if outcome.SelectionMode == "shadow_tool" {
		runtimeIdentity = applied
		runtimeBranch = "shadow_incumbent"
		fallbackExecuted = false
		overflow = metrics.EndpointGuardOverflow || metrics.StateGuardOverflow ||
			orientationPlanOverflow(outcome, postgresTraversalPlanReplay(metrics))
		wouldSelectIdentity = shadowWouldSelectIdentity(outcome, metrics)
	}
	if outcome.EmittedPolicy != "" {
		// Applied is a runtime fact for a same-statement policy; the translator
		// can report emitted arms but cannot know which branch executed.
		applied = runtimeIdentity
	}
	if runtimeIdentity != "" && !slices.Contains(planned, runtimeIdentity) {
		planned = append(planned, runtimeIdentity)
	}
	if fallbackExecuted && outcome.Fallback != "" {
		applied = outcome.Fallback
		runtimeIdentity = outcome.Fallback
	}
	selectorVersion := outcome.SelectorVersion
	if selectorVersion == "" {
		selectorVersion = "static-lowering-v1"
	}
	summary := TraversalExecutionSummary{
		RequestedIdentity:       requested,
		PlannedIdentities:       planned,
		EmittedIdentity:         emitted,
		RuntimeIdentity:         runtimeIdentity,
		AppliedIdentity:         applied,
		SelectorVersion:         selectorVersion,
		SchedulerVersion:        schedulerForIdentity(runtimeIdentity, outcome.Scheduler),
		ExecutionBoundary:       outcome.ExecutionBoundary,
		ObservationMode:         outcome.ObservationMode,
		Caps:                    outcomeTraversalCaps(outcome),
		RuntimeOutcomeAvailable: traversalTelemetryPointer(true),
		RuntimeBranch:           runtimeBranch,
		Overflow:                &overflow,
		FallbackExecuted:        &fallbackExecuted,
		WouldSelectIdentity:     wouldSelectIdentity,
		Provenance: map[string]string{
			"requested_identity":        "optimizer.target_outcome.candidate_or_selected",
			"planned_identities":        "optimizer.target_outcome.planned_candidates",
			"emitted_identity":          "translator.target_outcome.emitted_policy_or_candidates",
			"runtime_identity":          postgresTraversalPlanReplaySource + ".visible_branch_and_translator_applied",
			"applied_identity":          "translator.target_outcome.applied_or_fallback",
			"execution_boundary":        "optimizer.target_outcome.execution_boundary",
			"selector_version":          "optimizer.target_outcome.selector_version",
			"scheduler_version":         "optimizer.target_outcome.scheduler",
			"observation_mode":          "optimizer.target_outcome.observation_mode",
			"runtime_outcome_available": postgresTraversalPlanReplaySource + ".visible_branch",
			"runtime_branch":            postgresTraversalPlanReplaySource + ".visible_branch",
			"overflow":                  postgresTraversalPlanReplaySource + ".visible_guard",
			"fallback_executed":         postgresTraversalPlanReplaySource + ".visible_branch",
		},
	}
	if wouldSelectIdentity != "" {
		summary.Provenance["would_select_identity"] = postgresTraversalPlanReplaySource + ".orientation_shadow_marker_rows"
	}
	for name := range summary.Caps {
		summary.Provenance["caps."+name] = "optimizer.target_outcome." + traversalCapOutcomeField(name)
	}
	if fallbackExecuted {
		summary.FallbackIdentity = applied
		summary.Provenance["fallback_identity"] = "optimizer.target_outcome.fallback"
	}
	family := traversalFamilyForIdentity(runtimeIdentity, outcome.Family)
	if isOrientationProbePolicy(outcome.EmittedPolicy) ||
		outcome.EmittedPolicy == string(optimize.ExpansionSearchPolicyEndpointGuardV1) {
		family = TraversalTelemetryFamilyOrientation
	}
	if runtimeIdentity == "" {
		telemetry := TraversalExecutionTelemetry{Summary: summary}
		markTraversalSummaryUnavailable(&telemetry, "exact executed traversal marker is unavailable")
		summary = telemetry.Summary
	}
	return summary, family, nil
}

// traversalCapParameterName supports benchmark evidence processing for traversal cap parameter name.
func traversalCapParameterName(counterName string) string {
	switch counterName {
	case "state_rows":
		return "state_limit"
	case "frontier_rows", "queue_rows":
		return "frontier_limit"
	case "predecessor_rows":
		return "predecessor_limit"
	case "output_rows":
		return "enumeration_limit"
	case "output_bytes":
		return "output_bytes_limit"
	default:
		return counterName
	}
}

// traversalCapOutcomeField supports benchmark evidence processing for traversal cap outcome field.
func traversalCapOutcomeField(counterName string) string {
	switch counterName {
	case "state_rows":
		return "state_limit"
	case "frontier_rows", "queue_rows":
		return "frontier_limit"
	case "predecessor_rows":
		return "predecessor_limit"
	case "endpoint_probe_rows":
		return "endpoint_limit"
	case "output_rows":
		return "enumeration_limit"
	case "output_bytes":
		return "output_bytes_limit"
	default:
		return counterName
	}
}

// shadowWouldSelectIdentity derives the stable identity used to compare shadow would select.
func shadowWouldSelectIdentity(outcome translate.TargetLoweringOutcome, metrics PostgresPlanMetrics) string {
	plan := postgresTraversalPlanReplay(metrics)
	if plan.Counters["orientation_shadow_reverse_rows"] > 0 {
		return outcome.Candidate
	}
	if plan.Counters["orientation_shadow_forward_rows"] > 0 {
		if outcome.Fallback != "" {
			return outcome.Fallback
		}
		return outcome.Applied
	}
	return ""
}

// runtimeTraversalIdentity derives the stable identity used to compare runtime traversal.
func runtimeTraversalIdentity(outcome translate.TargetLoweringOutcome, metrics PostgresPlanMetrics, requested, applied string) (identity, branch string, fallback, overflow bool) {
	identity, branch = applied, "selected"
	if outcome.EmittedPolicy == "" && outcome.Fallback != "" && requested != applied && applied == outcome.Fallback {
		return applied, "compile_time_fallback", true, false
	}
	overflow = metrics.EndpointGuardOverflow || metrics.StateGuardOverflow
	if metrics.ExpansionFallbackExecuted {
		identity = outcome.Fallback
		if identity == "" {
			identity = applied
		}
		return identity, "runtime_fallback", true, overflow
	}

	plan := postgresTraversalPlanReplay(metrics)
	if outcome.EmittedPolicy == optimize.ShortestPathPolicyASPI1GuardedV1 {
		candidateRows, candidatePresent := plan.Counters["asp_i1_candidate_marker_rows"]
		fallbackRows, fallbackPresent := plan.Counters["asp_i1_fallback_marker_rows"]
		overflow = aspI1PlanOverflow(outcome, plan)
		if !candidatePresent || !fallbackPresent {
			return "", "runtime_outcome_unavailable", false, overflow
		}
		if candidateRows == 1 && fallbackRows == 0 {
			return string(optimize.ShortestPathExecutorASPI1DAG), "inline_predecessor_dag", false, false
		}
		if fallbackRows == 1 && candidateRows == 0 {
			return string(optimize.ShortestPathExecutorASPA1DAG), "exact_a1_fallback", true, true
		}
		return "", "runtime_outcome_unavailable", false, overflow
	}
	if outcome.EmittedPolicy == optimize.ShortestPathPolicyI1CanonicalGuardedV1 {
		candidateRows, candidatePresent := plan.Counters["asp_i1_candidate_marker_rows"]
		fallbackRows, fallbackPresent := plan.Counters["asp_i1_fallback_marker_rows"]
		overflow = aspI1PlanOverflow(outcome, plan)
		if !candidatePresent || !fallbackPresent {
			return "", "runtime_outcome_unavailable", false, overflow
		}
		if candidateRows == 1 && fallbackRows == 0 {
			outputRows, outputPresent := plan.Counters["asp_i1_output_rows"]
			if !outputPresent {
				return "", "runtime_outcome_unavailable", false, false
			}
			branch := "inline_canonical_witness"
			if outputRows == 0 {
				branch = "inline_canonical_no_path"
			}
			return string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness), branch, false, false
		}
		if fallbackRows == 1 && candidateRows == 0 {
			return string(optimize.ShortestPathExecutorS4CanonicalWitness), "exact_s4_fallback", true, true
		}
		return "", "runtime_outcome_unavailable", false, overflow
	}
	if outcome.EmittedPolicy != "" {
		candidateRows := plan.Counters["orientation_executed_candidate_rows"]
		incumbentRows := plan.Counters["orientation_executed_incumbent_rows"]
		overflow = overflow || orientationPlanOverflow(outcome, plan)
		if candidateRows == 1 && incumbentRows == 0 && outcome.Candidate != "" {
			if overflow {
				return "", "runtime_outcome_unavailable", false, true
			}
			return outcome.Candidate, "suffix_seeded_reverse", false, false
		}
		if incumbentRows == 1 && candidateRows == 0 && outcome.Fallback != "" {
			return outcome.Fallback, "exact_forward_incumbent", overflow, overflow
		}
		return "", "runtime_outcome_unavailable", false, overflow
	}
	return identity, branch, false, overflow
}

// aspI1PlanOverflow supports benchmark evidence processing for asp i1 plan overflow.
func aspI1PlanOverflow(outcome translate.TargetLoweringOutcome, plan *TraversalPlanReplayEvidence) bool {
	for counter, limit := range map[string]int64{
		"asp_i1_distance_rows":    outcome.StateLimit,
		"asp_i1_predecessor_rows": outcome.PredecessorLimit,
		"asp_i1_enumeration_rows": outcome.EnumerationLimit,
	} {
		if limit > 0 && plan.Counters[counter] > limit {
			return true
		}
	}
	return false
}

// orientationPlanOverflow supports benchmark evidence processing for orientation plan overflow.
func orientationPlanOverflow(outcome translate.TargetLoweringOutcome, plan *TraversalPlanReplayEvidence) bool {
	if outcome.StateLimit > 0 && plan.Counters["orientation_state_rows"] > outcome.StateLimit {
		return true
	}
	if outcome.ProbeCaps == nil {
		return false
	}
	for counter, limit := range map[string]int64{
		"orientation_root_probe_rows":     outcome.ProbeCaps.RootRowLimit,
		"orientation_suffix_probe_rows":   outcome.ProbeCaps.ReverseSeedRowLimit,
		"orientation_forward_degree_rows": outcome.ProbeCaps.DirectionalDegreeRowLimit,
		"orientation_reverse_degree_rows": outcome.ProbeCaps.DirectionalDegreeRowLimit,
	} {
		if limit > 0 && plan.Counters[counter] > limit {
			return true
		}
	}
	return false
}

// outcomeTraversalCaps returns the resource limits enforced for outcome traversal.
func outcomeTraversalCaps(outcome translate.TargetLoweringOutcome) map[string]int64 {
	caps := map[string]int64{}
	if outcome.StateLimit > 0 {
		caps["state_rows"] = outcome.StateLimit
	}
	if outcome.FrontierLimit > 0 {
		caps["frontier_rows"] = outcome.FrontierLimit
		caps["queue_rows"] = outcome.FrontierLimit
	}
	if outcome.PredecessorLimit > 0 {
		caps["predecessor_rows"] = outcome.PredecessorLimit
	}
	if outcome.EnumerationLimit > 0 {
		caps["output_rows"] = outcome.EnumerationLimit
	}
	if outcome.OutputBytesLimit > 0 {
		caps["output_bytes"] = outcome.OutputBytesLimit
	}
	if outcome.EndpointLimit > 0 {
		caps["endpoint_probe_rows"] = outcome.EndpointLimit
	}
	if outcome.ProbeCaps != nil {
		if outcome.ProbeCaps.RootRowLimit > 0 {
			caps["forward_seed_rows"] = outcome.ProbeCaps.RootRowLimit
		}
		if outcome.ProbeCaps.ReverseSeedRowLimit > 0 {
			caps["reverse_seed_rows"] = outcome.ProbeCaps.ReverseSeedRowLimit
		}
		if outcome.ProbeCaps.DirectionalDegreeRowLimit > 0 {
			caps["directional_degree_rows"] = outcome.ProbeCaps.DirectionalDegreeRowLimit
		}
		if outcome.ProbeCaps.SurvivalRowLimit > 0 {
			caps["survival_rows"] = outcome.ProbeCaps.SurvivalRowLimit
		}
	}
	return caps
}

// referenceTraversalCaps returns the resource limits enforced for reference traversal.
func referenceTraversalCaps(parameters map[string]any) map[string]int64 {
	caps := map[string]int64{}
	for _, name := range []string{"state_limit", "frontier_limit", "predecessor_limit", "enumeration_limit", "output_bytes_limit", "output_limit"} {
		if value, ok := integerParameter(parameters[name]); ok && value > 0 {
			counterName := strings.TrimSuffix(name, "_limit") + "_rows"
			switch name {
			case "enumeration_limit":
				counterName = "output_rows"
			case "output_bytes_limit":
				counterName = "output_bytes"
			}
			caps[counterName] = value
			if name == "frontier_limit" {
				caps["queue_rows"] = value
			}
		}
	}
	return caps
}

// integerParameter supports benchmark evidence processing for integer parameter.
func integerParameter(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

// traversalFamilyForIdentity derives the stable identity used to compare traversal family for.
func traversalFamilyForIdentity(identity, family string) TraversalTelemetryFamily {
	if strings.HasPrefix(identity, "ASP-") || family == "ASP" {
		return TraversalTelemetryFamilyASP
	}
	if strings.HasPrefix(identity, "SP-") || family == "SP" {
		return TraversalTelemetryFamilySP
	}
	if isOrientationProbePolicy(identity) || strings.Contains(identity, "ORIENTATION") {
		return TraversalTelemetryFamilyOrientation
	}
	if strings.HasPrefix(identity, "MAT-") {
		return TraversalTelemetryFamilyHydration
	}
	return TraversalTelemetryFamilyOrdinary
}

// traversalRequiredFamilies derives the complete observation contract from
// the emitted policy and public result shape. Families are deliberately kept
// separate so search counters cannot stand in for hydration or workspace
// evidence.
func traversalRequiredFamilies(summary TraversalExecutionSummary, base TraversalTelemetryFamily) []TraversalTelemetryFamily {
	var required []TraversalTelemetryFamily
	add := func(family TraversalTelemetryFamily) {
		if family != "" && !slices.Contains(required, family) {
			required = append(required, family)
		}
	}

	identity := summary.RuntimeIdentity
	if identity == "" {
		identity = summary.RequestedIdentity
	}
	if isOrientationProbePolicy(summary.EmittedIdentity) || isOrientationProbePolicy(summary.SelectorVersion) {
		add(TraversalTelemetryFamilyOrientation)
		add(TraversalTelemetryFamilyOrdinary)
		if observationRequiresHydration(summary.ObservationMode) {
			add(TraversalTelemetryFamilyHydration)
		}
	} else {
		add(base)
	}
	if strings.HasPrefix(identity, "ASP-") || base == TraversalTelemetryFamilyASP {
		add(TraversalTelemetryFamilyHydration)
	}
	if strings.Contains(identity, "WE+MAT") || strings.Contains(summary.RequestedIdentity, "WE+MAT") ||
		strings.HasPrefix(identity, "MAT-") ||
		(observationRequiresHydration(summary.ObservationMode) &&
			(strings.HasPrefix(identity, "SP-") || strings.HasPrefix(summary.RequestedIdentity, "SP-"))) {
		add(TraversalTelemetryFamilyHydration)
	}
	if isBidirectionalSPIdentity(identity) || isBidirectionalASPIdentity(identity) ||
		isBidirectionalSPIdentity(summary.RequestedIdentity) || isBidirectionalASPIdentity(summary.RequestedIdentity) {
		add(TraversalTelemetryFamilyWorkspace)
	}
	return required
}

// observationRequiresHydration supports benchmark evidence processing for observation requires hydration.
func observationRequiresHydration(observation string) bool {
	normalized := strings.ToLower(strings.TrimSpace(observation))
	return normalized == "one_path" || normalized == "all_paths" || normalized == "full_path" ||
		strings.Contains(normalized, "complete path") || strings.Contains(normalized, "all-shortest path")
}

// isBidirectionalTelemetryIdentity reports whether is bidirectional telemetry identity.
func isBidirectionalTelemetryIdentity(summary TraversalExecutionSummary) bool {
	return isBidirectionalSPIdentity(summary.RuntimeIdentity) || isBidirectionalASPIdentity(summary.RuntimeIdentity) ||
		isBidirectionalSPIdentity(summary.RequestedIdentity) || isBidirectionalASPIdentity(summary.RequestedIdentity)
}

// bidirectionalTelemetryIdentity derives the stable identity used to compare bidirectional telemetry.
func bidirectionalTelemetryIdentity(summary TraversalExecutionSummary) string {
	for _, identity := range []string{summary.RuntimeIdentity, summary.RequestedIdentity} {
		if isBidirectionalSPIdentity(identity) || isBidirectionalASPIdentity(identity) {
			return identity
		}
	}
	return ""
}

// schedulerForIdentity derives the stable identity used to compare scheduler for.
func schedulerForIdentity(identity, scheduler string) string {
	if scheduler != "" {
		return scheduler
	}
	switch {
	case strings.Contains(identity, "ALT-NODE"):
		return "strict_alternating_node"
	case strings.Contains(identity, "MIN-LEVEL"):
		return "smaller_current_level"
	case strings.HasPrefix(identity, "SP-"), strings.HasPrefix(identity, "ASP-"):
		return "single_ended_level"
	default:
		return "not_applicable"
	}
}

// functionBackedTraversal supports benchmark evidence processing for function backed traversal.
func functionBackedTraversal(metrics PostgresPlanMetrics) bool {
	for _, node := range metrics.PlanNodes {
		if node.NodeType == "Function Scan" && strings.TrimSpace(node.FunctionName) != "" {
			return true
		}
	}
	return false
}

// postgresTraversalPlanReplay supports benchmark evidence processing for postgres traversal plan replay.
func postgresTraversalPlanReplay(metrics PostgresPlanMetrics) *TraversalPlanReplayEvidence {
	replay := &TraversalPlanReplayEvidence{
		Source:     postgresTraversalPlanReplaySource,
		Counters:   map[string]int64{"plan_nodes": int64(len(metrics.PlanNodes))},
		Flags:      map[string]bool{},
		Provenance: map[string]string{"counters.plan_nodes": "postgres_metrics.plan_nodes"},
	}
	addCounter := func(name string, value int64, metricName string) {
		if provenance := metrics.Provenance[metricName]; provenance != "" {
			replay.Counters[name] = value
			replay.Provenance["counters."+name] = "postgres_metrics." + metricName + ":" + provenance
		}
	}
	addCounter("root_rows", metrics.RootRows, "root_rows")
	addCounter("recursive_rows", metrics.RecursiveRows, "recursive_rows")
	addCounter("recursive_loops", metrics.RecursiveLoops, "recursive_loops")
	addCounter("frontier_rows", metrics.FrontierRows, "frontier_rows")
	addCounter("witness_rows", metrics.WitnessRows, "witness_rows")
	addCounter("meeting_rows", metrics.MeetingRows, "meeting_rows")
	addCounter("hydration_rows", metrics.HydrationRows, "hydration_rows")
	addCounter("forward_edge_probe_loops", metrics.ForwardEdgeProbes, "forward_edge_probes")
	addCounter("reverse_edge_probe_loops", metrics.ReverseEdgeProbes, "reverse_edge_probes")
	addCounter("endpoint_probe_rows", metrics.EndpointProbeRows, "endpoint_probe_rows")
	addCounter("reverse_state_probe_rows", metrics.ReverseStateProbeRows, "reverse_state_probe_rows")
	addFlag := func(name string, value bool, metricName string) {
		if provenance := metrics.Provenance[metricName]; provenance != "" {
			replay.Flags[name] = value
			replay.Provenance["flags."+name] = "postgres_metrics." + metricName + ":" + provenance
		}
	}
	addFlag("endpoint_guard_overflow", metrics.EndpointGuardOverflow, "endpoint_probe_rows")
	addFlag("state_guard_overflow", metrics.StateGuardOverflow, "reverse_state_probe_rows")
	addFlag("fallback_executed", metrics.ExpansionFallbackExecuted, "expansion_fallback_executed")

	inlineCTECounters := map[string]string{
		"asp_i1_distance_bounded":    "asp_i1_distance_rows",
		"asp_i1_predecessor_bounded": "asp_i1_predecessor_rows",
		"asp_i1_paths_bounded":       "asp_i1_enumeration_rows",
		"asp_i1_shortest":            "asp_i1_output_rows",
		"asp_i1_candidate_marker":    "asp_i1_candidate_marker_rows",
		"asp_i1_fallback_marker":     "asp_i1_fallback_marker_rows",
		"asp_i1_candidate_rows":      "asp_i1_candidate_branch_rows",
		"asp_i1_fallback_rows":       "asp_i1_fallback_branch_rows",
	}
	inlineCTEBodies := map[string][]PostgresPlanNodeMetric{}
	for _, node := range metrics.PlanNodes {
		rows := node.ActualRows * node.ActualLoops
		for cteName := range inlineCTECounters {
			if inlinePredecessorCTEBody(node, cteName) {
				inlineCTEBodies[cteName] = append(inlineCTEBodies[cteName], node)
			}
		}
		for suffix, name := range map[string]string{
			"orientation_root_probe":           "orientation_root_probe_rows",
			"orientation_suffix_probe":         "orientation_suffix_probe_rows",
			"orientation_boundaries":           "orientation_boundary_rows",
			"orientation_forward_degree_probe": "orientation_forward_degree_rows",
			"orientation_reverse_degree_probe": "orientation_reverse_degree_rows",
			"orientation_states":               "orientation_state_rows",
		} {
			if orientationCTEBody(node, suffix) {
				replay.Counters[name] = rows
				replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.measured_plan_json"
			}
		}
		for suffix, name := range map[string]string{
			"orientation_shadow_forward":   "orientation_shadow_forward_rows",
			"orientation_shadow_reverse":   "orientation_shadow_reverse_rows",
			"orientation_shadow_selection": "orientation_shadow_selection_rows",
		} {
			if orientationCTEBody(node, suffix) {
				replay.Counters[name] = rows
				replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.measured_plan_json"
			}
		}
		for suffix, name := range map[string]string{
			"orientation_executed_candidate": "orientation_executed_candidate_rows",
			"orientation_executed_incumbent": "orientation_executed_incumbent_rows",
		} {
			if orientationCTEBody(node, suffix) {
				replay.Counters[name] = rows
				replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.measured_plan_json"
			}
		}
		for suffix, name := range map[string]string{
			"orientation_root_probe":           "orientation_root_probe_loops",
			"orientation_suffix_probe":         "orientation_suffix_probe_loops",
			"orientation_boundaries":           "orientation_boundary_probe_loops",
			"orientation_forward_degree_probe": "orientation_forward_degree_probe_loops",
			"orientation_reverse_degree_probe": "orientation_reverse_degree_probe_loops",
			"orientation_decision":             "orientation_decision_loops",
		} {
			if orientationCTEBody(node, suffix) {
				replay.Counters[name] = node.ActualLoops
				replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.measured_plan_json"
			}
		}
		for suffix, name := range map[string]string{
			"orientation_reverse":   "orientation_candidate_branch_loops",
			"orientation_incumbent": "orientation_incumbent_branch_loops",
		} {
			if orientationCTEBody(node, suffix) {
				replay.Counters[name] = node.ActualLoops
				replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.measured_plan_json"
			}
		}
		if node.NodeType == "Function Scan" && node.FunctionName != "" {
			replay.Counters["function_scan_loops"] += node.ActualLoops
			replay.Provenance["counters.function_scan_loops"] = "postgres_metrics.plan_nodes.function_scan_actual_loops"
		}
	}
	for cteName, counterName := range inlineCTECounters {
		bodies := inlineCTEBodies[cteName]
		if len(bodies) != 1 {
			continue
		}
		body := bodies[0]
		replay.Counters[counterName] = body.ActualRows * body.ActualLoops
		replay.Provenance["counters."+counterName] = "postgres_metrics.plan_nodes.exact_cte_materialization_body"

		branch := ""
		markerCTE := ""
		switch cteName {
		case "asp_i1_candidate_rows":
			branch, markerCTE = "candidate", "asp_i1_candidate_marker"
		case "asp_i1_fallback_rows":
			branch, markerCTE = "fallback", "asp_i1_fallback_marker"
		default:
			continue
		}
		if body.PlanNodeID <= 0 {
			continue
		}
		var directChildren, directOuterMarkers, directInnerExecutors []PostgresPlanNodeMetric
		for _, node := range metrics.PlanNodes {
			if node.ParentPlanNodeID != body.PlanNodeID {
				continue
			}
			directChildren = append(directChildren, node)
			switch {
			case strings.EqualFold(strings.TrimSpace(node.ParentRelationship), "Outer") &&
				strings.EqualFold(strings.TrimSpace(node.NodeType), "CTE Scan") &&
				strings.EqualFold(strings.TrimSpace(node.CTEName), markerCTE):
				directOuterMarkers = append(directOuterMarkers, node)
			case strings.EqualFold(strings.TrimSpace(node.ParentRelationship), "Inner"):
				directInnerExecutors = append(directInnerExecutors, node)
			}
		}
		markerBodies := inlineCTEBodies[markerCTE]
		if len(directChildren) != 2 || len(directOuterMarkers) != 1 || len(directInnerExecutors) != 1 || len(markerBodies) != 1 {
			continue
		}
		markerRows := markerBodies[0].ActualRows * markerBodies[0].ActualLoops
		outerMarkerRows := directOuterMarkers[0].ActualRows * directOuterMarkers[0].ActualLoops
		if directOuterMarkers[0].ActualLoops != 1 || outerMarkerRows != markerRows {
			continue
		}
		name := "asp_i1_" + branch + "_executor_loops"
		replay.Counters[name] = directInnerExecutors[0].ActualLoops
		replay.Provenance["counters."+name] = "postgres_metrics.plan_nodes.marker_gated_direct_inner_child_actual_loops"
	}
	return replay
}

// orientationCTEBody matches the single materialization node PostgreSQL
// labels "CTE <name>". Consumer CTE scans may execute many times and aliases
// such as reverse_degree_probe contain shorter branch names, so substring
// attribution would over-count probes and invent work in inactive arms.
func orientationCTEBody(node PostgresPlanNodeMetric, suffix string) bool {
	return namedCTEBody(node, suffix)
}

// namedCTEBody matches a PostgreSQL CTE's single materialization body. CTEName
// and Alias identify consumer scans and are intentionally excluded.
func namedCTEBody(node PostgresPlanNodeMetric, suffix string) bool {
	name := strings.ToLower(strings.TrimSpace(node.SubplanName))
	return strings.HasPrefix(name, "cte ") && strings.HasSuffix(name, suffix)
}

// inlinePredecessorCTEBody uses an exact fixed name because its qualification
// contract is tied to one emitted statement shape, not stage-prefixed CTEs.
func inlinePredecessorCTEBody(node PostgresPlanNodeMetric, name string) bool {
	return strings.EqualFold(strings.TrimSpace(node.SubplanName), "CTE "+name)
}

// postgresBidirectionalDiagnosticDocument is the invocation-local document
// returned by read_bidirectional_shortest_path_diagnostic_v1. Pointer fields
// preserve the distinction between a measured zero and missing evidence.
type postgresBidirectionalDiagnosticDocument struct {
	// SchemaVersion identifies the schema version for schema version.
	SchemaVersion int `json:"schema_version"`
	// InvocationID identifies the invocation id.
	InvocationID string `json:"invocation_id"`
	// Scheduler supplies the scheduler input to the postgresBidirectionalDiagnosticDocument contract.
	Scheduler string `json:"scheduler"`
	// StateLimit supplies the state limit input to the postgresBidirectionalDiagnosticDocument contract.
	StateLimit *int64 `json:"state_limit"`
	// FrontierLimit supplies the frontier limit input to the postgresBidirectionalDiagnosticDocument contract.
	FrontierLimit *int64 `json:"frontier_limit"`
	// PredecessorLimit supplies the predecessor limit input to the postgresBidirectionalDiagnosticDocument contract.
	PredecessorLimit *int64 `json:"predecessor_limit"`
	// SearchCalls supplies the search calls input to the postgresBidirectionalDiagnosticDocument contract.
	SearchCalls *int64 `json:"search_calls"`
	// RuntimeBranch supplies the runtime branch input to the postgresBidirectionalDiagnosticDocument contract.
	RuntimeBranch string `json:"runtime_branch"`
	// Overflowed supplies the overflowed input to the postgresBidirectionalDiagnosticDocument contract.
	Overflowed *bool `json:"overflowed"`
	// FallbackExecuted supplies the fallback executed input to the postgresBidirectionalDiagnosticDocument contract.
	FallbackExecuted *bool `json:"fallback_executed"`
	// Counters supplies the counters input to the postgresBidirectionalDiagnosticDocument contract.
	Counters *postgresBidirectionalDiagnosticCounts `json:"counters"`
	// Calls supplies the calls input to the postgresBidirectionalDiagnosticDocument contract.
	Calls []postgresBidirectionalDiagnosticCall `json:"calls"`
	// WorkspaceBytes supplies the workspace bytes input to the postgresBidirectionalDiagnosticDocument contract.
	WorkspaceBytes int64 `json:"-"`
}

// postgresBidirectionalDiagnosticCall groups state that must remain consistent while processing postgres bidirectional diagnostic call.
type postgresBidirectionalDiagnosticCall struct {
	// SearchID identifies the search id.
	SearchID *int64 `json:"search_id"`
	// SourceID identifies the source id.
	SourceID *int64 `json:"source_id"`
	// TargetID identifies the target id.
	TargetID *int64 `json:"target_id"`
	// RuntimeBranch supplies the runtime branch input to the postgresBidirectionalDiagnosticCall contract.
	RuntimeBranch string `json:"runtime_branch"`
	// SchedulerActions supplies the scheduler actions input to the postgresBidirectionalDiagnosticCall contract.
	SchedulerActions *int64 `json:"scheduler_actions"`
	// CandidateEdges supplies the candidate edges input to the postgresBidirectionalDiagnosticCall contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the postgresBidirectionalDiagnosticCall contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenPeak supplies the seen peak input to the postgresBidirectionalDiagnosticCall contract.
	SeenPeak *int64 `json:"seen_peak"`
	// FrontierPeak supplies the frontier peak input to the postgresBidirectionalDiagnosticCall contract.
	FrontierPeak *int64 `json:"frontier_peak"`
	// QueuePeak supplies the queue peak input to the postgresBidirectionalDiagnosticCall contract.
	QueuePeak *int64 `json:"queue_peak"`
	// PredecessorPeak supplies the predecessor peak input to the postgresBidirectionalDiagnosticCall contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingCandidates supplies the meeting candidates input to the postgresBidirectionalDiagnosticCall contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// FrozenDistance supplies the frozen distance input to the postgresBidirectionalDiagnosticCall contract.
	FrozenDistance *int64 `json:"frozen_distance"`
	// WitnessRows records the number of witness rows.
	WitnessRows *int64 `json:"witness_rows"`
	// Overflowed supplies the overflowed input to the postgresBidirectionalDiagnosticCall contract.
	Overflowed *bool `json:"overflowed"`
	// FallbackExecuted supplies the fallback executed input to the postgresBidirectionalDiagnosticCall contract.
	FallbackExecuted *bool `json:"fallback_executed"`
}

// postgresBidirectionalDiagnosticCounts aggregates counters observed while evaluating postgres bidirectional diagnostic.
type postgresBidirectionalDiagnosticCounts struct {
	// SchedulerActions supplies the scheduler actions input to the postgresBidirectionalDiagnosticCounts contract.
	SchedulerActions *int64 `json:"scheduler_actions"`
	// CandidateEdges supplies the candidate edges input to the postgresBidirectionalDiagnosticCounts contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the postgresBidirectionalDiagnosticCounts contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenPeak supplies the seen peak input to the postgresBidirectionalDiagnosticCounts contract.
	SeenPeak *int64 `json:"seen_peak"`
	// FrontierPeak supplies the frontier peak input to the postgresBidirectionalDiagnosticCounts contract.
	FrontierPeak *int64 `json:"frontier_peak"`
	// QueuePeak supplies the queue peak input to the postgresBidirectionalDiagnosticCounts contract.
	QueuePeak *int64 `json:"queue_peak"`
	// PredecessorPeak supplies the predecessor peak input to the postgresBidirectionalDiagnosticCounts contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingCandidates supplies the meeting candidates input to the postgresBidirectionalDiagnosticCounts contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// FrozenDistance supplies the frozen distance input to the postgresBidirectionalDiagnosticCounts contract.
	FrozenDistance *int64 `json:"frozen_distance"`
	// WitnessRows records the number of witness rows.
	WitnessRows *int64 `json:"witness_rows"`
	// Levels supplies the levels input to the postgresBidirectionalDiagnosticCounts contract.
	Levels []postgresBidirectionalDiagnosticLevel `json:"levels"`
}

// postgresBidirectionalDiagnosticLevel groups state that must remain consistent while processing postgres bidirectional diagnostic level.
type postgresBidirectionalDiagnosticLevel struct {
	// SearchID identifies the search id.
	SearchID *int64 `json:"search_id"`
	// ActionIndex supplies the action index input to the postgresBidirectionalDiagnosticLevel contract.
	ActionIndex *int64 `json:"action_index"`
	// Side supplies the side input to the postgresBidirectionalDiagnosticLevel contract.
	Side string `json:"side"`
	// Action supplies the action input to the postgresBidirectionalDiagnosticLevel contract.
	Action string `json:"action"`
	// Depth supplies the depth input to the postgresBidirectionalDiagnosticLevel contract.
	Depth *int64 `json:"depth"`
	// FrontierRows records the number of frontier rows.
	FrontierRows *int64 `json:"frontier_rows"`
	// CandidateEdges supplies the candidate edges input to the postgresBidirectionalDiagnosticLevel contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the postgresBidirectionalDiagnosticLevel contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenRows records the number of seen rows.
	SeenRows *int64 `json:"seen_rows"`
	// QueueRows records the number of queue rows.
	QueueRows *int64 `json:"queue_rows"`
	// PredecessorRows records the number of predecessor rows.
	PredecessorRows *int64 `json:"predecessor_rows"`
	// MeetingCandidates supplies the meeting candidates input to the postgresBidirectionalDiagnosticLevel contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
}

// postgresBidirectionalAllShortestDiagnosticDocument defines the serialized representation of postgres bidirectional all shortest diagnostic.
type postgresBidirectionalAllShortestDiagnosticDocument struct {
	// SchemaVersion identifies the schema version for schema version.
	SchemaVersion int `json:"schema_version"`
	// InvocationID identifies the invocation id.
	InvocationID string `json:"invocation_id"`
	// Scheduler supplies the scheduler input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	Scheduler string `json:"scheduler"`
	// StateLimit supplies the state limit input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	StateLimit *int64 `json:"state_limit"`
	// FrontierLimit supplies the frontier limit input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	FrontierLimit *int64 `json:"frontier_limit"`
	// PredecessorLimit supplies the predecessor limit input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	PredecessorLimit *int64 `json:"predecessor_limit"`
	// EnumerationLimit supplies the enumeration limit input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	EnumerationLimit *int64 `json:"enumeration_limit"`
	// OutputBytesLimit supplies the output bytes limit input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	OutputBytesLimit *int64 `json:"output_bytes_limit"`
	// SearchCalls supplies the search calls input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	SearchCalls *int64 `json:"search_calls"`
	// RuntimeBranch supplies the runtime branch input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	RuntimeBranch string `json:"runtime_branch"`
	// Overflowed supplies the overflowed input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	Overflowed *bool `json:"overflowed"`
	// FallbackExecuted supplies the fallback executed input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	FallbackExecuted *bool `json:"fallback_executed"`
	// Counters supplies the counters input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	Counters *postgresBidirectionalAllShortestDiagnosticCounts `json:"counters"`
	// Calls supplies the calls input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	Calls []postgresBidirectionalAllShortestDiagnosticCall `json:"calls"`
	// WorkspaceBytes supplies the workspace bytes input to the postgresBidirectionalAllShortestDiagnosticDocument contract.
	WorkspaceBytes int64 `json:"-"`
}

// postgresBidirectionalAllShortestDiagnosticCounts aggregates counters observed while evaluating postgres bidirectional all shortest diagnostic.
type postgresBidirectionalAllShortestDiagnosticCounts struct {
	// SchedulerActions supplies the scheduler actions input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	SchedulerActions *int64 `json:"scheduler_actions"`
	// CandidateEdges supplies the candidate edges input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenPeak supplies the seen peak input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	SeenPeak *int64 `json:"seen_peak"`
	// FrontierPeak supplies the frontier peak input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	FrontierPeak *int64 `json:"frontier_peak"`
	// QueuePeak supplies the queue peak input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	QueuePeak *int64 `json:"queue_peak"`
	// PredecessorPeak supplies the predecessor peak input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingCandidates supplies the meeting candidates input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// FrozenDistance supplies the frozen distance input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	FrozenDistance *int64 `json:"frozen_distance"`
	// WitnessRows records the number of witness rows.
	WitnessRows *int64 `json:"witness_rows"`
	// SameDepthPredecessorAdditions supplies the same depth predecessor additions input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	SameDepthPredecessorAdditions *int64 `json:"same_depth_predecessor_additions"`
	// MeetingNodes supplies the meeting nodes input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	MeetingNodes *int64 `json:"meeting_nodes"`
	// CutDepth supplies the cut depth input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	CutDepth *int64 `json:"cut_depth"`
	// PathCountEstimate supplies the path count estimate input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	PathCountEstimate *int64 `json:"path_count_estimate"`
	// PathCountSaturated supplies the path count saturated input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	PathCountSaturated *bool `json:"path_count_saturated"`
	// EnumeratedCandidates supplies the enumerated candidates input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	EnumeratedCandidates *int64 `json:"enumerated_candidates"`
	// DuplicateRejects supplies the duplicate rejects input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	DuplicateRejects *int64 `json:"duplicate_rejects"`
	// OutputPaths identifies the filesystem output paths.
	OutputPaths *int64 `json:"output_paths"`
	// OutputEdgeCells supplies the output edge cells input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	OutputEdgeCells *int64 `json:"output_edge_cells"`
	// OutputBytes supplies the output bytes input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	OutputBytes *int64 `json:"output_bytes"`
	// Levels supplies the levels input to the postgresBidirectionalAllShortestDiagnosticCounts contract.
	Levels []postgresBidirectionalDiagnosticLevel `json:"levels"`
}

// postgresBidirectionalAllShortestDiagnosticCall groups state that must remain consistent while processing postgres bidirectional all shortest diagnostic call.
type postgresBidirectionalAllShortestDiagnosticCall struct {
	// SearchID identifies the search id.
	SearchID *int64 `json:"search_id"`
	// SourceID identifies the source id.
	SourceID *int64 `json:"source_id"`
	// TargetID identifies the target id.
	TargetID *int64 `json:"target_id"`
	// RuntimeBranch supplies the runtime branch input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	RuntimeBranch string `json:"runtime_branch"`
	// SchedulerActions supplies the scheduler actions input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	SchedulerActions *int64 `json:"scheduler_actions"`
	// CandidateEdges supplies the candidate edges input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenPeak supplies the seen peak input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	SeenPeak *int64 `json:"seen_peak"`
	// FrontierPeak supplies the frontier peak input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	FrontierPeak *int64 `json:"frontier_peak"`
	// QueuePeak supplies the queue peak input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	QueuePeak *int64 `json:"queue_peak"`
	// PredecessorPeak supplies the predecessor peak input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingCandidates supplies the meeting candidates input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// FrozenDistance supplies the frozen distance input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	FrozenDistance *int64 `json:"frozen_distance"`
	// WitnessRows records the number of witness rows.
	WitnessRows *int64 `json:"witness_rows"`
	// SameDepthPredecessorAdditions supplies the same depth predecessor additions input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	SameDepthPredecessorAdditions *int64 `json:"same_depth_predecessor_additions"`
	// MeetingNodes supplies the meeting nodes input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	MeetingNodes *int64 `json:"meeting_nodes"`
	// CutDepth supplies the cut depth input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	CutDepth *int64 `json:"cut_depth"`
	// PathCountEstimate supplies the path count estimate input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	PathCountEstimate *int64 `json:"path_count_estimate"`
	// PathCountSaturated supplies the path count saturated input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	PathCountSaturated *bool `json:"path_count_saturated"`
	// EnumeratedCandidates supplies the enumerated candidates input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	EnumeratedCandidates *int64 `json:"enumerated_candidates"`
	// DuplicateRejects supplies the duplicate rejects input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	DuplicateRejects *int64 `json:"duplicate_rejects"`
	// OutputPaths identifies the filesystem output paths.
	OutputPaths *int64 `json:"output_paths"`
	// OutputEdgeCells supplies the output edge cells input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	OutputEdgeCells *int64 `json:"output_edge_cells"`
	// OutputBytes supplies the output bytes input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	OutputBytes *int64 `json:"output_bytes"`
	// Overflowed supplies the overflowed input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	Overflowed *bool `json:"overflowed"`
	// FallbackExecuted supplies the fallback executed input to the postgresBidirectionalAllShortestDiagnosticCall contract.
	FallbackExecuted *bool `json:"fallback_executed"`
}

// attachPostgresTraversalTelemetry runs only after every timed case,
// reference, raw-PGX, and concurrency sample has completed.
func (s *postgresSQLRunner) attachPostgresTraversalTelemetry(ctx context.Context, record *CaseResult, parameters map[string]any) error {
	if s.traversalTelemetry == "" || s.traversalTelemetry == postgresTraversalTelemetryOff {
		for idx := range record.PostgresReferences {
			record.PostgresReferences[idx].traversalTelemetryParameters = nil
		}
		return nil
	}

	level := TraversalTelemetryLevel(s.traversalTelemetry)
	if record.Optimization != nil && record.PostgresMetrics != nil {
		telemetry, err := buildPostgresCaseTraversalTelemetry(*record.Optimization, *record.PostgresMetrics, s.backendPID, level)
		if err != nil {
			return fmt.Errorf("build PostgreSQL case traversal telemetry: %w", err)
		}
		if telemetry != nil {
			if level == TraversalTelemetryLevelDiagnostic {
				enrichOrientationTraversalTelemetry(
					telemetry,
					*record.PostgresMetrics,
					record.RowCount,
					record.ObservedRows,
					orientationPolicyMaximumDepth(*record.Optimization, telemetry.Summary.EmittedIdentity),
				)
				enrichInlinePredecessorTraversalTelemetry(telemetry, *record.PostgresMetrics, record.RowCount, record.ObservedRows)
				if err := s.enrichBidirectionalTraversalTelemetry(ctx, telemetry, record.SQL, parameters, record.RowCount, record.ObservedRows, *record.PostgresMetrics); err != nil {
					return fmt.Errorf("capture PostgreSQL case traversal telemetry: %w", err)
				}
			}
			record.TraversalTelemetry = telemetry
		}
	}

	for idx := range record.PostgresReferences {
		reference := &record.PostgresReferences[idx]
		parameters := reference.traversalTelemetryParameters
		reference.traversalTelemetryParameters = nil
		telemetry, err := buildPostgresReferenceTraversalTelemetry(*reference, parameters, s.backendPID, level)
		if err != nil {
			return fmt.Errorf("build PostgreSQL reference %s traversal telemetry: %w", reference.Name, err)
		}
		if telemetry == nil {
			continue
		}
		if level == TraversalTelemetryLevelDiagnostic {
			if err := s.enrichBidirectionalTraversalTelemetry(ctx, telemetry, reference.SQL, parameters, reference.RowCount, reference.ObservedRows, *reference.PostgresMetrics); err != nil {
				return fmt.Errorf("capture PostgreSQL reference %s traversal telemetry: %w", reference.Name, err)
			}
		}
		reference.TraversalTelemetry = telemetry
	}
	return nil
}

// enrichInlineASPTraversalTelemetry maps the guarded statement's named CTEs
// to its dedicated bounded-work contract. Public observation bytes are a
// conservative ceiling for the staged edge-array bytes used by admission.
func enrichInlineASPTraversalTelemetry(telemetry *TraversalExecutionTelemetry, metrics PostgresPlanMetrics, outputRows int64, observedRows []string) {
	enrichInlinePredecessorTraversalTelemetry(telemetry, metrics, outputRows, observedRows)
}

// enrichInlinePredecessorTraversalTelemetry maps the shared guarded I1
// statement's named CTEs to either the all-paths or canonical one-path counter
// family. The separate serialized fields prevent evidence from one public
// observation contract from satisfying the other.
func enrichInlinePredecessorTraversalTelemetry(telemetry *TraversalExecutionTelemetry, metrics PostgresPlanMetrics, outputRows int64, observedRows []string) {
	if telemetry == nil || telemetry.Diagnostic == nil ||
		(telemetry.Summary.EmittedIdentity != optimize.ShortestPathPolicyASPI1GuardedV1 &&
			telemetry.Summary.EmittedIdentity != optimize.ShortestPathPolicyI1CanonicalGuardedV1) {
		return
	}
	plan := telemetry.Diagnostic.PlanReplay
	if plan == nil {
		return
	}
	requiredPlanCounters := []string{
		"asp_i1_distance_rows",
		"asp_i1_predecessor_rows",
		"asp_i1_enumeration_rows",
		"asp_i1_output_rows",
		"asp_i1_candidate_marker_rows",
		"asp_i1_fallback_marker_rows",
		"asp_i1_candidate_branch_rows",
		"asp_i1_fallback_branch_rows",
		"asp_i1_candidate_executor_loops",
		"asp_i1_fallback_executor_loops",
	}
	var missingPlanCounters []string
	for _, name := range requiredPlanCounters {
		if _, present := plan.Counters[name]; !present {
			missingPlanCounters = append(missingPlanCounters, name)
		}
	}
	if len(missingPlanCounters) > 0 {
		markTraversalCountersUnavailable(
			telemetry.Diagnostic,
			"inline predecessor plan replay is missing exact named counters: "+strings.Join(missingPlanCounters, ", "),
		)
		return
	}
	get := func(name string) int64 { return plan.Counters[name] }
	outputBytes := int64(0)
	for _, row := range observedRows {
		outputBytes += int64(len(row))
	}
	inline := &InlinePredecessorTraversalCounters{
		DistanceRows:           traversalTelemetryPointer(get("asp_i1_distance_rows")),
		PredecessorRows:        traversalTelemetryPointer(get("asp_i1_predecessor_rows")),
		EnumerationRows:        traversalTelemetryPointer(get("asp_i1_enumeration_rows")),
		OutputPaths:            traversalTelemetryPointer(outputRows),
		OutputBytes:            traversalTelemetryPointer(outputBytes),
		CandidateMarkerRows:    traversalTelemetryPointer(get("asp_i1_candidate_marker_rows")),
		FallbackMarkerRows:     traversalTelemetryPointer(get("asp_i1_fallback_marker_rows")),
		CandidateBranchRows:    traversalTelemetryPointer(get("asp_i1_candidate_branch_rows")),
		FallbackBranchRows:     traversalTelemetryPointer(get("asp_i1_fallback_branch_rows")),
		CandidateExecutorLoops: traversalTelemetryPointer(get("asp_i1_candidate_executor_loops")),
		FallbackExecutorLoops:  traversalTelemetryPointer(get("asp_i1_fallback_executor_loops")),
	}
	prefix := "inline_asp"
	if telemetry.Summary.EmittedIdentity == optimize.ShortestPathPolicyI1CanonicalGuardedV1 {
		prefix = "inline_shortest_path"
		telemetry.Diagnostic.Counters.InlineShortestPath = inline
	} else {
		telemetry.Diagnostic.Counters.InlineASP = inline
	}
	if telemetry.Diagnostic.Provenance == nil {
		telemetry.Diagnostic.Provenance = map[string]string{}
	}
	for _, name := range []string{
		"distance_rows", "predecessor_rows", "enumeration_rows", "candidate_marker_rows",
		"fallback_marker_rows", "candidate_branch_rows", "fallback_branch_rows",
		"candidate_executor_loops", "fallback_executor_loops",
	} {
		telemetry.Diagnostic.Provenance[prefix+"."+name] = "untimed_timing_on_plan.inline_predecessor_named_ctes"
	}
	telemetry.Diagnostic.Provenance[prefix+".output_paths"] = "exact_public_observation.row_count"
	telemetry.Diagnostic.Provenance[prefix+".output_bytes"] = "exact_public_observation.conservative_serialized_bytes"

	if slices.Contains(telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) {
		telemetry.Diagnostic.Counters.Hydration = &TraversalHydrationCounters{
			PathCount:   traversalTelemetryPointer(outputRows),
			NodeLookups: traversalTelemetryPointer(metrics.HydrationLoops),
			EdgeLookups: traversalTelemetryPointer(metrics.HydrationRows),
			Loops:       traversalTelemetryPointer(metrics.HydrationLoops),
			Rows:        traversalTelemetryPointer(metrics.HydrationRows),
			TimeNS:      traversalTelemetryPointer(int64(0)),
			Bytes:       traversalTelemetryPointer(outputBytes),
		}
		for _, name := range []string{"path_count", "node_lookups", "edge_lookups", "loops", "rows", "time_ns", "bytes"} {
			telemetry.Diagnostic.Provenance["hydration."+name] = "untimed_plan_and_exact_public_observation"
		}
	}
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusComplete
	telemetry.Diagnostic.IncompleteReasons = nil
}

// enrichOrientationTraversalTelemetry turns explicitly named SQL probe and
// branch nodes into a complete, conservative diagnostic document. Probe times
// come from the untimed TIMING ON JSON EXPLAIN replay; hydration bytes use the
// captured public observation, never an estimated tuple width.
func enrichOrientationTraversalTelemetry(telemetry *TraversalExecutionTelemetry, metrics PostgresPlanMetrics, outputRows int64, observedRows []string, maximumDepth int64) {
	if telemetry == nil || telemetry.Diagnostic == nil || !isOrientationProbePolicy(telemetry.Summary.EmittedIdentity) {
		return
	}
	if telemetry.Summary.EmittedIdentity == string(optimize.ExpansionSearchPolicyOrientationProbeV2) && maximumDepth <= 0 {
		markTraversalCountersUnavailable(telemetry.Diagnostic, "orientation-probe-v2 maximum depth is unavailable")
		return
	}
	plan := telemetry.Diagnostic.PlanReplay
	if plan == nil {
		return
	}
	get := func(name string) int64 { return plan.Counters[name] }
	forwardSeeds := get("orientation_root_probe_rows")
	reverseSeeds := get("orientation_suffix_probe_rows")
	boundaries := get("orientation_boundary_rows")
	forwardDegree := get("orientation_forward_degree_rows")
	reverseDegree := get("orientation_reverse_degree_rows")
	stateRows := get("orientation_state_rows")
	probeRows := forwardSeeds + reverseSeeds + boundaries + forwardDegree + reverseDegree
	duplicateSeeds := max(reverseSeeds-boundaries, int64(0))
	shallowSurvivalRows := boundaries
	shallowSurvival := float64(0)
	if reverseSeeds > 0 {
		shallowSurvival = float64(boundaries) / float64(reverseSeeds)
	}
	forwardScore := float64(forwardSeeds + forwardDegree)
	if telemetry.Summary.EmittedIdentity == string(optimize.ExpansionSearchPolicyOrientationProbeV2) {
		forwardScore = float64(forwardSeeds + maximumDepth*forwardDegree)
	}
	reverseScore := float64(reverseSeeds + boundaries + reverseDegree)
	selectedSide := "forward"
	if telemetry.Summary.RuntimeIdentity != telemetry.Summary.FallbackIdentity && strings.Contains(telemetry.Summary.RuntimeIdentity, "REVERSE") {
		selectedSide = "reverse"
	}
	overflow := false
	if telemetry.Summary.Overflow != nil {
		overflow = *telemetry.Summary.Overflow
	}
	branchLoops := get("orientation_candidate_branch_loops") + get("orientation_incumbent_branch_loops")

	var probeTimeNS, probeHits, probeReads, edgeCandidates, repeatRejects, hydrationLoops, hydrationRows, hydrationTimeNS int64
	for _, node := range metrics.PlanNodes {
		identity := strings.ToLower(strings.Join([]string{node.CTEName, node.Alias, node.SubplanName}, " "))
		rows := node.ActualRows * node.ActualLoops
		if strings.Contains(identity, "orientation_") && (strings.Contains(identity, "_probe") || strings.Contains(identity, "_boundaries") || strings.Contains(identity, "_decision")) {
			probeTimeNS += int64(node.ActualTotalMS * float64(time.Millisecond))
			probeHits += node.Buffers.SharedHit + node.Buffers.LocalHit
			probeReads += node.Buffers.SharedRead + node.Buffers.LocalRead
		}
		if node.RelationName == "edge" {
			edgeCandidates += rows + node.RowsRemovedByFilter
			repeatRejects += node.RowsRemovedByFilter
		}
		if strings.Contains(identity, "hydrat") || strings.Contains(identity, "materializ") {
			hydrationLoops += node.ActualLoops
			hydrationRows += rows
			hydrationTimeNS += int64(node.ActualTotalMS * float64(time.Millisecond))
		}
	}
	orientation := &OrientationTraversalCounters{
		ForwardSeeds:                  traversalTelemetryPointer(forwardSeeds),
		ReverseSeeds:                  traversalTelemetryPointer(reverseSeeds),
		DuplicateSeeds:                traversalTelemetryPointer(duplicateSeeds),
		SuffixRows:                    traversalTelemetryPointer(reverseSeeds),
		DistinctBoundaries:            traversalTelemetryPointer(boundaries),
		TypedDirectionalDegreeSamples: traversalTelemetryPointer(forwardDegree + reverseDegree),
		ForwardDegreeSamples:          traversalTelemetryPointer(forwardDegree),
		ReverseDegreeSamples:          traversalTelemetryPointer(reverseDegree),
		ShallowSurvivalRows:           traversalTelemetryPointer(shallowSurvivalRows),
		ShallowSurvival:               traversalTelemetryPointer(shallowSurvival),
		ProbeRows:                     traversalTelemetryPointer(probeRows),
		ProbeTimeNS:                   traversalTelemetryPointer(probeTimeNS),
		ProbeBufferHits:               traversalTelemetryPointer(probeHits),
		ProbeBufferReads:              traversalTelemetryPointer(probeReads),
		ForwardScore:                  traversalTelemetryPointer(forwardScore),
		ReverseScore:                  traversalTelemetryPointer(reverseScore),
		SelectedSide:                  selectedSide,
		SentinelOverflow:              traversalTelemetryPointer(overflow),
		BranchLoops:                   traversalTelemetryPointer(branchLoops),
	}
	ordinary := &OrdinaryTraversalCounters{
		Roots:                     traversalTelemetryPointer(forwardSeeds),
		EdgeCandidates:            traversalTelemetryPointer(edgeCandidates),
		AdmittedStates:            traversalTelemetryPointer(stateRows),
		RelationshipRepeatRejects: traversalTelemetryPointer(repeatRejects),
		RecursiveRows:             traversalTelemetryPointer(metrics.RecursiveRows),
		PeakState:                 traversalTelemetryPointer(stateRows),
		EmittedTrails:             traversalTelemetryPointer(outputRows),
		HydrationRows:             traversalTelemetryPointer(metrics.HydrationRows),
	}
	telemetry.Diagnostic.Counters.Orientation = orientation
	telemetry.Diagnostic.Counters.Ordinary = ordinary
	telemetry.Diagnostic.Provenance = map[string]string{}
	for _, name := range []string{"forward_seeds", "reverse_seeds", "duplicate_seeds", "suffix_rows", "distinct_boundaries", "typed_directional_degree_samples", "forward_degree_samples", "reverse_degree_samples", "shallow_survival_rows", "shallow_survival", "probe_rows", "probe_time_ns", "probe_buffer_hits", "probe_buffer_reads", "forward_score", "reverse_score", "selected_side", "sentinel_overflow", "branch_loops"} {
		telemetry.Diagnostic.Provenance["orientation."+name] = "untimed_timing_on_plan.orientation_named_ctes"
	}
	for _, name := range []string{"roots", "edge_candidates", "admitted_states", "relationship_repeat_rejects", "recursive_rows", "peak_state", "emitted_trails", "hydration_rows"} {
		telemetry.Diagnostic.Provenance["ordinary."+name] = "untimed_timing_on_plan.executed_orientation_branch"
	}
	if slices.Contains(telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) {
		bytes := int64(0)
		for _, row := range observedRows {
			bytes += int64(len(row))
		}
		nodeLookups := metrics.HydrationLoops
		edgeLookups := metrics.HydrationRows
		telemetry.Diagnostic.Counters.Hydration = &TraversalHydrationCounters{
			PathCount:   traversalTelemetryPointer(outputRows),
			NodeLookups: traversalTelemetryPointer(nodeLookups),
			EdgeLookups: traversalTelemetryPointer(edgeLookups),
			Loops:       traversalTelemetryPointer(hydrationLoops),
			Rows:        traversalTelemetryPointer(hydrationRows),
			TimeNS:      traversalTelemetryPointer(hydrationTimeNS),
			Bytes:       traversalTelemetryPointer(bytes),
		}
		for _, name := range []string{"path_count", "node_lookups", "edge_lookups", "loops", "rows", "time_ns", "bytes"} {
			telemetry.Diagnostic.Provenance["hydration."+name] = "untimed_timing_on_plan_and_exact_public_observation"
		}
	}
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusComplete
	telemetry.Diagnostic.IncompleteReasons = nil
}

// orientationPolicyMaximumDepth supports benchmark evidence processing for orientation policy maximum depth.
func orientationPolicyMaximumDepth(summary translate.OptimizationSummary, policy string) int64 {
	if !isOrientationProbePolicy(policy) {
		return 0
	}
	for _, outcome := range summary.TargetOutcomes {
		if outcome.EmittedPolicy == policy && outcome.MaximumDepth != nil {
			return *outcome.MaximumDepth
		}
	}
	return 0
}

// enrichBidirectionalTraversalTelemetry replaces opaque Function Scan
// evidence only when the exact SP-B1/B2 statement reports a validated,
// invocation-local diagnostic document. Other hidden functions stay
// explicitly unavailable.
func (s *postgresSQLRunner) enrichBidirectionalTraversalTelemetry(
	ctx context.Context,
	telemetry *TraversalExecutionTelemetry,
	sqlQuery string,
	parameters map[string]any,
	expectedRows int64,
	observedRows []string,
	metrics PostgresPlanMetrics,
) error {
	if telemetry == nil || telemetry.Level != TraversalTelemetryLevelDiagnostic || !isBidirectionalTelemetryIdentity(telemetry.Summary) {
		return nil
	}
	identity := bidirectionalTelemetryIdentity(telemetry.Summary)

	invocationID := newRunUUID()
	if telemetry.Diagnostic != nil {
		invocationID = telemetry.Diagnostic.InvocationID
	}
	var (
		unavailableReason string
		err               error
	)
	if isBidirectionalASPIdentity(identity) {
		var document *postgresBidirectionalAllShortestDiagnosticDocument
		document, unavailableReason, err = s.replayBidirectionalAllShortestTraversalDiagnostic(ctx, invocationID, sqlQuery, parameters, expectedRows)
		if err == nil && unavailableReason == "" {
			err = applyBidirectionalAllShortestTraversalDiagnostic(telemetry, document, invocationID, s.backendPID)
			if err == nil {
				enrichBidirectionalHydrationTelemetry(telemetry, document.Counters.OutputPaths, document.Counters.OutputEdgeCells, observedRows, metrics)
			}
		}
	} else {
		var document *postgresBidirectionalDiagnosticDocument
		document, unavailableReason, err = s.replayBidirectionalTraversalDiagnostic(ctx, invocationID, sqlQuery, parameters, expectedRows)
		if err == nil && unavailableReason == "" {
			err = applyBidirectionalTraversalDiagnostic(telemetry, document, invocationID, s.backendPID)
			if err == nil {
				pathCount := document.Counters.WitnessRows
				edgeCells := int64(0)
				if document.Counters.FrozenDistance != nil && *document.Counters.FrozenDistance > 0 && pathCount != nil {
					edgeCells = *document.Counters.FrozenDistance * *pathCount
				}
				enrichBidirectionalHydrationTelemetry(telemetry, pathCount, traversalTelemetryPointer(edgeCells), observedRows, metrics)
			}
		}
	}
	if err != nil {
		if telemetry.Diagnostic == nil {
			markTraversalSummaryUnavailable(telemetry, err.Error())
			return telemetry.Validate()
		}
		markTraversalCountersUnavailable(telemetry.Diagnostic, err.Error())
		return telemetry.Validate()
	}
	if unavailableReason != "" {
		if telemetry.Diagnostic == nil {
			markTraversalSummaryUnavailable(telemetry, unavailableReason)
			return telemetry.Validate()
		}
		markTraversalCountersUnavailable(telemetry.Diagnostic, unavailableReason)
		return telemetry.Validate()
	}
	return telemetry.Validate()
}

// enrichBidirectionalHydrationTelemetry supports benchmark evidence processing for enrich bidirectional hydration telemetry.
func enrichBidirectionalHydrationTelemetry(
	telemetry *TraversalExecutionTelemetry,
	pathCount, edgeCells *int64,
	observedRows []string,
	metrics PostgresPlanMetrics,
) {
	if telemetry == nil || telemetry.Diagnostic == nil || !slices.Contains(telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) || pathCount == nil || edgeCells == nil {
		return
	}
	bytes := int64(0)
	for _, row := range observedRows {
		bytes += int64(len(row))
	}
	nodeLookups := *edgeCells + *pathCount
	var hydrationTimeNS int64
	for _, node := range metrics.PlanNodes {
		identity := strings.ToLower(strings.Join([]string{node.CTEName, node.Alias, node.SubplanName}, " "))
		if strings.Contains(identity, "hydrat") || strings.Contains(identity, "materializ") || node.RelationName == "node" {
			hydrationTimeNS += int64(node.ActualTotalMS * float64(time.Millisecond))
		}
	}
	rows := metrics.HydrationRows
	if rows == 0 {
		rows = nodeLookups + *edgeCells
	}
	loops := metrics.HydrationLoops
	telemetry.Diagnostic.Counters.Hydration = &TraversalHydrationCounters{
		PathCount:   pathCount,
		NodeLookups: traversalTelemetryPointer(nodeLookups),
		EdgeLookups: edgeCells,
		Loops:       traversalTelemetryPointer(loops),
		Rows:        traversalTelemetryPointer(rows),
		TimeNS:      traversalTelemetryPointer(hydrationTimeNS),
		Bytes:       traversalTelemetryPointer(bytes),
	}
	for _, name := range []string{"path_count", "node_lookups", "edge_lookups", "loops", "rows", "time_ns", "bytes"} {
		telemetry.Diagnostic.Provenance["hydration."+name] = "invocation_local_path_counts+untimed_timing_on_plan+exact_public_observation"
	}
	if telemetry.Summary.FallbackExecuted != nil && !*telemetry.Summary.FallbackExecuted {
		telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusComplete
		telemetry.Diagnostic.IncompleteReasons = nil
	}
}

// replayBidirectionalTraversalDiagnostic executes the exact statement in a
// separate repeatable-read transaction on the runner's single physical
// connection. Its duration and counters are never added to latency samples.
func (s *postgresSQLRunner) replayBidirectionalTraversalDiagnostic(
	ctx context.Context,
	invocationID string,
	sqlQuery string,
	parameters map[string]any,
	expectedRows int64,
) (*postgresBidirectionalDiagnosticDocument, string, error) {
	rawDocument, workspaceBytes, unavailableReason, err := s.replayInvocationLocalTraversalDiagnostic(
		ctx, invocationID, sqlQuery, parameters, expectedRows,
		"select public.begin_bidirectional_shortest_path_diagnostic_v1($1)",
		"select coalesce(public.read_bidirectional_shortest_path_diagnostic_v1($1)::text, '')",
		"select public.clear_bidirectional_shortest_path_diagnostic_v1($1)",
	)
	if err != nil || unavailableReason != "" {
		return nil, unavailableReason, err
	}
	document := &postgresBidirectionalDiagnosticDocument{}
	if err := json.Unmarshal([]byte(rawDocument), document); err != nil {
		return nil, "diagnostic reader returned malformed JSON: " + err.Error(), nil
	}
	document.WorkspaceBytes = workspaceBytes
	return document, "", nil
}

// replayBidirectionalAllShortestTraversalDiagnostic supports benchmark evidence processing for replay bidirectional all shortest traversal diagnostic.
func (s *postgresSQLRunner) replayBidirectionalAllShortestTraversalDiagnostic(
	ctx context.Context,
	invocationID string,
	sqlQuery string,
	parameters map[string]any,
	expectedRows int64,
) (*postgresBidirectionalAllShortestDiagnosticDocument, string, error) {
	rawDocument, workspaceBytes, unavailableReason, err := s.replayInvocationLocalTraversalDiagnostic(
		ctx, invocationID, sqlQuery, parameters, expectedRows,
		"select public.begin_bidirectional_all_shortest_path_diagnostic_v1($1)",
		"select coalesce(public.read_bidirectional_all_shortest_path_diagnostic_v1($1)::text, '')",
		"select public.clear_bidirectional_all_shortest_path_diagnostic_v1($1)",
	)
	if err != nil || unavailableReason != "" {
		return nil, unavailableReason, err
	}
	document := &postgresBidirectionalAllShortestDiagnosticDocument{}
	if err := json.Unmarshal([]byte(rawDocument), document); err != nil {
		return nil, "all-shortest diagnostic reader returned malformed JSON: " + err.Error(), nil
	}
	document.WorkspaceBytes = workspaceBytes
	return document, "", nil
}

// replayInvocationLocalTraversalDiagnostic supports benchmark evidence processing for replay invocation local traversal diagnostic.
func (s *postgresSQLRunner) replayInvocationLocalTraversalDiagnostic(
	ctx context.Context,
	invocationID string,
	sqlQuery string,
	parameters map[string]any,
	expectedRows int64,
	beginSQL string,
	readSQL string,
	clearSQL string,
) (string, int64, string, error) {
	connection, err := s.pool.Acquire(ctx)
	if err != nil {
		return "", 0, "", fmt.Errorf("acquire diagnostic connection: %w", err)
	}
	defer connection.Release()

	var backendPID int32
	if err := connection.QueryRow(ctx, "select pg_backend_pid()").Scan(&backendPID); err != nil {
		return "", 0, "", fmt.Errorf("read diagnostic connection identity: %w", err)
	}
	connectionID := strconv.FormatInt(int64(backendPID), 10)
	if connectionID != s.backendPID {
		return "", 0, "", fmt.Errorf("diagnostic connection identity %s differs from timed-sample connection %s", connectionID, s.backendPID)
	}

	tx, err := connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return "", 0, "", fmt.Errorf("begin repeatable-read diagnostic transaction: %w", err)
	}
	initialized := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if initialized {
			_, _ = tx.Exec(cleanupCtx, clearSQL, invocationID)
		}
		_ = tx.Rollback(cleanupCtx)
	}()

	if _, err := tx.Exec(ctx, beginSQL, invocationID); err != nil {
		return "", 0, "", fmt.Errorf("begin invocation-local diagnostic: %w", err)
	}
	initialized = true

	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}}
	if len(parameters) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(parameters))
	}
	rows, err := tx.Query(ctx, sqlQuery, queryArgs...)
	if err != nil {
		return "", 0, "", fmt.Errorf("execute untimed diagnostic replay: %w", err)
	}
	var rowCount int64
	for rows.Next() {
		rowCount++
		if _, err := rows.Values(); err != nil {
			rows.Close()
			return "", 0, "", fmt.Errorf("decode untimed diagnostic replay: %w", err)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", 0, "", fmt.Errorf("drain untimed diagnostic replay: %w", err)
	}
	if rowCount != expectedRows {
		return "", 0, "", fmt.Errorf("untimed diagnostic replay row count %d differs from measured row count %d", rowCount, expectedRows)
	}
	var workspaceBytes int64
	if err := tx.QueryRow(ctx, `
		select coalesce(sum(pg_total_relation_size(c.oid)), 0)::int8
		from pg_class c
		where c.relnamespace = pg_my_temp_schema()
		  and (c.relname like 'spb_%' or c.relname like 'asb_%')
		  and c.relname not like '%telemetry%'
	`).Scan(&workspaceBytes); err != nil {
		return "", 0, "", fmt.Errorf("measure diagnostic workspace high-water bytes: %w", err)
	}

	var replayBackendPID int32
	if err := tx.QueryRow(ctx, "select pg_backend_pid()").Scan(&replayBackendPID); err != nil {
		return "", 0, "", fmt.Errorf("verify diagnostic transaction connection identity: %w", err)
	}
	if replayBackendPID != backendPID {
		return "", 0, "", fmt.Errorf("diagnostic transaction changed physical connection from %d to %d", backendPID, replayBackendPID)
	}

	var rawDocument string
	if err := tx.QueryRow(ctx, readSQL, invocationID).Scan(&rawDocument); err != nil {
		return "", 0, "", fmt.Errorf("read invocation-local diagnostic: %w", err)
	}
	if _, err := tx.Exec(ctx, clearSQL, invocationID); err != nil {
		return "", 0, "", fmt.Errorf("clear invocation-local diagnostic: %w", err)
	}
	initialized = false
	if err := tx.Commit(ctx); err != nil {
		return "", 0, "", fmt.Errorf("commit cleared diagnostic transaction: %w", err)
	}

	if strings.TrimSpace(rawDocument) == "" {
		return "", workspaceBytes, "diagnostic reader returned no document for this invocation", nil
	}
	return rawDocument, workspaceBytes, "", nil
}

// applyBidirectionalTraversalDiagnostic applies bidirectional traversal diagnostic.
func applyBidirectionalTraversalDiagnostic(
	telemetry *TraversalExecutionTelemetry,
	document *postgresBidirectionalDiagnosticDocument,
	expectedInvocationID string,
	expectedConnectionID string,
) error {
	if telemetry == nil || document == nil {
		return fmt.Errorf("bidirectional diagnostic document is missing")
	}
	if document.SchemaVersion != 1 {
		return fmt.Errorf("bidirectional diagnostic schema_version must be 1")
	}
	if document.InvocationID != expectedInvocationID {
		return fmt.Errorf("bidirectional diagnostic invocation identity %q differs from requested %q", document.InvocationID, expectedInvocationID)
	}
	if telemetry.Diagnostic != nil && telemetry.Diagnostic.ConnectionID != expectedConnectionID {
		return fmt.Errorf("attached diagnostic connection identity %q differs from replay connection %q", telemetry.Diagnostic.ConnectionID, expectedConnectionID)
	}
	if document.SearchCalls == nil || *document.SearchCalls != 1 {
		return fmt.Errorf("instrumented singleton SP-B1/B2 replay must invoke exactly one search call")
	}
	if int64(len(document.Calls)) != *document.SearchCalls {
		return fmt.Errorf("bidirectional diagnostic call count %d differs from search_calls %d", len(document.Calls), *document.SearchCalls)
	}
	if document.RuntimeBranch == "" || document.RuntimeBranch == "missing" || document.RuntimeBranch == "mixed" {
		return fmt.Errorf("bidirectional diagnostic runtime branch is not singular")
	}
	if document.Overflowed == nil || document.FallbackExecuted == nil {
		return fmt.Errorf("bidirectional diagnostic runtime outcome flags are missing")
	}
	if err := validateDiagnosticRuntimeOutcome(document.RuntimeBranch, *document.Overflowed, *document.FallbackExecuted, "exact_s4_fallback", []string{
		"preflight_zero_hop", "preflight_one_hop", "preflight_two_hop", "preflight_no_path", "search_no_path", "bidirectional_search",
	}); err != nil {
		return fmt.Errorf("bidirectional diagnostic: %w", err)
	}
	if err := validateBidirectionalDiagnosticCalls(document.Calls, document.Overflowed, document.FallbackExecuted); err != nil {
		return err
	}
	if document.Calls[0].RuntimeBranch != document.RuntimeBranch {
		return fmt.Errorf("bidirectional diagnostic aggregate runtime branch differs from its call")
	}
	if strings.TrimSpace(document.Scheduler) == "" || document.Scheduler != telemetry.Summary.SchedulerVersion {
		return fmt.Errorf("bidirectional diagnostic scheduler %q differs from planned scheduler %q", document.Scheduler, telemetry.Summary.SchedulerVersion)
	}
	for name, observed := range map[string]*int64{
		"state_rows":       document.StateLimit,
		"frontier_rows":    document.FrontierLimit,
		"queue_rows":       document.FrontierLimit,
		"predecessor_rows": document.PredecessorLimit,
	} {
		planned, ok := telemetry.Summary.Caps[name]
		if !ok || observed == nil || *observed != planned {
			return fmt.Errorf("bidirectional diagnostic cap %s does not match the planned value", name)
		}
	}
	if document.Counters == nil {
		return fmt.Errorf("bidirectional diagnostic counters are missing")
	}
	if err := validateBidirectionalDiagnosticCounts(document.Counters); err != nil {
		return err
	}
	if err := validateBidirectionalSingleCallAggregate(document.Counters, document.Calls[0]); err != nil {
		return err
	}

	fallbackIdentity := bidirectionalFallbackIdentity(bidirectionalTelemetryIdentity(telemetry.Summary))
	if *document.FallbackExecuted {
		if fallbackIdentity == "" {
			return fmt.Errorf("bidirectional diagnostic reports fallback without a declared exact control")
		}
		if !slices.Contains(telemetry.Summary.PlannedIdentities, fallbackIdentity) {
			telemetry.Summary.PlannedIdentities = append(telemetry.Summary.PlannedIdentities, fallbackIdentity)
		}
		telemetry.Summary.RuntimeIdentity = fallbackIdentity
		telemetry.Summary.AppliedIdentity = fallbackIdentity
		telemetry.Summary.FallbackIdentity = fallbackIdentity
		telemetry.Summary.Provenance["fallback_identity"] = postgresBidirectionalDiagnosticSource + ".fallback_executed"
	} else {
		identity := bidirectionalTelemetryIdentity(telemetry.Summary)
		telemetry.Summary.RuntimeIdentity = identity
		telemetry.Summary.AppliedIdentity = identity
		telemetry.Summary.FallbackIdentity = ""
	}
	telemetry.Summary.RuntimeBranch = document.RuntimeBranch
	telemetry.Summary.RuntimeOutcomeAvailable = traversalTelemetryPointer(true)
	telemetry.Summary.Overflow = traversalTelemetryPointer(*document.Overflowed)
	telemetry.Summary.FallbackExecuted = traversalTelemetryPointer(*document.FallbackExecuted)
	for _, name := range []string{"runtime_identity", "applied_identity", "runtime_branch", "overflow", "fallback_executed", "scheduler_version"} {
		telemetry.Summary.Provenance[name] = postgresBidirectionalDiagnosticSource
	}
	telemetry.Summary.Provenance["runtime_outcome_available"] = postgresBidirectionalDiagnosticSource

	if telemetry.Diagnostic == nil {
		return nil
	}
	levels := make([]ShortestPathLevelCounters, len(document.Counters.Levels))
	for idx, level := range document.Counters.Levels {
		levels[idx] = ShortestPathLevelCounters{
			SearchID:          *level.SearchID,
			ActionIndex:       *level.ActionIndex,
			Side:              level.Side,
			Action:            level.Action,
			Depth:             level.Depth,
			FrontierRows:      level.FrontierRows,
			CandidateEdges:    level.CandidateEdges,
			DistinctNewNodes:  level.DistinctNewNodes,
			SeenRows:          level.SeenRows,
			QueueRows:         level.QueueRows,
			PredecessorRows:   level.PredecessorRows,
			MeetingCandidates: level.MeetingCandidates,
			Provenance:        fmt.Sprintf("%s.counters.levels[%d]", postgresBidirectionalDiagnosticSource, idx),
		}
	}
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusComplete
	telemetry.Diagnostic.IncompleteReasons = nil
	telemetry.Diagnostic.RequiredFamilies = traversalRequiredFamilies(telemetry.Summary, TraversalTelemetryFamilySP)
	telemetry.Diagnostic.Counters = TraversalDiagnosticCounters{ShortestPath: &ShortestPathTraversalCounters{
		SchedulerActions:  document.Counters.SchedulerActions,
		Levels:            levels,
		CandidateEdges:    document.Counters.CandidateEdges,
		DistinctNewNodes:  document.Counters.DistinctNewNodes,
		SeenPeak:          document.Counters.SeenPeak,
		FrontierPeak:      document.Counters.FrontierPeak,
		QueuePeak:         document.Counters.QueuePeak,
		PredecessorPeak:   document.Counters.PredecessorPeak,
		MeetingCandidates: document.Counters.MeetingCandidates,
		FrozenDistance:    document.Counters.FrozenDistance,
		WitnessRows:       document.Counters.WitnessRows,
		FallbackExecuted:  document.FallbackExecuted,
	}}
	telemetry.Diagnostic.Counters.Workspace = &TraversalWorkspaceCounters{
		SessionPeakBytes: traversalTelemetryPointer(document.WorkspaceBytes),
		PoolPeakBytes:    traversalTelemetryPointer(document.WorkspaceBytes),
	}
	telemetry.Diagnostic.Provenance = map[string]string{}
	for _, name := range []string{
		"scheduler_actions", "candidate_edges", "distinct_new_nodes", "seen_peak", "frontier_peak", "queue_peak",
		"predecessor_peak", "meeting_candidates", "frozen_distance", "witness_rows", "fallback_executed",
	} {
		telemetry.Diagnostic.Provenance["shortest_path."+name] = postgresBidirectionalDiagnosticSource + ".counters." + name
	}
	telemetry.Diagnostic.Provenance["workspace.session_peak_bytes"] = "pg_total_relation_size(pg_temp.spb_*)"
	telemetry.Diagnostic.Provenance["workspace.pool_peak_bytes"] = "single_connection_diagnostic_pool.session_peak_bytes"
	if *document.FallbackExecuted {
		// The document completely describes bounded B-candidate work and the
		// exact-fallback decision, but the nested S4 executor does not yet emit
		// its own edge/state counters. Keep the measured candidate evidence and
		// fail total-work qualification closed.
		telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
		telemetry.Diagnostic.IncompleteReasons = []string{"nested exact S4 fallback traversal work counters are unavailable"}
	}
	if slices.Contains(telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) {
		telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
		telemetry.Diagnostic.IncompleteReasons = append(telemetry.Diagnostic.IncompleteReasons, "complete invocation-local path hydration counters are unavailable")
	}
	return nil
}

// validateBidirectionalDiagnosticCounts validates bidirectional diagnostic counts.
func validateBidirectionalDiagnosticCounts(counters *postgresBidirectionalDiagnosticCounts) error {
	for name, value := range map[string]*int64{
		"scheduler_actions": counters.SchedulerActions, "candidate_edges": counters.CandidateEdges,
		"distinct_new_nodes": counters.DistinctNewNodes, "seen_peak": counters.SeenPeak,
		"frontier_peak": counters.FrontierPeak, "queue_peak": counters.QueuePeak,
		"predecessor_peak": counters.PredecessorPeak, "meeting_candidates": counters.MeetingCandidates,
		"witness_rows": counters.WitnessRows,
	} {
		if value == nil || *value < 0 {
			return fmt.Errorf("bidirectional diagnostic counter %s is missing or negative", name)
		}
	}
	if counters.FrozenDistance == nil || *counters.FrozenDistance < -1 {
		return fmt.Errorf("bidirectional diagnostic frozen_distance is missing or invalid")
	}
	if len(counters.Levels) == 0 {
		return fmt.Errorf("bidirectional diagnostic level counters are missing")
	}
	for idx, level := range counters.Levels {
		if level.SearchID == nil || level.ActionIndex == nil || *level.SearchID < 1 || *level.ActionIndex < 1 ||
			strings.TrimSpace(level.Side) == "" || strings.TrimSpace(level.Action) == "" {
			return fmt.Errorf("bidirectional diagnostic level %d has incomplete identity", idx)
		}
		for name, value := range map[string]*int64{
			"depth": level.Depth, "frontier_rows": level.FrontierRows, "candidate_edges": level.CandidateEdges,
			"distinct_new_nodes": level.DistinctNewNodes, "seen_rows": level.SeenRows, "queue_rows": level.QueueRows,
			"predecessor_rows": level.PredecessorRows, "meeting_candidates": level.MeetingCandidates,
		} {
			if value == nil || *value < 0 {
				return fmt.Errorf("bidirectional diagnostic level %d counter %s is missing or negative", idx, name)
			}
		}
	}
	return nil
}

// validateBidirectionalDiagnosticCalls validates bidirectional diagnostic calls.
func validateBidirectionalDiagnosticCalls(calls []postgresBidirectionalDiagnosticCall, overflowed, fallbackExecuted *bool) error {
	return validateBidirectionalDiagnosticCallsFor(calls, overflowed, fallbackExecuted, "exact_s4_fallback")
}

// validateBidirectionalDiagnosticCallsFor validates bidirectional diagnostic calls for.
func validateBidirectionalDiagnosticCallsFor(calls []postgresBidirectionalDiagnosticCall, overflowed, fallbackExecuted *bool, exactFallback string) error {
	seen := map[int64]struct{}{}
	anyOverflow, anyFallback := false, false
	for idx, call := range calls {
		if call.SearchID == nil || *call.SearchID < 1 || call.SourceID == nil || call.TargetID == nil {
			return fmt.Errorf("bidirectional diagnostic call %d has incomplete identity", idx)
		}
		if _, duplicate := seen[*call.SearchID]; duplicate {
			return fmt.Errorf("bidirectional diagnostic call %d repeats search_id %d", idx, *call.SearchID)
		}
		seen[*call.SearchID] = struct{}{}
		if call.RuntimeBranch == "" || call.RuntimeBranch == "started" {
			return fmt.Errorf("bidirectional diagnostic call %d did not finish", idx)
		}
		for name, value := range map[string]*int64{
			"scheduler_actions": call.SchedulerActions, "candidate_edges": call.CandidateEdges,
			"distinct_new_nodes": call.DistinctNewNodes, "seen_peak": call.SeenPeak,
			"frontier_peak": call.FrontierPeak, "queue_peak": call.QueuePeak,
			"predecessor_peak": call.PredecessorPeak, "meeting_candidates": call.MeetingCandidates,
			"witness_rows": call.WitnessRows,
		} {
			if value == nil || *value < 0 {
				return fmt.Errorf("bidirectional diagnostic call %d counter %s is missing or negative", idx, name)
			}
		}
		if call.Overflowed == nil || call.FallbackExecuted == nil {
			return fmt.Errorf("bidirectional diagnostic call %d outcome flags are missing", idx)
		}
		if err := validateDiagnosticRuntimeOutcome(call.RuntimeBranch, *call.Overflowed, *call.FallbackExecuted, exactFallback, []string{
			"preflight_zero_hop", "preflight_one_hop", "preflight_two_hop", "preflight_no_path", "search_no_path", "bidirectional_search",
		}); err != nil {
			return fmt.Errorf("bidirectional diagnostic call %d: %w", idx, err)
		}
		anyOverflow = anyOverflow || *call.Overflowed
		anyFallback = anyFallback || *call.FallbackExecuted
	}
	if overflowed == nil || fallbackExecuted == nil || anyOverflow != *overflowed || anyFallback != *fallbackExecuted {
		return fmt.Errorf("bidirectional diagnostic aggregate outcome differs from its calls")
	}
	return nil
}

// validateDiagnosticRuntimeOutcome validates diagnostic runtime outcome.
func validateDiagnosticRuntimeOutcome(branch string, overflowed, fallbackExecuted bool, exactFallback string, nonFallback []string) error {
	allowed := slices.Contains(nonFallback, branch) || branch == exactFallback
	if !allowed {
		return fmt.Errorf("runtime branch %q is unsupported", branch)
	}
	if fallbackExecuted != (branch == exactFallback) {
		return fmt.Errorf("runtime branch %q contradicts fallback_executed=%t", branch, fallbackExecuted)
	}
	if overflowed != fallbackExecuted {
		return fmt.Errorf("overflowed=%t contradicts fallback_executed=%t", overflowed, fallbackExecuted)
	}
	return nil
}

// validateBidirectionalSingleCallAggregate validates bidirectional single call aggregate.
func validateBidirectionalSingleCallAggregate(counters *postgresBidirectionalDiagnosticCounts, call postgresBidirectionalDiagnosticCall) error {
	for name, values := range map[string][2]*int64{
		"scheduler_actions":  {counters.SchedulerActions, call.SchedulerActions},
		"candidate_edges":    {counters.CandidateEdges, call.CandidateEdges},
		"distinct_new_nodes": {counters.DistinctNewNodes, call.DistinctNewNodes},
		"seen_peak":          {counters.SeenPeak, call.SeenPeak}, "frontier_peak": {counters.FrontierPeak, call.FrontierPeak},
		"queue_peak": {counters.QueuePeak, call.QueuePeak}, "predecessor_peak": {counters.PredecessorPeak, call.PredecessorPeak},
		"meeting_candidates": {counters.MeetingCandidates, call.MeetingCandidates},
		"frozen_distance":    {counters.FrozenDistance, call.FrozenDistance}, "witness_rows": {counters.WitnessRows, call.WitnessRows},
	} {
		if values[0] == nil || values[1] == nil || *values[0] != *values[1] {
			return fmt.Errorf("bidirectional diagnostic aggregate counter %s differs from its single call", name)
		}
	}
	for idx, level := range counters.Levels {
		if level.SearchID == nil || call.SearchID == nil || *level.SearchID != *call.SearchID {
			return fmt.Errorf("bidirectional diagnostic level %d is not attributed to its single call", idx)
		}
	}
	return nil
}

// applyBidirectionalAllShortestTraversalDiagnostic applies bidirectional all shortest traversal diagnostic.
func applyBidirectionalAllShortestTraversalDiagnostic(
	telemetry *TraversalExecutionTelemetry,
	document *postgresBidirectionalAllShortestDiagnosticDocument,
	expectedInvocationID string,
	expectedConnectionID string,
) error {
	if telemetry == nil || document == nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic document is missing")
	}
	if document.SchemaVersion != 1 {
		return fmt.Errorf("bidirectional all-shortest diagnostic schema_version must be 1")
	}
	if document.InvocationID != expectedInvocationID {
		return fmt.Errorf("bidirectional all-shortest diagnostic invocation identity %q differs from requested %q", document.InvocationID, expectedInvocationID)
	}
	if telemetry.Diagnostic != nil && telemetry.Diagnostic.ConnectionID != expectedConnectionID {
		return fmt.Errorf("attached diagnostic connection identity %q differs from replay connection %q", telemetry.Diagnostic.ConnectionID, expectedConnectionID)
	}
	if document.SearchCalls == nil || *document.SearchCalls != 1 {
		return fmt.Errorf("instrumented singleton ASP-B1/B2 replay must invoke exactly one search call")
	}
	if int64(len(document.Calls)) != *document.SearchCalls {
		return fmt.Errorf("bidirectional all-shortest diagnostic call count %d differs from search_calls %d", len(document.Calls), *document.SearchCalls)
	}
	if document.RuntimeBranch == "" || document.RuntimeBranch == "missing" || document.RuntimeBranch == "mixed" {
		return fmt.Errorf("bidirectional all-shortest diagnostic runtime branch is not singular")
	}
	if document.Overflowed == nil || document.FallbackExecuted == nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic runtime outcome flags are missing")
	}
	if err := validateDiagnosticRuntimeOutcome(document.RuntimeBranch, *document.Overflowed, *document.FallbackExecuted, "exact_a1_fallback", []string{
		"preflight_one_hop", "preflight_two_hop", "preflight_no_path", "search_no_path", "bidirectional_search",
	}); err != nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic: %w", err)
	}
	if strings.TrimSpace(document.Scheduler) == "" || document.Scheduler != telemetry.Summary.SchedulerVersion {
		return fmt.Errorf("bidirectional all-shortest diagnostic scheduler %q differs from planned scheduler %q", document.Scheduler, telemetry.Summary.SchedulerVersion)
	}
	for name, observed := range map[string]*int64{
		"state_rows":       document.StateLimit,
		"frontier_rows":    document.FrontierLimit,
		"queue_rows":       document.FrontierLimit,
		"predecessor_rows": document.PredecessorLimit,
		"output_rows":      document.EnumerationLimit,
		"output_bytes":     document.OutputBytesLimit,
	} {
		planned, ok := telemetry.Summary.Caps[name]
		if !ok || observed == nil || *observed != planned {
			return fmt.Errorf("bidirectional all-shortest diagnostic cap %s does not match the planned value", name)
		}
	}
	if document.Counters == nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic counters are missing")
	}
	if err := validateBidirectionalAllShortestDiagnosticCounts(document.Counters); err != nil {
		return err
	}
	if err := validateBidirectionalAllShortestDiagnosticCalls(document.Calls, document.Overflowed, document.FallbackExecuted); err != nil {
		return err
	}
	if document.Calls[0].RuntimeBranch != document.RuntimeBranch {
		return fmt.Errorf("bidirectional all-shortest diagnostic aggregate runtime branch differs from its call")
	}
	if err := validateBidirectionalAllShortestSingleCallAggregate(document.Counters, document.Calls[0]); err != nil {
		return err
	}

	fallbackIdentity := bidirectionalFallbackIdentity(bidirectionalTelemetryIdentity(telemetry.Summary))
	if *document.FallbackExecuted {
		if fallbackIdentity == "" {
			return fmt.Errorf("bidirectional all-shortest diagnostic reports fallback without a declared exact control")
		}
		if !slices.Contains(telemetry.Summary.PlannedIdentities, fallbackIdentity) {
			telemetry.Summary.PlannedIdentities = append(telemetry.Summary.PlannedIdentities, fallbackIdentity)
		}
		telemetry.Summary.RuntimeIdentity = fallbackIdentity
		telemetry.Summary.AppliedIdentity = fallbackIdentity
		telemetry.Summary.FallbackIdentity = fallbackIdentity
		telemetry.Summary.Provenance["fallback_identity"] = postgresBidirectionalAllShortestDiagnosticSource + ".fallback_executed"
	} else {
		identity := bidirectionalTelemetryIdentity(telemetry.Summary)
		telemetry.Summary.RuntimeIdentity = identity
		telemetry.Summary.AppliedIdentity = identity
		telemetry.Summary.FallbackIdentity = ""
	}
	telemetry.Summary.RuntimeOutcomeAvailable = traversalTelemetryPointer(true)
	telemetry.Summary.RuntimeBranch = document.RuntimeBranch
	telemetry.Summary.Overflow = traversalTelemetryPointer(*document.Overflowed)
	telemetry.Summary.FallbackExecuted = traversalTelemetryPointer(*document.FallbackExecuted)
	for _, name := range []string{"runtime_identity", "applied_identity", "runtime_branch", "overflow", "fallback_executed", "scheduler_version", "runtime_outcome_available"} {
		telemetry.Summary.Provenance[name] = postgresBidirectionalAllShortestDiagnosticSource
	}

	if telemetry.Diagnostic == nil {
		return nil
	}
	search := shortestPathCountersFromAllShortest(document.Counters, document.FallbackExecuted)
	telemetry.Diagnostic.RequiredFamilies = traversalRequiredFamilies(telemetry.Summary, TraversalTelemetryFamilyASP)
	telemetry.Diagnostic.Counters = TraversalDiagnosticCounters{AllShortestPaths: &AllShortestPathsTraversalCounters{
		Search:                        search,
		SameDepthPredecessorAdditions: document.Counters.SameDepthPredecessorAdditions,
		PredecessorPeak:               document.Counters.PredecessorPeak,
		MeetingNodes:                  document.Counters.MeetingNodes,
		CutDepth:                      document.Counters.CutDepth,
		PathCountEstimate:             document.Counters.PathCountEstimate,
		PathCountSaturated:            document.Counters.PathCountSaturated,
		EnumeratedCandidates:          document.Counters.EnumeratedCandidates,
		DuplicateRejects:              document.Counters.DuplicateRejects,
		OutputPaths:                   document.Counters.OutputPaths,
		OutputEdgeCells:               document.Counters.OutputEdgeCells,
		OutputBytes:                   document.Counters.OutputBytes,
	}}
	telemetry.Diagnostic.Counters.Workspace = &TraversalWorkspaceCounters{
		SessionPeakBytes: traversalTelemetryPointer(document.WorkspaceBytes),
		PoolPeakBytes:    traversalTelemetryPointer(document.WorkspaceBytes),
	}
	telemetry.Diagnostic.Provenance = map[string]string{}
	for _, name := range []string{
		"scheduler_actions", "candidate_edges", "distinct_new_nodes", "seen_peak", "frontier_peak", "queue_peak",
		"predecessor_peak", "meeting_candidates", "frozen_distance", "witness_rows", "fallback_executed",
	} {
		telemetry.Diagnostic.Provenance["all_shortest_paths.search."+name] = postgresBidirectionalAllShortestDiagnosticSource + ".counters." + name
	}
	telemetry.Diagnostic.Provenance["workspace.session_peak_bytes"] = "pg_total_relation_size(pg_temp.asb_*)"
	telemetry.Diagnostic.Provenance["workspace.pool_peak_bytes"] = "single_connection_diagnostic_pool.session_peak_bytes"
	for _, name := range []string{
		"same_depth_predecessor_additions", "predecessor_peak", "meeting_nodes", "cut_depth", "path_count_estimate",
		"path_count_saturated", "enumerated_candidates", "duplicate_rejects", "output_paths", "output_edge_cells", "output_bytes",
	} {
		telemetry.Diagnostic.Provenance["all_shortest_paths."+name] = postgresBidirectionalAllShortestDiagnosticSource + ".counters." + name
	}
	telemetry.Diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
	telemetry.Diagnostic.IncompleteReasons = []string{
		"complete invocation-local path hydration counters are unavailable",
	}
	if *document.FallbackExecuted {
		telemetry.Diagnostic.IncompleteReasons = append(telemetry.Diagnostic.IncompleteReasons, "nested exact ASP-A1 fallback traversal work counters are unavailable")
	}
	return nil
}

// shortestPathCountersFromAllShortest supports benchmark evidence processing for shortest path counters from all shortest.
func shortestPathCountersFromAllShortest(counters *postgresBidirectionalAllShortestDiagnosticCounts, fallbackExecuted *bool) ShortestPathTraversalCounters {
	levels := make([]ShortestPathLevelCounters, len(counters.Levels))
	for idx, level := range counters.Levels {
		levels[idx] = ShortestPathLevelCounters{
			SearchID:          *level.SearchID,
			ActionIndex:       *level.ActionIndex,
			Side:              level.Side,
			Action:            level.Action,
			Depth:             level.Depth,
			FrontierRows:      level.FrontierRows,
			CandidateEdges:    level.CandidateEdges,
			DistinctNewNodes:  level.DistinctNewNodes,
			SeenRows:          level.SeenRows,
			QueueRows:         level.QueueRows,
			PredecessorRows:   level.PredecessorRows,
			MeetingCandidates: level.MeetingCandidates,
			Provenance:        fmt.Sprintf("%s.counters.levels[%d]", postgresBidirectionalAllShortestDiagnosticSource, idx),
		}
	}
	return ShortestPathTraversalCounters{
		SchedulerActions:  counters.SchedulerActions,
		Levels:            levels,
		CandidateEdges:    counters.CandidateEdges,
		DistinctNewNodes:  counters.DistinctNewNodes,
		SeenPeak:          counters.SeenPeak,
		FrontierPeak:      counters.FrontierPeak,
		QueuePeak:         counters.QueuePeak,
		PredecessorPeak:   counters.PredecessorPeak,
		MeetingCandidates: counters.MeetingCandidates,
		FrozenDistance:    counters.FrozenDistance,
		WitnessRows:       counters.WitnessRows,
		FallbackExecuted:  fallbackExecuted,
	}
}

// validateBidirectionalAllShortestDiagnosticCounts validates bidirectional all shortest diagnostic counts.
func validateBidirectionalAllShortestDiagnosticCounts(counters *postgresBidirectionalAllShortestDiagnosticCounts) error {
	if counters == nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic counters are missing")
	}
	if err := validateBidirectionalDiagnosticCounts(&postgresBidirectionalDiagnosticCounts{
		SchedulerActions:  counters.SchedulerActions,
		CandidateEdges:    counters.CandidateEdges,
		DistinctNewNodes:  counters.DistinctNewNodes,
		SeenPeak:          counters.SeenPeak,
		FrontierPeak:      counters.FrontierPeak,
		QueuePeak:         counters.QueuePeak,
		PredecessorPeak:   counters.PredecessorPeak,
		MeetingCandidates: counters.MeetingCandidates,
		FrozenDistance:    counters.FrozenDistance,
		WitnessRows:       counters.WitnessRows,
		Levels:            counters.Levels,
	}); err != nil {
		return err
	}
	for name, value := range map[string]*int64{
		"same_depth_predecessor_additions": counters.SameDepthPredecessorAdditions,
		"meeting_nodes":                    counters.MeetingNodes, "path_count_estimate": counters.PathCountEstimate,
		"enumerated_candidates": counters.EnumeratedCandidates, "duplicate_rejects": counters.DuplicateRejects,
		"output_paths": counters.OutputPaths, "output_edge_cells": counters.OutputEdgeCells, "output_bytes": counters.OutputBytes,
	} {
		if value == nil || *value < 0 {
			return fmt.Errorf("bidirectional all-shortest diagnostic counter %s is missing or negative", name)
		}
	}
	if counters.CutDepth == nil || *counters.CutDepth < -1 {
		return fmt.Errorf("bidirectional all-shortest diagnostic cut_depth is missing or invalid")
	}
	if counters.PathCountSaturated == nil {
		return fmt.Errorf("bidirectional all-shortest diagnostic path_count_saturated is missing")
	}
	return nil
}

// validateBidirectionalAllShortestDiagnosticCalls validates bidirectional all shortest diagnostic calls.
func validateBidirectionalAllShortestDiagnosticCalls(calls []postgresBidirectionalAllShortestDiagnosticCall, overflowed, fallbackExecuted *bool) error {
	baseCalls := make([]postgresBidirectionalDiagnosticCall, len(calls))
	for idx, call := range calls {
		baseCalls[idx] = postgresBidirectionalDiagnosticCall{
			SearchID:          call.SearchID,
			SourceID:          call.SourceID,
			TargetID:          call.TargetID,
			RuntimeBranch:     call.RuntimeBranch,
			SchedulerActions:  call.SchedulerActions,
			CandidateEdges:    call.CandidateEdges,
			DistinctNewNodes:  call.DistinctNewNodes,
			SeenPeak:          call.SeenPeak,
			FrontierPeak:      call.FrontierPeak,
			QueuePeak:         call.QueuePeak,
			PredecessorPeak:   call.PredecessorPeak,
			MeetingCandidates: call.MeetingCandidates,
			FrozenDistance:    call.FrozenDistance,
			WitnessRows:       call.WitnessRows,
			Overflowed:        call.Overflowed,
			FallbackExecuted:  call.FallbackExecuted,
		}
	}
	if err := validateBidirectionalDiagnosticCallsFor(baseCalls, overflowed, fallbackExecuted, "exact_a1_fallback"); err != nil {
		return err
	}
	for idx, call := range calls {
		for name, value := range map[string]*int64{
			"same_depth_predecessor_additions": call.SameDepthPredecessorAdditions,
			"meeting_nodes":                    call.MeetingNodes, "path_count_estimate": call.PathCountEstimate,
			"enumerated_candidates": call.EnumeratedCandidates, "duplicate_rejects": call.DuplicateRejects,
			"output_paths": call.OutputPaths, "output_edge_cells": call.OutputEdgeCells, "output_bytes": call.OutputBytes,
		} {
			if value == nil || *value < 0 {
				return fmt.Errorf("bidirectional all-shortest diagnostic call %d counter %s is missing or negative", idx, name)
			}
		}
		if call.CutDepth == nil || *call.CutDepth < -1 || call.PathCountSaturated == nil {
			return fmt.Errorf("bidirectional all-shortest diagnostic call %d has incomplete cut/count state", idx)
		}
	}
	return nil
}

// validateBidirectionalAllShortestSingleCallAggregate validates bidirectional all shortest single call aggregate.
func validateBidirectionalAllShortestSingleCallAggregate(counters *postgresBidirectionalAllShortestDiagnosticCounts, call postgresBidirectionalAllShortestDiagnosticCall) error {
	if err := validateBidirectionalSingleCallAggregate(&postgresBidirectionalDiagnosticCounts{
		SchedulerActions:  counters.SchedulerActions,
		CandidateEdges:    counters.CandidateEdges,
		DistinctNewNodes:  counters.DistinctNewNodes,
		SeenPeak:          counters.SeenPeak,
		FrontierPeak:      counters.FrontierPeak,
		QueuePeak:         counters.QueuePeak,
		PredecessorPeak:   counters.PredecessorPeak,
		MeetingCandidates: counters.MeetingCandidates,
		FrozenDistance:    counters.FrozenDistance,
		WitnessRows:       counters.WitnessRows,
		Levels:            counters.Levels,
	}, postgresBidirectionalDiagnosticCall{
		SearchID:          call.SearchID,
		SchedulerActions:  call.SchedulerActions,
		CandidateEdges:    call.CandidateEdges,
		DistinctNewNodes:  call.DistinctNewNodes,
		SeenPeak:          call.SeenPeak,
		FrontierPeak:      call.FrontierPeak,
		QueuePeak:         call.QueuePeak,
		PredecessorPeak:   call.PredecessorPeak,
		MeetingCandidates: call.MeetingCandidates,
		FrozenDistance:    call.FrozenDistance,
		WitnessRows:       call.WitnessRows,
	}); err != nil {
		return err
	}
	for name, values := range map[string][2]*int64{
		"same_depth_predecessor_additions": {counters.SameDepthPredecessorAdditions, call.SameDepthPredecessorAdditions},
		"meeting_nodes":                    {counters.MeetingNodes, call.MeetingNodes}, "cut_depth": {counters.CutDepth, call.CutDepth},
		"path_count_estimate":   {counters.PathCountEstimate, call.PathCountEstimate},
		"enumerated_candidates": {counters.EnumeratedCandidates, call.EnumeratedCandidates},
		"duplicate_rejects":     {counters.DuplicateRejects, call.DuplicateRejects},
		"output_paths":          {counters.OutputPaths, call.OutputPaths}, "output_edge_cells": {counters.OutputEdgeCells, call.OutputEdgeCells},
		"output_bytes": {counters.OutputBytes, call.OutputBytes},
	} {
		if values[0] == nil || values[1] == nil || *values[0] != *values[1] {
			return fmt.Errorf("bidirectional all-shortest diagnostic aggregate counter %s differs from its single call", name)
		}
	}
	if counters.PathCountSaturated == nil || call.PathCountSaturated == nil || *counters.PathCountSaturated != *call.PathCountSaturated {
		return fmt.Errorf("bidirectional all-shortest diagnostic aggregate path_count_saturated differs from its single call")
	}
	return nil
}

// markTraversalCountersUnavailable supports benchmark evidence processing for mark traversal counters unavailable.
func markTraversalCountersUnavailable(diagnostic *TraversalExecutionDiagnostic, reason string) {
	if diagnostic == nil {
		return
	}
	diagnostic.CounterStatus = TraversalTelemetryCounterStatusHiddenUnavailable
	diagnostic.IncompleteReasons = []string{reason}
	diagnostic.Counters = TraversalDiagnosticCounters{}
	diagnostic.Provenance = map[string]string{}
}

// markTraversalSummaryUnavailable supports benchmark evidence processing for mark traversal summary unavailable.
func markTraversalSummaryUnavailable(telemetry *TraversalExecutionTelemetry, reason string) {
	if telemetry == nil {
		return
	}
	telemetry.Summary.RuntimeOutcomeAvailable = traversalTelemetryPointer(false)
	telemetry.Summary.RuntimeIdentity = ""
	telemetry.Summary.AppliedIdentity = ""
	telemetry.Summary.RuntimeBranch = "runtime_outcome_unavailable"
	telemetry.Summary.Overflow = nil
	telemetry.Summary.FallbackExecuted = nil
	telemetry.Summary.FallbackIdentity = ""
	for _, name := range []string{"runtime_identity", "applied_identity", "runtime_branch", "runtime_outcome_available"} {
		telemetry.Summary.Provenance[name] = "runtime_outcome_unavailable:" + reason
	}
	delete(telemetry.Summary.Provenance, "overflow")
	delete(telemetry.Summary.Provenance, "fallback_executed")
	delete(telemetry.Summary.Provenance, "fallback_identity")
}

// isBidirectionalSPIdentity reports whether is bidirectional sp identity.
func isBidirectionalSPIdentity(identity string) bool {
	return strings.HasPrefix(identity, "SP-B1-") || strings.HasPrefix(identity, "SP-B2-")
}

// bidirectionalFallbackIdentity derives the stable identity used to compare bidirectional fallback.
func bidirectionalFallbackIdentity(identity string) string {
	if isBidirectionalASPIdentity(identity) {
		return "ASP-A1-DAG"
	}
	if isBidirectionalSPIdentity(identity) {
		if strings.Contains(identity, "WE+") {
			return "SP-S4-C-WE+MAT-M0"
		}
		return "SP-S4-C-D"
	}
	return ""
}

// isBidirectionalASPIdentity reports whether is bidirectional asp identity.
func isBidirectionalASPIdentity(identity string) bool {
	return strings.HasPrefix(identity, "ASP-B1-") || strings.HasPrefix(identity, "ASP-B2-")
}

// traversalTelemetryPointer returns an addressable representation of traversal telemetry.
func traversalTelemetryPointer[T any](value T) *T {
	return &value
}
