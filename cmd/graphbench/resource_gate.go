// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
)

// resourceGateVersion identifies the serialized schema revision for resource gate.
const resourceGateVersion = 3

// ResourceGateReport reports whether production and reference plan resources remain within their allowed envelopes.
type ResourceGateReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Cases contains resource-envelope decisions for each evaluated production or reference executor.
	Cases []ResourceGateCase `json:"cases"`
}

// ResourceGateCase attributes resource-gate failures to one production or reference executor architecture.
type ResourceGateCase struct {
	// Dataset identifies the fixture dataset.
	Dataset string `json:"dataset"`
	// Name identifies the case or record within its dataset.
	Name string `json:"name"`
	// Reference identifies the reference arm evaluated by the resource gate.
	Reference string `json:"reference,omitempty"`
	// Tier identifies the resource envelope applied to the case.
	Tier string `json:"tier"`
	// QualificationSplit identifies training, frozen holdout, or diagnostic evidence.
	QualificationSplit string `json:"qualification_split"`
	// Architecture identifies the executor architecture.
	Architecture string `json:"architecture,omitempty"`
	// FallbackArchitecture identifies the executor architecture used after fallback.
	FallbackArchitecture string `json:"fallback_architecture,omitempty"`
	// Passed reports whether every required gate condition succeeded.
	Passed bool `json:"passed"`
	// Reasons lists explanations for the reported disposition.
	Reasons []string `json:"reasons,omitempty"`
	// NumericLimits records declared telemetry ceilings applied by this gate.
	NumericLimits map[string]int64 `json:"numeric_limits,omitempty"`
	// NumericObserved records invocation-local high-water marks compared with the limits.
	NumericObserved map[string]int64 `json:"numeric_observed,omitempty"`
	// RuntimeReceiptChains preserves complete measured branch chains alongside
	// the resource decision.
	RuntimeReceiptChains [][]RuntimeReceiptEvent `json:"runtime_receipt_chains,omitempty"`
}

// createResourceGateReport evaluates production and reference plan metrics against resource ceilings and writes the report.
func createResourceGateReport(artifact, output string) (bool, error) {
	records, err := readJSONLFile(artifact)
	if err != nil {
		return false, err
	}
	report := ResourceGateReport{
		Version: resourceGateVersion,
		Passed:  true,
	}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL {
			continue
		}
		gateCase := ResourceGateCase{
			Dataset:              record.Dataset,
			Name:                 record.Name,
			Tier:                 record.Shape.FixtureTier,
			QualificationSplit:   record.Shape.QualificationSplit,
			Passed:               true,
			RuntimeReceiptChains: runtimeReceiptChains(record.Stats.Samples),
		}
		if gateCase.Tier == "" {
			gateCase.Tier = "legacy"
		}
		if gateCase.QualificationSplit == "" {
			gateCase.QualificationSplit = "legacy"
		}
		gateCase.Architecture = appliedPostgresArchitecture(record)
		portableCandidate := gateCase.Architecture != "" && gateCase.Architecture != "SP-S0"
		workspaceCandidate := compactWorkspaceArchitecture(gateCase.Architecture)
		if gateCase.Architecture == "SP-S0-DIRECT" {
			if loops, found, err := postgresPlanFunctionLoops(record.PostgresPlanJSON, "bidirectional_sp_harness"); err != nil {
				gateCase.Reasons = append(gateCase.Reasons, "direct preflight fallback attribution failed: "+err.Error())
			} else if !found {
				gateCase.Reasons = append(gateCase.Reasons, "direct preflight fallback plan node is missing")
			} else if loops > 0 {
				portableCandidate = false
				gateCase.FallbackArchitecture = "SP-S0"
			}
		}
		if record.Status != StatusOK {
			gateCase.Reasons = append(gateCase.Reasons, "record status is "+record.Status)
		}
		if record.PostgresMetrics == nil {
			gateCase.Reasons = append(gateCase.Reasons, "structured PostgreSQL plan metrics are missing")
		} else if workspaceCandidate {
			appendWorkspaceResourceReasons(&gateCase, record.PostgresMetrics)
		} else if portableCandidate {
			appendPortableResourceReasons(&gateCase, record.PostgresMetrics)
		}
		telemetryRequired := telemetryRequiredForRecord(record, gateCase.Architecture)
		appendTelemetryResourceReasons(&gateCase, record.TraversalTelemetry, telemetryRequired)
		appendFallbackExpectationReasons(&gateCase, record)
		appendWorkspaceCeilingReasons(&gateCase, record.Environment, record.TraversalTelemetry, workspaceCandidate, compactBidirectionalWorkspaceArchitecture(gateCase.Architecture))
		gateCase.Passed = len(gateCase.Reasons) == 0
		if !gateCase.Passed {
			report.Passed = false
		}
		report.Cases = append(report.Cases, gateCase)
		for _, reference := range record.PostgresReferences {
			if !reference.FullComparator || reference.Architecture == "" {
				continue
			}
			referenceCase := ResourceGateCase{
				Dataset:            record.Dataset,
				Name:               record.Name,
				Reference:          reference.Name,
				Tier:               gateCase.Tier,
				QualificationSplit: gateCase.QualificationSplit,
				Architecture:       reference.Architecture,
				Passed:             true,
			}
			if reference.PostgresMetrics == nil {
				referenceCase.Reasons = append(referenceCase.Reasons, "structured PostgreSQL reference plan metrics are missing")
			} else if compactBidirectionalWorkspaceArchitecture(reference.Architecture) {
				appendWorkspaceResourceReasons(&referenceCase, reference.PostgresMetrics)
			} else if reference.Architecture != "SP-S0" {
				appendPortableResourceReasons(&referenceCase, reference.PostgresMetrics)
			}
			appendTelemetryResourceReasons(&referenceCase, reference.TraversalTelemetry, telemetryRequiredForArchitecture(reference.Architecture))
			appendWorkspaceCeilingReasons(&referenceCase, record.Environment, reference.TraversalTelemetry, compactBidirectionalWorkspaceArchitecture(reference.Architecture), compactBidirectionalWorkspaceArchitecture(reference.Architecture))
			referenceCase.Passed = len(referenceCase.Reasons) == 0
			if !referenceCase.Passed {
				report.Passed = false
			}
			report.Cases = append(report.Cases, referenceCase)
		}
	}
	if len(report.Cases) == 0 {
		return false, fmt.Errorf("resource artifact contains no PostgreSQL cases")
	}
	sort.Slice(report.Cases, func(i, j int) bool {
		if report.Cases[i].Dataset != report.Cases[j].Dataset {
			return report.Cases[i].Dataset < report.Cases[j].Dataset
		}
		if report.Cases[i].Name != report.Cases[j].Name {
			return report.Cases[i].Name < report.Cases[j].Name
		}
		return report.Cases[i].Reference < report.Cases[j].Reference
	})

	var raw []byte
	if raw, err = json.MarshalIndent(report, "", "  "); err != nil {
		return false, err
	}
	if output == "" {
		_, err = os.Stdout.Write(append(raw, '\n'))
	} else {
		err = os.WriteFile(output, append(raw, '\n'), 0o644)
	}
	if err != nil {
		return false, err
	}

	return report.Passed, nil
}

// compactWorkspaceArchitecture reports whether an executor deliberately uses
// bounded session-local typed workspace rather than portable recursive state.
func compactWorkspaceArchitecture(architecture string) bool {
	switch architecture {
	case "ASP-A1-DAG",
		"ASP-B1-DAG-ALT-NODE",
		"ASP-B2-DAG-MIN-LEVEL",
		"SP-S4-C-D",
		"SP-S4-C-WE+MAT-M0",
		"SP-B1-C-ALT-NODE-D",
		"SP-B1-C-ALT-NODE-WE+MAT-M0",
		"SP-B2-C-MIN-LEVEL-D",
		"SP-B2-C-MIN-LEVEL-WE+MAT-M0":
		return true
	default:
		return false
	}
}

// compactBidirectionalWorkspaceArchitecture identifies reference arms whose
// measured boundary deliberately includes the reusable spb_* workspace.
func compactBidirectionalWorkspaceArchitecture(architecture string) bool {
	switch architecture {
	case "SP-B1-C-ALT-NODE-D",
		"SP-B1-C-ALT-NODE-WE+MAT-M0",
		"SP-B2-C-MIN-LEVEL-D",
		"SP-B2-C-MIN-LEVEL-WE+MAT-M0",
		"ASP-B1-DAG-ALT-NODE",
		"ASP-B2-DAG-MIN-LEVEL":
		return true
	default:
		return false
	}
}

// telemetryRequiredForArchitecture identifies candidates whose qualification
// depends on executor-visible work rather than outer EXPLAIN counters.
func telemetryRequiredForArchitecture(architecture string) bool {
	return strings.HasPrefix(architecture, "SP-B1-") ||
		strings.HasPrefix(architecture, "SP-B2-") ||
		strings.HasPrefix(architecture, "ASP-B1-") ||
		strings.HasPrefix(architecture, "ASP-B2-") ||
		architecture == "orientation-probe-v1"
}

func telemetryRequiredForRecord(record CaseResult, architecture string) bool {
	if telemetryRequiredForArchitecture(architecture) {
		return true
	}
	if record.Optimization != nil {
		for _, outcome := range record.Optimization.TargetOutcomes {
			if outcome.EmittedPolicy == "orientation-probe-v1" {
				return true
			}
		}
	}
	return record.TraversalTelemetry != nil &&
		(record.TraversalTelemetry.Summary.EmittedIdentity == "orientation-probe-v1" ||
			record.TraversalTelemetry.Summary.SelectorVersion == "orientation-probe-v1")
}

func appendFallbackExpectationReasons(gateCase *ResourceGateCase, record CaseResult) {
	expectation := record.Shape.FallbackExpectation
	if expectation == "" {
		if telemetryRequiredForRecord(record, "") {
			gateCase.Reasons = append(gateCase.Reasons, "candidate resource qualification requires a typed fallback expectation")
		}
		return
	}
	if record.TraversalTelemetry == nil {
		gateCase.Reasons = append(gateCase.Reasons, "fallback expectation lacks runtime telemetry")
		return
	}
	summary := record.TraversalTelemetry.Summary
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable || summary.FallbackExecuted == nil {
		gateCase.Reasons = append(gateCase.Reasons, "fallback runtime outcome is unavailable")
		return
	}
	switch expectation {
	case "required":
		if !*summary.FallbackExecuted {
			gateCase.Reasons = append(gateCase.Reasons, "declared overflow-fallback expectation did not execute its exact fallback")
		}
	case "forbidden":
		if *summary.FallbackExecuted {
			gateCase.Reasons = append(gateCase.Reasons, "normal/envelope candidate unexpectedly executed fallback")
		}
	case "allowed":
	default:
		gateCase.Reasons = append(gateCase.Reasons, "unknown fallback expectation "+expectation)
	}
}

func appendWorkspaceCeilingReasons(gateCase *ResourceGateCase, environment *RunEnvironment, telemetry *TraversalExecutionTelemetry, workspaceArchitecture, ceilingsRequired bool) {
	if !workspaceArchitecture {
		return
	}
	if environment == nil || environment.SessionMemoryCeilingBytes <= 0 || environment.PoolMemoryCeilingBytes <= 0 {
		if ceilingsRequired {
			gateCase.Reasons = append(gateCase.Reasons, "workspace candidate requires positive declared session and pool memory ceilings")
		}
		return
	}
	if telemetry == nil || telemetry.Diagnostic == nil || telemetry.Diagnostic.Counters.Workspace == nil {
		gateCase.Reasons = append(gateCase.Reasons, "declared workspace memory ceilings lack measured session and pool high-water evidence")
		return
	}
	if environment.PoolSize <= 0 {
		gateCase.Reasons = append(gateCase.Reasons, "workspace candidate requires a declared positive pool size")
		return
	}
	workspace := telemetry.Diagnostic.Counters.Workspace
	if workspace.SessionPeakBytes == nil {
		gateCase.Reasons = append(gateCase.Reasons, "declared session memory ceiling lacks a measured session high-water value")
	} else if *workspace.SessionPeakBytes > environment.SessionMemoryCeilingBytes {
		gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("session workspace peak %d exceeds declared ceiling %d", *workspace.SessionPeakBytes, environment.SessionMemoryCeilingBytes))
	}
	if workspace.PoolPeakBytes == nil {
		gateCase.Reasons = append(gateCase.Reasons, "declared pool memory ceiling lacks a measured pool high-water value")
	} else if *workspace.PoolPeakBytes > environment.PoolMemoryCeilingBytes {
		gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("pool workspace peak %d exceeds declared ceiling %d", *workspace.PoolPeakBytes, environment.PoolMemoryCeilingBytes))
	}
	if environment.PoolSize > 1 && telemetry != nil && telemetry.Diagnostic != nil &&
		telemetry.Diagnostic.Provenance["workspace.pool_peak_bytes"] == "single_connection_diagnostic_pool.session_peak_bytes" {
		gateCase.Reasons = append(gateCase.Reasons, "pool workspace ceiling lacks an aggregate multi-session high-water measurement")
	}
}

// appendTelemetryResourceReasons validates identity attribution and numeric
// cap evidence from a distinct untimed diagnostic invocation.
func appendTelemetryResourceReasons(gateCase *ResourceGateCase, telemetry *TraversalExecutionTelemetry, required bool) {
	if telemetry == nil {
		if required {
			gateCase.Reasons = append(gateCase.Reasons, "required traversal execution telemetry is missing")
		}
		return
	}
	if err := ValidateTraversalExecutionTelemetry(telemetry); err != nil {
		gateCase.Reasons = append(gateCase.Reasons, err.Error())
		return
	}

	summary := telemetry.Summary
	if summary.RuntimeOutcomeAvailable != nil && !*summary.RuntimeOutcomeAvailable {
		if required {
			gateCase.Reasons = append(gateCase.Reasons, "candidate qualification requires an observed runtime traversal outcome")
		}
		return
	}
	if !slices.Contains(summary.PlannedIdentities, summary.RuntimeIdentity) {
		gateCase.Reasons = append(gateCase.Reasons, "runtime traversal identity is not a planned candidate")
	}
	if summary.AppliedIdentity != summary.RuntimeIdentity {
		gateCase.Reasons = append(gateCase.Reasons, "applied traversal identity does not match runtime identity")
	}
	if summary.FallbackExecuted != nil && *summary.FallbackExecuted && summary.RuntimeIdentity != summary.FallbackIdentity {
		gateCase.Reasons = append(gateCase.Reasons, "fallback traversal identity does not match runtime identity")
	}
	if required && telemetry.Level != TraversalTelemetryLevelDiagnostic {
		gateCase.Reasons = append(gateCase.Reasons, "candidate qualification requires an untimed diagnostic replay")
		return
	}
	if telemetry.Diagnostic == nil {
		return
	}
	counterStatus := telemetry.Diagnostic.CounterStatus
	if counterStatus == "" {
		counterStatus = TraversalTelemetryCounterStatusComplete
	}
	if required && counterStatus != TraversalTelemetryCounterStatusComplete {
		gateCase.Reasons = append(gateCase.Reasons, "candidate qualification requires complete executor counters; diagnostic status is "+string(counterStatus))
		return
	}
	if required && (summary.EmittedIdentity == "orientation-probe-v1" || summary.SelectorVersion == "orientation-probe-v1") {
		requiredFamilies := []TraversalTelemetryFamily{TraversalTelemetryFamilyOrientation, TraversalTelemetryFamilyOrdinary}
		if observationRequiresHydration(summary.ObservationMode) {
			requiredFamilies = append(requiredFamilies, TraversalTelemetryFamilyHydration)
		}
		for _, family := range requiredFamilies {
			if !slices.Contains(telemetry.Diagnostic.RequiredFamilies, family) {
				gateCase.Reasons = append(gateCase.Reasons, "orientation qualification is missing required counter family "+string(family))
			}
		}
		appendOrientationAttributionReasons(gateCase, telemetry.Diagnostic)
	}
	if required && summary.EmittedIdentity == "asp-i1-guarded-v1" {
		appendInlineASPAttributionReasons(gateCase, telemetry.Diagnostic)
	}

	observed := traversalNumericObservations(telemetry.Diagnostic.Counters)
	gateCase.NumericLimits = make(map[string]int64, len(summary.Caps))
	gateCase.NumericObserved = map[string]int64{}
	for name, limit := range summary.Caps {
		gateCase.NumericLimits[name] = limit
		if limit < 0 {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("traversal cap %s is negative", name))
			continue
		}
		value, found := observed[name]
		if !found {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("required numeric traversal counter %s is missing", name))
			continue
		}
		gateCase.NumericObserved[name] = value
		allowed := limit
		if traversalCapUsesSentinel(name) {
			allowed++
		}
		if value > allowed {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("traversal counter %s=%d exceeds ceiling %d", name, value, allowed))
		}
	}
}

func appendInlineASPAttributionReasons(gateCase *ResourceGateCase, diagnostic *TraversalExecutionDiagnostic) {
	if diagnostic == nil || diagnostic.PlanReplay == nil {
		gateCase.Reasons = append(gateCase.Reasons, "inline ASP qualification requires exact plan branch evidence")
		return
	}
	counters := diagnostic.PlanReplay.Counters
	candidate, candidatePresent := counters["asp_i1_candidate_marker_rows"]
	fallback, fallbackPresent := counters["asp_i1_fallback_marker_rows"]
	if !candidatePresent || !fallbackPresent || candidate+fallback != 1 {
		gateCase.Reasons = append(gateCase.Reasons, "inline ASP execution must attribute exactly one candidate or fallback marker")
	}
	if candidate == 1 && counters["asp_i1_fallback_branch_rows"] != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "inline ASP fallback arm performed work while the candidate was selected")
	}
	if fallback == 1 && counters["asp_i1_candidate_branch_rows"] != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "inline ASP candidate output arm performed work while fallback was selected")
	}
}

func appendOrientationAttributionReasons(gateCase *ResourceGateCase, diagnostic *TraversalExecutionDiagnostic) {
	if diagnostic == nil || diagnostic.PlanReplay == nil {
		gateCase.Reasons = append(gateCase.Reasons, "orientation qualification requires exact plan branch and probe evidence")
		return
	}
	counters := diagnostic.PlanReplay.Counters
	candidate, candidatePresent := counters["orientation_executed_candidate_rows"]
	incumbent, incumbentPresent := counters["orientation_executed_incumbent_rows"]
	if !candidatePresent || !incumbentPresent {
		gateCase.Reasons = append(gateCase.Reasons, "orientation execution is missing exact selected and unselected arm markers")
	}
	if candidate+incumbent != 1 {
		gateCase.Reasons = append(gateCase.Reasons, "orientation execution must attribute exactly one selected arm and zero unselected-arm work")
	}
	for _, name := range []string{
		"orientation_root_probe_loops", "orientation_suffix_probe_loops", "orientation_boundary_probe_loops",
		"orientation_forward_degree_probe_loops", "orientation_reverse_degree_probe_loops", "orientation_decision_loops",
	} {
		loops, present := counters[name]
		if !present {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("orientation probe %s has no execution-count evidence", name))
		} else if loops > 1 {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("orientation probe %s executed more than once", name))
		}
	}
}

// traversalCapUsesSentinel reports whether the counter may observe the
// deliberate cap+1 row used to prove overflow before exact fallback.
func traversalCapUsesSentinel(name string) bool {
	return strings.Contains(name, "probe") || strings.Contains(name, "state") ||
		strings.Contains(name, "frontier") || strings.Contains(name, "queue") ||
		strings.Contains(name, "seen") || strings.Contains(name, "predecessor") ||
		strings.Contains(name, "output")
}

// traversalNumericObservations maps typed diagnostic counters to stable gate names.
func traversalNumericObservations(counters TraversalDiagnosticCounters) map[string]int64 {
	observed := map[string]int64{}
	set := func(name string, value *int64) {
		if value != nil {
			observed[name] = *value
		}
	}
	if ordinary := counters.Ordinary; ordinary != nil {
		set("root_rows", ordinary.Roots)
		set("edge_candidates", ordinary.EdgeCandidates)
		set("state_rows", ordinary.PeakState)
		set("output_paths", ordinary.EmittedTrails)
	}
	if orientation := counters.Orientation; orientation != nil {
		set("forward_seed_rows", orientation.ForwardSeeds)
		set("reverse_seed_rows", orientation.ReverseSeeds)
		set("probe_rows", orientation.ProbeRows)
		if orientation.ForwardDegreeSamples != nil && orientation.ReverseDegreeSamples != nil {
			degreePeak := max(*orientation.ForwardDegreeSamples, *orientation.ReverseDegreeSamples)
			observed["directional_degree_rows"] = degreePeak
		}
		set("survival_rows", orientation.ShallowSurvivalRows)
		set("branch_loops", orientation.BranchLoops)
	}
	if shortest := counters.ShortestPath; shortest != nil {
		set("state_rows", shortest.SeenPeak)
		set("frontier_rows", shortest.FrontierPeak)
		set("queue_rows", shortest.QueuePeak)
		set("seen_rows", shortest.SeenPeak)
		set("predecessor_rows", shortest.PredecessorPeak)
		set("meeting_rows", shortest.MeetingCandidates)
		set("witness_rows", shortest.WitnessRows)
	}
	if all := counters.AllShortestPaths; all != nil {
		set("state_rows", all.Search.SeenPeak)
		set("frontier_rows", all.Search.FrontierPeak)
		set("queue_rows", all.Search.QueuePeak)
		set("seen_rows", all.Search.SeenPeak)
		set("predecessor_rows", all.PredecessorPeak)
		set("output_paths", all.OutputPaths)
		set("output_rows", all.EnumeratedCandidates)
		set("output_edge_cells", all.OutputEdgeCells)
		set("output_bytes", all.OutputBytes)
	}
	if inline := counters.InlineASP; inline != nil {
		set("state_rows", inline.DistanceRows)
		set("predecessor_rows", inline.PredecessorRows)
		set("output_rows", inline.EnumerationRows)
		set("output_paths", inline.OutputPaths)
		set("output_bytes", inline.OutputBytes)
	}
	if hydration := counters.Hydration; hydration != nil {
		set("hydration_rows", hydration.Rows)
		set("hydration_bytes", hydration.Bytes)
	}
	return observed
}

// appendWorkspaceResourceReasons adds failures for excessive executor or session workspace usage.
func appendWorkspaceResourceReasons(gateCase *ResourceGateCase, metrics *PostgresPlanMetrics) {
	if metrics.Buffers.TempRead != 0 || metrics.Buffers.TempWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "compact workspace candidate spilled to executor temporary storage")
	}
	if metrics.WALRecords != 0 || metrics.WALBytes != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "non-mutating compact workspace candidate emitted WAL")
	}
}

// appendPortableResourceReasons adds failures for spill, loops, or cardinality evidence that violates portable limits.
func appendPortableResourceReasons(gateCase *ResourceGateCase, metrics *PostgresPlanMetrics) {
	buffers := metrics.Buffers
	if buffers.TempRead != 0 || buffers.TempWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "portable candidate used temporary buffers")
	}
	if buffers.LocalHit != 0 || buffers.LocalRead != 0 || buffers.LocalDirtied != 0 || buffers.LocalWritten != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "portable candidate used local workspace")
	}
	if metrics.WALRecords != 0 || metrics.WALBytes != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "non-mutating portable candidate emitted WAL")
	}
}

// postgresPlanFunctionLoops sums actual loops for PostgreSQL plan nodes invoking the named function.
func postgresPlanFunctionLoops(raw json.RawMessage, function string) (int64, bool, error) {
	if len(raw) == 0 {
		return 0, false, nil
	}
	var document []map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return 0, false, err
	}
	if len(document) == 0 {
		return 0, false, nil
	}
	root, ok := document[0]["Plan"].(map[string]any)
	if !ok {
		return 0, false, nil
	}
	var loops int64
	found := false
	var walk func(map[string]any)
	walk = func(node map[string]any) {
		alias, _ := node["Alias"].(string)
		functionName, _ := node["Function Name"].(string)
		if alias == function || functionName == function {
			found = true
			if actualLoops, ok := node["Actual Loops"].(float64); ok {
				loops += int64(actualLoops)
			}
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				walk(childNode)
			}
		}
	}
	walk(root)
	return loops, found, nil
}

// appliedPostgresArchitecture returns the effective PostgreSQL executor architecture, including fallback attribution.
func appliedPostgresArchitecture(record CaseResult) string {
	if record.Optimization == nil {
		return ""
	}
	for _, outcome := range record.Optimization.TargetOutcomes {
		if outcome.Family == "SP" || outcome.Family == "ASP" || outcome.Family == "fixed_suffix_expansion" || outcome.Family == "fixed_prefix_terminal_expansion" {
			if outcome.Applied != "" {
				return outcome.Applied
			}
			return outcome.Selected
		}
	}
	return ""
}
