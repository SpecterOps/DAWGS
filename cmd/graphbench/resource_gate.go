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

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

// resourceGateVersion identifies the serialized schema revision for resource gate.
const resourceGateVersion = 5

// ResourceGateReport reports whether production and reference plan resources remain within their allowed envelopes.
type ResourceGateReport struct {
	// Version identifies the serialized schema revision.
	Version int `json:"version"`
	// ArtifactSHA256 binds this report to the exact input JSONL artifact.
	ArtifactSHA256 string `json:"artifact_sha256"`
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
	// Round identifies the measured record that produced this decision.
	Round int `json:"round,omitempty"`
	// Block identifies the paired measurement block for this record.
	Block int `json:"block,omitempty"`
	// RunUUID binds the resource decision to one run series.
	RunUUID string `json:"run_uuid,omitempty"`
	// Arm identifies the measured executor arm.
	Arm string `json:"arm,omitempty"`
	// ArmOrder supplies the arm order input to the ResourceGateCase contract.
	ArmOrder int `json:"arm_order,omitempty"`
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
	artifactSHA256, err := fileSHA256(artifact)
	if err != nil {
		return false, err
	}
	report := ResourceGateReport{
		Version:        resourceGateVersion,
		ArtifactSHA256: artifactSHA256,
		Passed:         true,
	}
	for _, record := range records {
		if record.ExecutionMode != ModePostgresSQL {
			continue
		}
		gateCase := evaluateProductionResourceGateCase(record)
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
				Round:              gateCase.Round,
				Block:              gateCase.Block,
				RunUUID:            gateCase.RunUUID,
				Arm:                gateCase.Arm,
				ArmOrder:           gateCase.ArmOrder,
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
		if report.Cases[i].Round != report.Cases[j].Round {
			return report.Cases[i].Round < report.Cases[j].Round
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

// evaluateProductionResourceGateCase derives the complete production decision
// from one artifact record. Qualification reuses this exact evaluator so a
// serialized report cannot suppress spill, WAL, attribution, fallback, or cap
// failures while retaining the candidate artifact digest.
func evaluateProductionResourceGateCase(record CaseResult) ResourceGateCase {
	gateCase := ResourceGateCase{
		Dataset:              record.Dataset,
		Name:                 record.Name,
		Tier:                 record.Shape.FixtureTier,
		QualificationSplit:   record.Shape.QualificationSplit,
		Passed:               true,
		RuntimeReceiptChains: runtimeReceiptChains(record.Stats.Samples),
	}
	if record.Environment != nil {
		gateCase.Round = record.Environment.Round
		gateCase.Block = record.Environment.Block
		gateCase.RunUUID = record.Environment.RunUUID
		gateCase.Arm = record.Environment.Arm
		gateCase.ArmOrder = record.Environment.ArmOrder
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
	if contract, guarded := guardedInlineResourceContractForArchitecture(gateCase.Architecture); guarded {
		appendGuardedInlineResourceBindingReasons(&gateCase, record, contract)
	}
	telemetryRequired := telemetryRequiredForRecord(record, gateCase.Architecture)
	appendTelemetryResourceReasons(&gateCase, record.TraversalTelemetry, telemetryRequired)
	appendFallbackExpectationReasons(&gateCase, record)
	appendWorkspaceCeilingReasons(&gateCase, record.Environment, record.TraversalTelemetry, workspaceCandidate, compactBidirectionalWorkspaceArchitecture(gateCase.Architecture))
	gateCase.Passed = len(gateCase.Reasons) == 0
	return gateCase
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
// depends on executor-visible work rather than outer EXPLAIN counters. This
// architecture-only check also applies to explicit reference arms, so guarded
// inline I1 production requirements deliberately belong to the record-aware
// check below instead.
func telemetryRequiredForArchitecture(architecture string) bool {
	return strings.HasPrefix(architecture, "SP-B1-") ||
		strings.HasPrefix(architecture, "SP-B2-") ||
		strings.HasPrefix(architecture, "ASP-B1-") ||
		strings.HasPrefix(architecture, "ASP-B2-") ||
		isOrientationProbePolicy(architecture) ||
		isSuffixReverseGuardPolicy(architecture)
}

// telemetryRequiredForRecord supports benchmark evidence processing for telemetry required for record.
func telemetryRequiredForRecord(record CaseResult, architecture string) bool {
	if _, guarded := guardedInlineResourceContractForArchitecture(architecture); guarded {
		return true
	}
	if telemetryRequiredForArchitecture(architecture) {
		return true
	}
	if record.Optimization != nil {
		for _, outcome := range record.Optimization.TargetOutcomes {
			if isOrientationProbePolicy(outcome.EmittedPolicy) || isSuffixReverseGuardPolicy(outcome.EmittedPolicy) || guardedInlineResourcePolicy(outcome.EmittedPolicy) {
				return true
			}
		}
	}
	return record.TraversalTelemetry != nil &&
		(isOrientationProbePolicy(record.TraversalTelemetry.Summary.EmittedIdentity) ||
			isOrientationProbePolicy(record.TraversalTelemetry.Summary.SelectorVersion) ||
			isSuffixReverseGuardPolicy(record.TraversalTelemetry.Summary.EmittedIdentity) ||
			isSuffixReverseGuardPolicy(record.TraversalTelemetry.Summary.SelectorVersion) ||
			guardedInlineResourcePolicy(record.TraversalTelemetry.Summary.EmittedIdentity))
}

// guardedInlineResourceContract groups state that must remain consistent while processing guarded inline resource contract.
type guardedInlineResourceContract struct {
	// architecture retains the architecture while guardedInlineResourceContract is assembled or evaluated.
	architecture string
	// family retains the family while guardedInlineResourceContract is assembled or evaluated.
	family string
	// telemetryFamily retains the telemetry family while guardedInlineResourceContract is assembled or evaluated.
	telemetryFamily TraversalTelemetryFamily
	// policy retains the policy while guardedInlineResourceContract is assembled or evaluated.
	policy string
	// namespace retains the namespace while guardedInlineResourceContract is assembled or evaluated.
	namespace string
	// label retains the label while guardedInlineResourceContract is assembled or evaluated.
	label string
}

// guardedInlineResourceContractForArchitecture supports benchmark evidence processing for guarded inline resource contract for architecture.
func guardedInlineResourceContractForArchitecture(architecture string) (guardedInlineResourceContract, bool) {
	switch architecture {
	case string(optimize.ShortestPathExecutorI1CanonicalPredecessorWitness):
		return guardedInlineResourceContract{
			architecture:    architecture,
			family:          "SP",
			telemetryFamily: TraversalTelemetryFamilySP,
			policy:          optimize.ShortestPathPolicyI1CanonicalGuardedV1,
			namespace:       "inline_shortest_path",
			label:           "inline canonical SP",
		}, true
	case string(optimize.ShortestPathExecutorASPI1DAG):
		return guardedInlineResourceContract{
			architecture:    architecture,
			family:          "ASP",
			telemetryFamily: TraversalTelemetryFamilyASP,
			policy:          optimize.ShortestPathPolicyASPI1GuardedV1,
			namespace:       "inline_asp",
			label:           "inline ASP",
		}, true
	case string(optimize.ShortestPathExecutorI2GuardedDistance):
		return guardedInlineResourceContract{architecture: architecture, family: "SP", telemetryFamily: TraversalTelemetryFamilySP, policy: optimize.ShortestPathPolicyI2DistanceGuardedV1, namespace: "inline_shortest_distance", label: "inline SP distance"}, true
	case string(optimize.ShortestPathExecutorI2GuardedDistanceV2),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E0),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1D),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1P),
		string(optimize.ShortestPathExecutorI2GuardedDistanceV2E1DP):
		return guardedInlineResourceContract{architecture: architecture, family: "SP", telemetryFamily: TraversalTelemetryFamilySP, policy: optimize.ShortestPathPolicyI2DistanceGuardedV2, namespace: "inline_shortest_distance", label: "inline SP distance V2"}, true
	default:
		return guardedInlineResourceContract{}, false
	}
}

// guardedInlineResourcePolicy supports benchmark evidence processing for guarded inline resource policy.
func guardedInlineResourcePolicy(policy string) bool {
	return policy == optimize.ShortestPathPolicyI1CanonicalGuardedV1 || policy == optimize.ShortestPathPolicyASPI1GuardedV1 || policy == optimize.ShortestPathPolicyI2DistanceGuardedV1 || policy == optimize.ShortestPathPolicyI2DistanceGuardedV2
}

// appendGuardedInlineResourceBindingReasons prevents an unguarded comparator
// with the same executor architecture from satisfying production resource
// evidence. Production I1 must bind the translated outcome and telemetry to
// its exact policy and to the observation-specific typed counter namespace.
func appendGuardedInlineResourceBindingReasons(gateCase *ResourceGateCase, record CaseResult, contract guardedInlineResourceContract) {
	emittedPolicy := ""
	outcomeFound := false
	if record.Optimization != nil {
		for _, outcome := range record.Optimization.TargetOutcomes {
			applied := outcome.Applied
			if applied == "" {
				applied = outcome.Selected
			}
			if outcome.Family == contract.family && applied == contract.architecture {
				emittedPolicy = outcome.EmittedPolicy
				outcomeFound = true
				break
			}
		}
	}
	if !outcomeFound || emittedPolicy != contract.policy {
		gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
			"%s production architecture requires emitted policy %q; found %q",
			contract.label, contract.policy, emittedPolicy,
		))
	}

	telemetry := record.TraversalTelemetry
	if telemetry == nil {
		return
	}
	if telemetry.Summary.EmittedIdentity != contract.policy {
		gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
			"%s production telemetry requires emitted identity %q; found %q",
			contract.label, contract.policy, telemetry.Summary.EmittedIdentity,
		))
	}
	if telemetry.Diagnostic == nil {
		return
	}
	if !slices.Contains(telemetry.Diagnostic.RequiredFamilies, contract.telemetryFamily) {
		gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf(
			"%s production telemetry requires declared counter family %q",
			contract.label, contract.telemetryFamily,
		))
	}
	if observationRequiresHydration(telemetry.Summary.ObservationMode) &&
		!slices.Contains(telemetry.Diagnostic.RequiredFamilies, TraversalTelemetryFamilyHydration) {
		gateCase.Reasons = append(gateCase.Reasons, contract.label+" production telemetry requires declared hydration counters for its observation mode")
	}

	inlineASP := telemetry.Diagnostic.Counters.InlineASP
	inlineShortestPath := telemetry.Diagnostic.Counters.InlineShortestPath
	inlineShortestDistance := telemetry.Diagnostic.Counters.InlineShortestDistance
	switch contract.namespace {
	case "inline_shortest_path":
		if inlineShortestPath == nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline canonical SP production telemetry requires inline_shortest_path counters")
		}
		if inlineASP != nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline canonical SP production telemetry must not use inline_asp counters")
		}
	case "inline_asp":
		if inlineASP == nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline ASP production telemetry requires inline_asp counters")
		}
		if inlineShortestPath != nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline ASP production telemetry must not use inline_shortest_path counters")
		}
	case "inline_shortest_distance":
		if inlineShortestDistance == nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance production telemetry requires inline_shortest_distance counters")
		} else if inlineShortestDistance.OutputRows != nil && *inlineShortestDistance.OutputRows != record.RowCount {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance typed output does not match the exact public observation")
		}
		if inlineASP != nil || inlineShortestPath != nil {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance production telemetry must not use predecessor counter namespaces")
		}
		if telemetry.Diagnostic.PlanReplay != nil {
			if outputRows, found := telemetry.Diagnostic.PlanReplay.Counters["sp_i2_output_rows"]; found && outputRows != record.RowCount {
				gateCase.Reasons = append(gateCase.Reasons, "inline SP distance plan output does not match the exact public observation")
			}
		}
	}
}

// appendFallbackExpectationReasons appends fallback expectation reasons.
func appendFallbackExpectationReasons(gateCase *ResourceGateCase, record CaseResult) {
	expectation := record.Shape.FallbackExpectation
	if expectation == "" {
		if telemetryRequiredForRecord(record, appliedPostgresArchitecture(record)) {
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

// appendWorkspaceCeilingReasons appends workspace ceiling reasons.
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
	if required && (isOrientationProbePolicy(summary.EmittedIdentity) || isOrientationProbePolicy(summary.SelectorVersion)) {
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
	if required && (isSuffixReverseGuardPolicy(summary.EmittedIdentity) || isSuffixReverseGuardPolicy(summary.SelectorVersion)) {
		requiredFamilies := []TraversalTelemetryFamily{TraversalTelemetryFamilySuffixGuard, TraversalTelemetryFamilyOrdinary}
		if observationRequiresHydration(summary.ObservationMode) {
			requiredFamilies = append(requiredFamilies, TraversalTelemetryFamilyHydration)
		}
		for _, family := range requiredFamilies {
			if !slices.Contains(telemetry.Diagnostic.RequiredFamilies, family) {
				gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard qualification is missing required counter family "+string(family))
			}
		}
		appendSuffixGuardAttributionReasons(gateCase, telemetry.Diagnostic)
	}
	if required && summary.EmittedIdentity == optimize.ShortestPathPolicyASPI1GuardedV1 {
		appendInlinePredecessorAttributionReasons(gateCase, telemetry.Diagnostic, "inline ASP")
	}
	if required && summary.EmittedIdentity == optimize.ShortestPathPolicyI1CanonicalGuardedV1 {
		appendInlinePredecessorAttributionReasons(gateCase, telemetry.Diagnostic, "inline canonical SP")
	}
	if required && (summary.EmittedIdentity == optimize.ShortestPathPolicyI2DistanceGuardedV1 ||
		summary.EmittedIdentity == optimize.ShortestPathPolicyI2DistanceGuardedV2) {
		appendInlineDistanceAttributionReasons(gateCase, telemetry)
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
		if value < 0 {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("traversal counter %s=%d is negative", name, value))
		} else if value > allowed {
			gateCase.Reasons = append(gateCase.Reasons, fmt.Sprintf("traversal counter %s=%d exceeds ceiling %d", name, value, allowed))
		}
	}
}

// appendInlineDistanceAttributionReasons binds the SP-I2 runtime receipt to
// exact named-plan counters. Qualification accepts one marker and one executor
// loop only, proves the inactive arm remained uninitialized, and requires the
// typed counters to agree with their plan-derived sources.
func appendInlineDistanceAttributionReasons(gateCase *ResourceGateCase, telemetry *TraversalExecutionTelemetry) {
	if telemetry == nil || telemetry.Diagnostic == nil || telemetry.Diagnostic.PlanReplay == nil {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance qualification requires exact plan branch evidence")
		return
	}
	inline := telemetry.Diagnostic.Counters.InlineShortestDistance
	if inline == nil {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance counters are missing")
		return
	}

	plan := telemetry.Diagnostic.PlanReplay.Counters
	required := []string{
		"sp_i2_distance_rows", "sp_i2_target_rows", "sp_i2_output_rows",
		"sp_i2_candidate_marker_rows", "sp_i2_fallback_marker_rows",
		"sp_i2_candidate_branch_rows", "sp_i2_fallback_branch_rows",
		"sp_i2_candidate_executor_loops", "sp_i2_fallback_executor_loops",
	}
	if telemetry.Summary.EmittedIdentity == optimize.ShortestPathPolicyI2DistanceGuardedV2 {
		required = append(required, "sp_i2_admission_rows", "sp_i2_admission_loops")
		if spI2DirectDevelopmentIdentity(telemetry.Summary.RequestedIdentity) {
			required = append(required, "sp_i2_direct_rows", "sp_i2_direct_loops")
		}
	}
	for _, name := range required {
		if _, found := plan[name]; !found {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance execution is missing exact plan counter "+name)
			return
		}
	}

	candidateMarker := plan["sp_i2_candidate_marker_rows"]
	fallbackMarker := plan["sp_i2_fallback_marker_rows"]
	candidateRows := plan["sp_i2_candidate_branch_rows"]
	fallbackRows := plan["sp_i2_fallback_branch_rows"]
	outputRows := plan["sp_i2_output_rows"]
	candidateLoops := plan["sp_i2_candidate_executor_loops"]
	fallbackLoops := plan["sp_i2_fallback_executor_loops"]
	stateRows := plan["sp_i2_distance_rows"]
	if (candidateMarker != 0 && candidateMarker != 1) || (fallbackMarker != 0 && fallbackMarker != 1) || candidateMarker+fallbackMarker != 1 {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance execution must attribute exactly one candidate or fallback marker")
	}
	if candidateRows < 0 || candidateRows > 1 || fallbackRows < 0 || fallbackRows > 1 || outputRows != candidateRows+fallbackRows {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance output does not equal its complementary branch rows")
	}
	directRows := plan["sp_i2_direct_rows"]
	if directRows < 0 || directRows > 1 {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance direct floor must return at most one row")
	}
	if plan["sp_i2_target_rows"]+directRows != candidateRows {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance candidate branch does not agree with its target receipt")
	}

	typed := map[string]*int64{
		"sp_i2_distance_rows":            inline.StateRows,
		"sp_i2_output_rows":              inline.OutputRows,
		"sp_i2_candidate_marker_rows":    inline.CandidateMarkerRows,
		"sp_i2_fallback_marker_rows":     inline.FallbackMarkerRows,
		"sp_i2_candidate_branch_rows":    inline.CandidateBranchRows,
		"sp_i2_fallback_branch_rows":     inline.FallbackBranchRows,
		"sp_i2_candidate_executor_loops": inline.CandidateExecutorLoops,
		"sp_i2_fallback_executor_loops":  inline.FallbackExecutorLoops,
	}
	if telemetry.Summary.EmittedIdentity == optimize.ShortestPathPolicyI2DistanceGuardedV2 {
		typed["sp_i2_admission_rows"] = inline.AdmissionProbeRows
		typed["sp_i2_admission_loops"] = inline.AdmissionProbeLoops
		typed["sp_i2_target_rows"] = inline.TargetRows
		if inline.FrontierGuardDominated == nil || inline.CapRelationship == "" || inline.ObservedOverflowReason == "" {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance V2 admission telemetry is incomplete")
		}
		if spI2DirectDevelopmentIdentity(telemetry.Summary.RequestedIdentity) {
			typed["sp_i2_direct_rows"] = inline.DirectProbeRows
			typed["sp_i2_direct_loops"] = inline.DirectProbeLoops
		}
	}
	for name, value := range typed {
		if value == nil || *value != plan[name] {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance typed counter does not match plan counter "+name)
		}
	}
	if inline.FrontierRows == nil || inline.StateRows == nil || *inline.FrontierRows != *inline.StateRows {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance conservative frontier bound does not match bounded state rows")
	}

	summary := telemetry.Summary
	stateLimit, stateLimitFound := summary.Caps["state_rows"]
	frontierLimit, frontierLimitFound := summary.Caps["frontier_rows"]
	validCaps := stateLimitFound && frontierLimitFound && stateLimit > 0 && frontierLimit > 0
	if !validCaps {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance attribution requires positive state and frontier caps")
	}
	if summary.RuntimeOutcomeAvailable == nil || !*summary.RuntimeOutcomeAvailable || summary.FallbackExecuted == nil || summary.Overflow == nil {
		gateCase.Reasons = append(gateCase.Reasons, "inline SP distance marker selection lacks a complete runtime receipt")
		return
	}
	if candidateMarker == 1 {
		expectedBranch := "inline_canonical_distance"
		if directRows == 1 {
			expectedBranch = "inline_direct_distance"
		} else if outputRows == 0 {
			expectedBranch = "inline_canonical_distance_no_path"
		}
		if candidateLoops != 1 || fallbackLoops != 0 || fallbackRows != 0 {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance candidate selection did not suppress the fallback executor and output arm")
		}
		if directRows == 1 && (stateRows != 0 || plan["sp_i2_admission_rows"] != 0 || plan["sp_i2_target_rows"] != 0) {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance direct floor did not suppress recursive admission and target work")
		}
		// FrontierRows is deliberately a conservative alias for the complete
		// bounded state relation, not an independently observable peak level.
		// Requiring that relation to remain within both reported caps is
		// conservative and admits the exact boundary without guessing which
		// aggregate gate would otherwise have fired.
		if validCaps && (stateRows > stateLimit || stateRows > frontierLimit) {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance candidate selection exceeds its state or conservative frontier cap")
		}
		expectedIdentity := string(optimize.ShortestPathExecutorI2GuardedDistance)
		if summary.EmittedIdentity == optimize.ShortestPathPolicyI2DistanceGuardedV2 {
			expectedIdentity = summary.RequestedIdentity
		}
		if summary.RuntimeIdentity != expectedIdentity || summary.RuntimeBranch != expectedBranch || *summary.FallbackExecuted || *summary.Overflow {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance candidate marker contradicts the runtime receipt")
		}
	}
	if fallbackMarker == 1 {
		if fallbackLoops != 1 || candidateLoops != 0 || candidateRows != 0 {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance fallback selection did not suppress the candidate executor and output arm")
		}
		// The current diagnostic contract exposes one bounded-state count and a
		// conservative frontier alias. It cannot identify which aggregate gate
		// fired, but an exact cap+1 row for at least one reported bound is required
		// to corroborate the overflow branch. Qualified production caps are equal,
		// so every production overflow has this observable sentinel.
		if validCaps && stateRows != stateLimit+1 && stateRows != frontierLimit+1 {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance fallback selection lacks an exact state or conservative frontier cap+1 sentinel")
		}
		if summary.RuntimeIdentity != string(optimize.ShortestPathExecutorS4CanonicalDistance) || summary.RuntimeBranch != "exact_s4_distance_fallback" || !*summary.FallbackExecuted || !*summary.Overflow {
			gateCase.Reasons = append(gateCase.Reasons, "inline SP distance fallback marker contradicts the runtime receipt")
		}
	}
}

// appendSuffixGuardAttributionReasons proves one and only one output arm ran,
// and rejects plans that accidentally retain orientation-v2's topology work.
func appendSuffixGuardAttributionReasons(gateCase *ResourceGateCase, diagnostic *TraversalExecutionDiagnostic) {
	if diagnostic == nil || diagnostic.PlanReplay == nil {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard qualification requires exact plan branch and sentinel evidence")
		return
	}
	counters := diagnostic.PlanReplay.Counters
	candidate, candidatePresent := counters["suffix_guard_candidate_marker_rows"]
	fallback, fallbackPresent := counters["suffix_guard_fallback_marker_rows"]
	if !candidatePresent || !fallbackPresent || (candidate != 0 && candidate != 1) || (fallback != 0 && fallback != 1) || candidate+fallback != 1 {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard execution must attribute exactly one candidate or fallback marker")
	}
	candidateRows, candidateRowsPresent := counters["suffix_guard_candidate_branch_rows"]
	fallbackRows, fallbackRowsPresent := counters["suffix_guard_fallback_branch_rows"]
	outputRows, outputRowsPresent := counters["suffix_guard_output_rows"]
	if !candidateRowsPresent || !fallbackRowsPresent || !outputRowsPresent || outputRows != candidateRows+fallbackRows {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard execution is missing exact complementary output-branch evidence")
	}
	candidateLoops, candidateLoopsPresent := counters["suffix_guard_candidate_executor_loops"]
	fallbackLoops, fallbackLoopsPresent := counters["suffix_guard_fallback_executor_loops"]
	if !candidateLoopsPresent || !fallbackLoopsPresent {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard execution is missing candidate or fallback executor-loop evidence")
	}
	if candidate == 1 && (candidateLoops != 1 || fallbackLoops != 0 || fallbackRows != 0) {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard candidate selection did not suppress the fallback executor and output arm")
	}
	if fallback == 1 && (fallbackLoops != 1 || candidateLoops != 0 || candidateRows != 0) {
		gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard fallback selection did not suppress the candidate executor and output arm")
	}
	for name := range counters {
		if strings.HasPrefix(name, "orientation_") &&
			(strings.Contains(name, "degree") || strings.Contains(name, "score") || strings.Contains(name, "decision")) {
			gateCase.Reasons = append(gateCase.Reasons, "suffix-reverse guard plan unexpectedly contains orientation topology work "+name)
		}
	}
}

// appendInlineASPAttributionReasons appends inline asp attribution reasons.
func appendInlineASPAttributionReasons(gateCase *ResourceGateCase, diagnostic *TraversalExecutionDiagnostic) {
	appendInlinePredecessorAttributionReasons(gateCase, diagnostic, "inline ASP")
}

// appendInlinePredecessorAttributionReasons appends inline predecessor attribution reasons.
func appendInlinePredecessorAttributionReasons(gateCase *ResourceGateCase, diagnostic *TraversalExecutionDiagnostic, label string) {
	if diagnostic == nil || diagnostic.PlanReplay == nil {
		gateCase.Reasons = append(gateCase.Reasons, label+" qualification requires exact plan branch evidence")
		return
	}
	counters := diagnostic.PlanReplay.Counters
	candidate, candidatePresent := counters["asp_i1_candidate_marker_rows"]
	fallback, fallbackPresent := counters["asp_i1_fallback_marker_rows"]
	if !candidatePresent || !fallbackPresent || candidate+fallback != 1 {
		gateCase.Reasons = append(gateCase.Reasons, label+" execution must attribute exactly one candidate or fallback marker")
	}
	candidateBranchRows, candidateBranchPresent := counters["asp_i1_candidate_branch_rows"]
	fallbackBranchRows, fallbackBranchPresent := counters["asp_i1_fallback_branch_rows"]
	if !candidateBranchPresent || !fallbackBranchPresent {
		gateCase.Reasons = append(gateCase.Reasons, label+" execution is missing exact candidate or fallback output-branch row evidence")
	}
	candidateExecutorLoops, candidateExecutorPresent := counters["asp_i1_candidate_executor_loops"]
	fallbackExecutorLoops, fallbackExecutorPresent := counters["asp_i1_fallback_executor_loops"]
	if !candidateExecutorPresent || !fallbackExecutorPresent {
		gateCase.Reasons = append(gateCase.Reasons, label+" execution is missing exact candidate or fallback executor-loop evidence")
	}
	if candidate == 1 && fallbackBranchRows != 0 {
		gateCase.Reasons = append(gateCase.Reasons, label+" fallback output arm emitted rows while the candidate was selected")
	}
	if candidate == 1 && fallbackExecutorLoops != 0 {
		gateCase.Reasons = append(gateCase.Reasons, label+" fallback executor ran while the candidate was selected")
	}
	if candidate == 1 && candidateExecutorLoops != 1 {
		gateCase.Reasons = append(gateCase.Reasons, label+" candidate marker must bind exactly one selected executor loop")
	}
	if fallback == 1 && candidateBranchRows != 0 {
		gateCase.Reasons = append(gateCase.Reasons, label+" candidate output arm emitted rows while fallback was selected")
	}
	if fallback == 1 && candidateExecutorLoops != 0 {
		gateCase.Reasons = append(gateCase.Reasons, label+" candidate executor ran while fallback was selected")
	}
	if fallback == 1 && fallbackExecutorLoops != 1 {
		gateCase.Reasons = append(gateCase.Reasons, label+" fallback marker must bind exactly one selected executor loop")
	}
}

// appendOrientationAttributionReasons appends orientation attribution reasons.
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
	if candidate == 1 && counters["orientation_incumbent_branch_loops"] != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "orientation incumbent arm performed work while the candidate was selected")
	}
	if incumbent == 1 && counters["orientation_candidate_branch_loops"] != 0 {
		gateCase.Reasons = append(gateCase.Reasons, "orientation candidate arm performed work while the incumbent was selected")
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
	return strings.Contains(name, "probe") || strings.Contains(name, "suffix") || strings.Contains(name, "state") ||
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
	if guard := counters.SuffixGuard; guard != nil {
		set("root_rows", guard.RootPresenceRows)
		set("suffix_rows", guard.SuffixRows)
		set("reverse_seed_rows", guard.SuffixRows)
		set("state_rows", guard.StateRows)
		set("output_rows", guard.OutputRows)
	}
	if component := counters.SuffixComponent; component != nil {
		set("suffix_rows", component.SuffixRows)
		set("reverse_seed_rows", component.SuffixRows)
		set("state_rows", component.ReverseStateRows)
		set("output_rows", component.OutputRows)
		if component.OrderedNodeHydrationRows != nil && component.OrderedEdgeHydrationRows != nil {
			observed["hydration_rows"] = *component.OrderedNodeHydrationRows + *component.OrderedEdgeHydrationRows
		}
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
	if inline := counters.InlineShortestPath; inline != nil {
		set("state_rows", inline.DistanceRows)
		set("predecessor_rows", inline.PredecessorRows)
		set("output_rows", inline.EnumerationRows)
		set("output_paths", inline.OutputPaths)
		set("output_bytes", inline.OutputBytes)
	}
	if inline := counters.InlineShortestDistance; inline != nil {
		set("state_rows", inline.StateRows)
		set("frontier_rows", inline.FrontierRows)
		set("queue_rows", inline.FrontierRows)
		set("output_rows", inline.OutputRows)
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
