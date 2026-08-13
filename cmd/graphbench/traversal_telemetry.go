// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"
	"strings"
)

const (
	// TraversalExecutionTelemetrySchemaVersion is the current serialized telemetry schema revision.
	TraversalExecutionTelemetrySchemaVersion = 1

	// TraversalTelemetryLevelSummary records only the production execution identity and outcome.
	TraversalTelemetryLevelSummary TraversalTelemetryLevel = "summary"
	// TraversalTelemetryLevelDiagnostic adds counters from a separate untimed replay.
	TraversalTelemetryLevelDiagnostic TraversalTelemetryLevel = "diagnostic"

	// TraversalTelemetryCounterStatusComplete records a replay with every declared family populated by invocation-local counters.
	TraversalTelemetryCounterStatusComplete TraversalTelemetryCounterStatus = "complete"
	// TraversalTelemetryCounterStatusPlanPartial records honest SQL-visible EXPLAIN evidence that is insufficient for qualification.
	TraversalTelemetryCounterStatusPlanPartial TraversalTelemetryCounterStatus = "plan_derived_partial"
	// TraversalTelemetryCounterStatusHiddenUnavailable records a function-backed executor whose internal work counters were unavailable.
	TraversalTelemetryCounterStatusHiddenUnavailable TraversalTelemetryCounterStatus = "hidden_counters_unavailable"

	// TraversalTelemetryFamilyOrdinary identifies ordinary DFS or recursive-CTE traversal work.
	TraversalTelemetryFamilyOrdinary TraversalTelemetryFamily = "ordinary"
	// TraversalTelemetryFamilyOrientation identifies runtime orientation-policy work.
	TraversalTelemetryFamilyOrientation TraversalTelemetryFamily = "orientation"
	// TraversalTelemetryFamilySP identifies singleton shortest-path work.
	TraversalTelemetryFamilySP TraversalTelemetryFamily = "shortest_path"
	// TraversalTelemetryFamilyASP identifies all-shortest-path work.
	TraversalTelemetryFamilyASP TraversalTelemetryFamily = "all_shortest_paths"
	// TraversalTelemetryFamilyHydration identifies post-discovery path hydration work.
	TraversalTelemetryFamilyHydration TraversalTelemetryFamily = "hydration"
	// TraversalTelemetryFamilyWorkspace identifies measured session and pool workspace high-water marks.
	TraversalTelemetryFamilyWorkspace TraversalTelemetryFamily = "workspace"
)

// TraversalTelemetryLevel identifies whether a record contains only lightweight summary data or an untimed diagnostic replay.
type TraversalTelemetryLevel string

// TraversalTelemetryFamily identifies a counter family required for an invocation.
type TraversalTelemetryFamily string

// TraversalTelemetryCounterStatus identifies whether an untimed replay exposes every required invocation-local counter.
type TraversalTelemetryCounterStatus string

// TraversalExecutionTelemetry records versioned execution identity and optional diagnostic replay counters.
type TraversalExecutionTelemetry struct {
	// SchemaVersion identifies the serialized telemetry schema revision.
	SchemaVersion int `json:"schema_version"`
	// Level identifies the instrumentation boundary represented by this record.
	Level TraversalTelemetryLevel `json:"level"`
	// Summary contains lightweight data captured for the production invocation.
	Summary TraversalExecutionSummary `json:"summary"`
	// Diagnostic contains counters from a separate untimed replay when Level is diagnostic.
	Diagnostic *TraversalExecutionDiagnostic `json:"diagnostic,omitempty"`
}

// TraversalExecutionSummary identifies the planned and executed traversal policy without detailed work counters.
type TraversalExecutionSummary struct {
	RequestedIdentity string   `json:"requested_identity"`
	PlannedIdentities []string `json:"planned_identities"`
	EmittedIdentity   string   `json:"emitted_identity"`
	RuntimeIdentity   string   `json:"runtime_identity"`
	AppliedIdentity   string   `json:"applied_identity"`
	SelectorVersion   string   `json:"selector_version"`
	SchedulerVersion  string   `json:"scheduler_version"`
	ExecutionBoundary string   `json:"execution_boundary,omitempty"`
	// ObservationMode identifies whether the public boundary consumes scalar,
	// ordered-ID, or hydrated path values.
	ObservationMode string           `json:"observation_mode,omitempty"`
	Caps            map[string]int64 `json:"caps"`
	// RuntimeOutcomeAvailable distinguishes executor evidence from a
	// translator prediction. When false, runtime-dependent facts stay unset.
	RuntimeOutcomeAvailable *bool  `json:"runtime_outcome_available,omitempty"`
	RuntimeBranch           string `json:"runtime_branch"`
	Overflow                *bool  `json:"overflow"`
	FallbackExecuted        *bool  `json:"fallback_executed"`
	FallbackIdentity        string `json:"fallback_identity,omitempty"`
	// WouldSelectIdentity records a shadow policy choice while RuntimeIdentity
	// and AppliedIdentity remain bound to the only executed incumbent arm.
	WouldSelectIdentity string `json:"would_select_identity,omitempty"`
	// Provenance maps summary field paths to the optimizer, SQL branch, function, or executor fact that produced them.
	Provenance map[string]string `json:"provenance"`
}

// TraversalExecutionDiagnostic contains counters from one tool-only replay, separate from all timed samples.
type TraversalExecutionDiagnostic struct {
	// InvocationID uniquely identifies the diagnostic invocation and its session-local workspace.
	InvocationID string `json:"invocation_id"`
	// ConnectionID identifies the same backend connection used by the production invocation.
	ConnectionID string `json:"connection_id"`
	// TimedSample is required and must be false so replay resources cannot be attributed to latency samples.
	TimedSample *bool `json:"timed_sample"`
	// RequiredFamilies declares exactly which counter groups must be complete for this invocation.
	RequiredFamilies []TraversalTelemetryFamily  `json:"required_families"`
	Counters         TraversalDiagnosticCounters `json:"counters"`
	// CounterStatus distinguishes qualification-complete invocation metrics from partial plan evidence or opaque function work.
	CounterStatus TraversalTelemetryCounterStatus `json:"counter_status"`
	// IncompleteReasons explains why a diagnostic replay cannot qualify when CounterStatus is not complete.
	IncompleteReasons []string `json:"incomplete_reasons,omitempty"`
	// PlanReplay records only counters PostgreSQL exposes through the separate TIMING OFF JSON EXPLAIN replay.
	PlanReplay *TraversalPlanReplayEvidence `json:"plan_replay,omitempty"`
	// Provenance maps diagnostic counter paths to the function, CTE, or executor metric that produced them.
	Provenance map[string]string `json:"provenance"`
}

// TraversalPlanReplayEvidence contains honest SQL-visible counters without pretending an outer Function Scan exposes hidden executor work.
type TraversalPlanReplayEvidence struct {
	// Source identifies the exact untimed diagnostic boundary.
	Source string `json:"source"`
	// Counters contains only values with explicit PostgreSQL plan provenance.
	Counters map[string]int64 `json:"counters,omitempty"`
	// Flags contains only boolean outcomes observable from named plan branches or guards.
	Flags map[string]bool `json:"flags,omitempty"`
	// Provenance maps every counter and flag to its JSON EXPLAIN derivation.
	Provenance map[string]string `json:"provenance"`
}

// TraversalDiagnosticCounters groups independent runtime counter families.
type TraversalDiagnosticCounters struct {
	Ordinary         *OrdinaryTraversalCounters         `json:"ordinary,omitempty"`
	Orientation      *OrientationTraversalCounters      `json:"orientation,omitempty"`
	ShortestPath     *ShortestPathTraversalCounters     `json:"shortest_path,omitempty"`
	AllShortestPaths *AllShortestPathsTraversalCounters `json:"all_shortest_paths,omitempty"`
	InlineASP        *InlineASPTraversalCounters        `json:"inline_asp,omitempty"`
	Hydration        *TraversalHydrationCounters        `json:"hydration,omitempty"`
	Workspace        *TraversalWorkspaceCounters        `json:"workspace,omitempty"`
}

// InlineASPTraversalCounters records the complete set of bounded relations
// and complementary branch markers exposed by the guarded I1 statement.
type InlineASPTraversalCounters struct {
	DistanceRows        *int64 `json:"distance_rows"`
	PredecessorRows     *int64 `json:"predecessor_rows"`
	EnumerationRows     *int64 `json:"enumeration_rows"`
	OutputPaths         *int64 `json:"output_paths"`
	OutputBytes         *int64 `json:"output_bytes"`
	CandidateMarkerRows *int64 `json:"candidate_marker_rows"`
	FallbackMarkerRows  *int64 `json:"fallback_marker_rows"`
	CandidateBranchRows *int64 `json:"candidate_branch_rows"`
	FallbackBranchRows  *int64 `json:"fallback_branch_rows"`
}

// OrdinaryTraversalCounters records DFS or recursive-CTE discovery work.
type OrdinaryTraversalCounters struct {
	Roots                     *int64 `json:"roots"`
	EdgeCandidates            *int64 `json:"edge_candidates"`
	AdmittedStates            *int64 `json:"admitted_states"`
	RelationshipRepeatRejects *int64 `json:"relationship_repeat_rejects"`
	RecursiveRows             *int64 `json:"recursive_rows"`
	PeakState                 *int64 `json:"peak_state"`
	EmittedTrails             *int64 `json:"emitted_trails"`
	HydrationRows             *int64 `json:"hydration_rows"`
}

// OrientationTraversalCounters records bounded policy probes and selected-branch work.
type OrientationTraversalCounters struct {
	ForwardSeeds                  *int64   `json:"forward_seeds"`
	ReverseSeeds                  *int64   `json:"reverse_seeds"`
	DuplicateSeeds                *int64   `json:"duplicate_seeds"`
	SuffixRows                    *int64   `json:"suffix_rows"`
	DistinctBoundaries            *int64   `json:"distinct_boundaries"`
	TypedDirectionalDegreeSamples *int64   `json:"typed_directional_degree_samples"`
	ForwardDegreeSamples          *int64   `json:"forward_degree_samples"`
	ReverseDegreeSamples          *int64   `json:"reverse_degree_samples"`
	ShallowSurvivalRows           *int64   `json:"shallow_survival_rows"`
	ShallowSurvival               *float64 `json:"shallow_survival"`
	ProbeRows                     *int64   `json:"probe_rows"`
	ProbeTimeNS                   *int64   `json:"probe_time_ns"`
	ProbeBufferHits               *int64   `json:"probe_buffer_hits"`
	ProbeBufferReads              *int64   `json:"probe_buffer_reads"`
	ForwardScore                  *float64 `json:"forward_score"`
	ReverseScore                  *float64 `json:"reverse_score"`
	SelectedSide                  string   `json:"selected_side"`
	SentinelOverflow              *bool    `json:"sentinel_overflow"`
	BranchLoops                   *int64   `json:"branch_loops"`
}

// ShortestPathLevelCounters records one scheduler action and the two-sided frontier state it observed.
type ShortestPathLevelCounters struct {
	SearchID          int64  `json:"search_id"`
	ActionIndex       int64  `json:"action_index"`
	Side              string `json:"side"`
	Action            string `json:"action"`
	Depth             *int64 `json:"depth"`
	FrontierRows      *int64 `json:"frontier_rows"`
	CandidateEdges    *int64 `json:"candidate_edges"`
	DistinctNewNodes  *int64 `json:"distinct_new_nodes"`
	SeenRows          *int64 `json:"seen_rows"`
	QueueRows         *int64 `json:"queue_rows"`
	PredecessorRows   *int64 `json:"predecessor_rows"`
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// Provenance names the invocation-local stage or executor metric that produced this level row.
	Provenance string `json:"provenance"`
}

// ShortestPathTraversalCounters records bidirectional scheduler, frontier, and witness work.
type ShortestPathTraversalCounters struct {
	SchedulerActions  *int64                      `json:"scheduler_actions"`
	Levels            []ShortestPathLevelCounters `json:"levels"`
	CandidateEdges    *int64                      `json:"candidate_edges"`
	DistinctNewNodes  *int64                      `json:"distinct_new_nodes"`
	SeenPeak          *int64                      `json:"seen_peak"`
	FrontierPeak      *int64                      `json:"frontier_peak"`
	QueuePeak         *int64                      `json:"queue_peak"`
	PredecessorPeak   *int64                      `json:"predecessor_peak"`
	MeetingCandidates *int64                      `json:"meeting_candidates"`
	FrozenDistance    *int64                      `json:"frozen_distance"`
	WitnessRows       *int64                      `json:"witness_rows"`
	FallbackExecuted  *bool                       `json:"fallback_executed"`
}

// AllShortestPathsTraversalCounters records SP search work plus predecessor and output enumeration work.
type AllShortestPathsTraversalCounters struct {
	Search                        ShortestPathTraversalCounters `json:"search"`
	SameDepthPredecessorAdditions *int64                        `json:"same_depth_predecessor_additions"`
	PredecessorPeak               *int64                        `json:"predecessor_peak"`
	MeetingNodes                  *int64                        `json:"meeting_nodes"`
	CutDepth                      *int64                        `json:"cut_depth"`
	PathCountEstimate             *int64                        `json:"path_count_estimate"`
	PathCountSaturated            *bool                         `json:"path_count_saturated"`
	EnumeratedCandidates          *int64                        `json:"enumerated_candidates"`
	DuplicateRejects              *int64                        `json:"duplicate_rejects"`
	OutputPaths                   *int64                        `json:"output_paths"`
	OutputEdgeCells               *int64                        `json:"output_edge_cells"`
	OutputBytes                   *int64                        `json:"output_bytes"`
}

// TraversalHydrationCounters records post-discovery materialization separately from traversal work.
type TraversalHydrationCounters struct {
	PathCount   *int64 `json:"path_count"`
	NodeLookups *int64 `json:"node_lookups"`
	EdgeLookups *int64 `json:"edge_lookups"`
	Loops       *int64 `json:"loops"`
	Rows        *int64 `json:"rows"`
	TimeNS      *int64 `json:"time_ns"`
	Bytes       *int64 `json:"bytes"`
}

// TraversalWorkspaceCounters records measured high-water memory attributed to
// one diagnostic invocation and to all simultaneously active pool sessions.
type TraversalWorkspaceCounters struct {
	SessionPeakBytes *int64 `json:"session_peak_bytes"`
	PoolPeakBytes    *int64 `json:"pool_peak_bytes"`
}

// ValidateTraversalExecutionTelemetry rejects incomplete or contradictory telemetry.
func ValidateTraversalExecutionTelemetry(telemetry *TraversalExecutionTelemetry) error {
	if telemetry == nil {
		return fmt.Errorf("traversal execution telemetry is missing")
	}

	return telemetry.Validate()
}

// Validate rejects unsupported schema versions, incomplete summaries, timed diagnostic replays, and missing counters or provenance.
func (s TraversalExecutionTelemetry) Validate() error {
	var problems []string

	if s.SchemaVersion != TraversalExecutionTelemetrySchemaVersion {
		problems = append(problems, fmt.Sprintf("schema_version must be %d", TraversalExecutionTelemetrySchemaVersion))
	}
	if s.Level != TraversalTelemetryLevelSummary && s.Level != TraversalTelemetryLevelDiagnostic {
		problems = append(problems, "level must be summary or diagnostic")
	}

	validateTraversalSummary(s.Summary, &problems)

	switch s.Level {
	case TraversalTelemetryLevelSummary:
		if s.Diagnostic != nil {
			problems = append(problems, "summary telemetry must not contain a diagnostic replay")
		}
	case TraversalTelemetryLevelDiagnostic:
		validateTraversalDiagnostic(s.Diagnostic, &problems)
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid traversal execution telemetry: %s", strings.Join(problems, "; "))
	}

	return nil
}

func validateTraversalSummary(summary TraversalExecutionSummary, problems *[]string) {
	requireText("summary.requested_identity", summary.RequestedIdentity, problems)
	if len(summary.PlannedIdentities) == 0 {
		*problems = append(*problems, "summary.planned_identities is missing")
	}
	planned := map[string]struct{}{}
	for idx, identity := range summary.PlannedIdentities {
		requireText(fmt.Sprintf("summary.planned_identities[%d]", idx), identity, problems)
		if _, duplicate := planned[identity]; duplicate {
			*problems = append(*problems, fmt.Sprintf("summary.planned_identities contains duplicate %q", identity))
		}
		planned[identity] = struct{}{}
	}
	requireText("summary.emitted_identity", summary.EmittedIdentity, problems)
	runtimeOutcomeAvailable := summary.RuntimeOutcomeAvailable == nil || *summary.RuntimeOutcomeAvailable
	if runtimeOutcomeAvailable {
		requireText("summary.runtime_identity", summary.RuntimeIdentity, problems)
		requireText("summary.applied_identity", summary.AppliedIdentity, problems)
	} else {
		if summary.RuntimeIdentity != "" || summary.AppliedIdentity != "" {
			*problems = append(*problems, "summary unavailable runtime outcome must not assert runtime or applied identity")
		}
		if summary.RuntimeBranch != "runtime_outcome_unavailable" {
			*problems = append(*problems, "summary unavailable runtime outcome must use runtime_outcome_unavailable branch")
		}
		if summary.Overflow != nil || summary.FallbackExecuted != nil || summary.FallbackIdentity != "" {
			*problems = append(*problems, "summary unavailable runtime outcome must not assert overflow or fallback facts")
		}
	}
	requireText("summary.selector_version", summary.SelectorVersion, problems)
	requireText("summary.scheduler_version", summary.SchedulerVersion, problems)
	requireText("summary.runtime_branch", summary.RuntimeBranch, problems)
	if runtimeOutcomeAvailable {
		requirePointer("summary.overflow", summary.Overflow, problems)
		requirePointer("summary.fallback_executed", summary.FallbackExecuted, problems)
	}
	if runtimeOutcomeAvailable && summary.RuntimeIdentity != "" {
		if _, ok := planned[summary.RuntimeIdentity]; !ok {
			*problems = append(*problems, "summary.runtime_identity is not a planned identity")
		}
	}
	if runtimeOutcomeAvailable && summary.FallbackExecuted != nil && *summary.FallbackExecuted {
		requireText("summary.fallback_identity", summary.FallbackIdentity, problems)
		if summary.FallbackIdentity != "" {
			if _, ok := planned[summary.FallbackIdentity]; !ok {
				*problems = append(*problems, "summary.fallback_identity is not a planned identity")
			}
			if summary.AppliedIdentity != summary.FallbackIdentity {
				*problems = append(*problems, "summary.applied_identity must equal fallback_identity when fallback executes")
			}
		}
	} else if runtimeOutcomeAvailable && summary.FallbackExecuted != nil && summary.AppliedIdentity != "" && summary.RuntimeIdentity != "" && summary.AppliedIdentity != summary.RuntimeIdentity {
		*problems = append(*problems, "summary.applied_identity must equal runtime_identity when fallback does not execute")
	}
	if summary.WouldSelectIdentity != "" {
		if _, ok := planned[summary.WouldSelectIdentity]; !ok {
			*problems = append(*problems, "summary.would_select_identity is not a planned identity")
		}
		requireProvenance("summary.would_select_identity", summary.Provenance["would_select_identity"], problems)
	}

	for _, path := range []string{
		"requested_identity", "planned_identities", "emitted_identity", "runtime_identity", "applied_identity",
		"selector_version", "scheduler_version", "runtime_branch",
	} {
		requireProvenance("summary."+path, summary.Provenance[path], problems)
	}
	if summary.RuntimeOutcomeAvailable != nil {
		requireProvenance("summary.runtime_outcome_available", summary.Provenance["runtime_outcome_available"], problems)
	}
	if summary.ObservationMode != "" {
		requireProvenance("summary.observation_mode", summary.Provenance["observation_mode"], problems)
	}
	if runtimeOutcomeAvailable {
		for _, path := range []string{"overflow", "fallback_executed"} {
			requireProvenance("summary."+path, summary.Provenance[path], problems)
		}
	}
	for capName := range summary.Caps {
		requireProvenance("summary.caps."+capName, summary.Provenance["caps."+capName], problems)
	}
	if runtimeOutcomeAvailable && summary.FallbackExecuted != nil && *summary.FallbackExecuted {
		requireProvenance("summary.fallback_identity", summary.Provenance["fallback_identity"], problems)
	}
}

func validateTraversalDiagnostic(diagnostic *TraversalExecutionDiagnostic, problems *[]string) {
	if diagnostic == nil {
		*problems = append(*problems, "diagnostic replay is missing")
		return
	}

	requireText("diagnostic.invocation_id", diagnostic.InvocationID, problems)
	requireText("diagnostic.connection_id", diagnostic.ConnectionID, problems)
	requirePointer("diagnostic.timed_sample", diagnostic.TimedSample, problems)
	if diagnostic.TimedSample != nil && *diagnostic.TimedSample {
		*problems = append(*problems, "diagnostic.timed_sample must be false")
	}
	if len(diagnostic.RequiredFamilies) == 0 {
		*problems = append(*problems, "diagnostic.required_families is missing")
	}
	counterStatus := diagnostic.CounterStatus
	if counterStatus == "" {
		// Version-one in-memory callers predate the explicit completeness field;
		// their fully populated typed counters retain complete semantics.
		counterStatus = TraversalTelemetryCounterStatusComplete
	}
	if counterStatus != TraversalTelemetryCounterStatusComplete &&
		counterStatus != TraversalTelemetryCounterStatusPlanPartial &&
		counterStatus != TraversalTelemetryCounterStatusHiddenUnavailable {
		*problems = append(*problems, "diagnostic.counter_status is unsupported")
	}
	if counterStatus != TraversalTelemetryCounterStatusComplete && len(diagnostic.IncompleteReasons) == 0 {
		*problems = append(*problems, "diagnostic.incomplete_reasons is missing for incomplete counters")
	}
	if counterStatus == TraversalTelemetryCounterStatusPlanPartial && diagnostic.PlanReplay == nil {
		*problems = append(*problems, "diagnostic.plan_replay is missing for plan-derived counters")
	}
	if diagnostic.PlanReplay != nil {
		validateTraversalPlanReplay(diagnostic.PlanReplay, problems)
	}

	seen := map[TraversalTelemetryFamily]struct{}{}
	for _, family := range diagnostic.RequiredFamilies {
		if _, duplicate := seen[family]; duplicate {
			*problems = append(*problems, fmt.Sprintf("diagnostic.required_families contains duplicate %q", family))
			continue
		}
		seen[family] = struct{}{}

		if counterStatus != TraversalTelemetryCounterStatusComplete {
			continue
		}

		switch family {
		case TraversalTelemetryFamilyOrdinary:
			validateOrdinaryCounters(diagnostic.Counters.Ordinary, diagnostic.Provenance, problems)
		case TraversalTelemetryFamilyOrientation:
			validateOrientationCounters(diagnostic.Counters.Orientation, diagnostic.Provenance, problems)
		case TraversalTelemetryFamilySP:
			validateShortestPathCounters("shortest_path", diagnostic.Counters.ShortestPath, diagnostic.Provenance, problems)
		case TraversalTelemetryFamilyASP:
			if diagnostic.Counters.InlineASP != nil {
				validateInlineASPCounters(diagnostic.Counters.InlineASP, diagnostic.Provenance, problems)
			} else {
				validateAllShortestPathsCounters(diagnostic.Counters.AllShortestPaths, diagnostic.Provenance, problems)
			}
		case TraversalTelemetryFamilyHydration:
			validateHydrationCounters(diagnostic.Counters.Hydration, diagnostic.Provenance, problems)
		case TraversalTelemetryFamilyWorkspace:
			validateWorkspaceCounters(diagnostic.Counters.Workspace, diagnostic.Provenance, problems)
		default:
			*problems = append(*problems, fmt.Sprintf("diagnostic.required_families contains unsupported family %q", family))
		}
	}

	if counterStatus != TraversalTelemetryCounterStatusComplete {
		return
	}

	for family, present := range map[TraversalTelemetryFamily]bool{
		TraversalTelemetryFamilyOrdinary:    diagnostic.Counters.Ordinary != nil,
		TraversalTelemetryFamilyOrientation: diagnostic.Counters.Orientation != nil,
		TraversalTelemetryFamilySP:          diagnostic.Counters.ShortestPath != nil,
		TraversalTelemetryFamilyASP:         diagnostic.Counters.AllShortestPaths != nil || diagnostic.Counters.InlineASP != nil,
		TraversalTelemetryFamilyHydration:   diagnostic.Counters.Hydration != nil,
		TraversalTelemetryFamilyWorkspace:   diagnostic.Counters.Workspace != nil,
	} {
		if present && !slices.Contains(diagnostic.RequiredFamilies, family) {
			*problems = append(*problems, fmt.Sprintf("diagnostic counter family %q is present but not declared", family))
		}
	}
}

func validateInlineASPCounters(counters *InlineASPTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.inline_asp is missing")
		return
	}
	requireCounters("inline_asp", provenance, problems, map[string]*int64{
		"distance_rows": counters.DistanceRows, "predecessor_rows": counters.PredecessorRows,
		"enumeration_rows": counters.EnumerationRows, "output_paths": counters.OutputPaths,
		"output_bytes": counters.OutputBytes, "candidate_marker_rows": counters.CandidateMarkerRows,
		"fallback_marker_rows": counters.FallbackMarkerRows, "candidate_branch_rows": counters.CandidateBranchRows,
		"fallback_branch_rows": counters.FallbackBranchRows,
	})
}

func validateTraversalPlanReplay(replay *TraversalPlanReplayEvidence, problems *[]string) {
	if replay == nil {
		return
	}
	requireText("diagnostic.plan_replay.source", replay.Source, problems)
	if len(replay.Counters) == 0 && len(replay.Flags) == 0 {
		*problems = append(*problems, "diagnostic.plan_replay contains no observable counters or flags")
	}
	for name := range replay.Counters {
		requireProvenance("diagnostic.plan_replay.counters."+name, replay.Provenance["counters."+name], problems)
	}
	for name := range replay.Flags {
		requireProvenance("diagnostic.plan_replay.flags."+name, replay.Provenance["flags."+name], problems)
	}
}

func validateOrdinaryCounters(counters *OrdinaryTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.ordinary is missing")
		return
	}

	requireCounters("ordinary", provenance, problems, map[string]*int64{
		"roots": counters.Roots, "edge_candidates": counters.EdgeCandidates, "admitted_states": counters.AdmittedStates,
		"relationship_repeat_rejects": counters.RelationshipRepeatRejects, "recursive_rows": counters.RecursiveRows,
		"peak_state": counters.PeakState, "emitted_trails": counters.EmittedTrails, "hydration_rows": counters.HydrationRows,
	})
}

func validateOrientationCounters(counters *OrientationTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.orientation is missing")
		return
	}

	requireCounters("orientation", provenance, problems, map[string]*int64{
		"forward_seeds": counters.ForwardSeeds, "reverse_seeds": counters.ReverseSeeds, "duplicate_seeds": counters.DuplicateSeeds,
		"suffix_rows": counters.SuffixRows, "distinct_boundaries": counters.DistinctBoundaries,
		"typed_directional_degree_samples": counters.TypedDirectionalDegreeSamples, "probe_rows": counters.ProbeRows,
		"forward_degree_samples": counters.ForwardDegreeSamples, "reverse_degree_samples": counters.ReverseDegreeSamples,
		"shallow_survival_rows": counters.ShallowSurvivalRows,
		"probe_time_ns":         counters.ProbeTimeNS, "probe_buffer_hits": counters.ProbeBufferHits,
		"probe_buffer_reads": counters.ProbeBufferReads, "branch_loops": counters.BranchLoops,
	})
	requirePointerAndProvenance("orientation.shallow_survival", counters.ShallowSurvival, provenance, problems)
	requirePointerAndProvenance("orientation.forward_score", counters.ForwardScore, provenance, problems)
	requirePointerAndProvenance("orientation.reverse_score", counters.ReverseScore, provenance, problems)
	requireText("diagnostic.counters.orientation.selected_side", counters.SelectedSide, problems)
	requireProvenance("diagnostic.counters.orientation.selected_side", provenance["orientation.selected_side"], problems)
	requirePointerAndProvenance("orientation.sentinel_overflow", counters.SentinelOverflow, provenance, problems)
}

func validateShortestPathCounters(prefix string, counters *ShortestPathTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters."+prefix+" is missing")
		return
	}

	requireCounters(prefix, provenance, problems, map[string]*int64{
		"scheduler_actions": counters.SchedulerActions, "candidate_edges": counters.CandidateEdges,
		"distinct_new_nodes": counters.DistinctNewNodes, "seen_peak": counters.SeenPeak, "frontier_peak": counters.FrontierPeak,
		"queue_peak": counters.QueuePeak, "predecessor_peak": counters.PredecessorPeak, "meeting_candidates": counters.MeetingCandidates,
		"frozen_distance": counters.FrozenDistance, "witness_rows": counters.WitnessRows,
	})
	requirePointerAndProvenance(prefix+".fallback_executed", counters.FallbackExecuted, provenance, problems)
	if len(counters.Levels) == 0 {
		*problems = append(*problems, "diagnostic.counters."+prefix+".levels is missing")
	}
	for idx, level := range counters.Levels {
		levelPath := fmt.Sprintf("diagnostic.counters.%s.levels[%d]", prefix, idx)
		requireText(levelPath+".side", level.Side, problems)
		requireText(levelPath+".action", level.Action, problems)
		requirePointer(levelPath+".depth", level.Depth, problems)
		requirePointer(levelPath+".frontier_rows", level.FrontierRows, problems)
		requirePointer(levelPath+".candidate_edges", level.CandidateEdges, problems)
		requirePointer(levelPath+".distinct_new_nodes", level.DistinctNewNodes, problems)
		requirePointer(levelPath+".seen_rows", level.SeenRows, problems)
		requirePointer(levelPath+".queue_rows", level.QueueRows, problems)
		requirePointer(levelPath+".predecessor_rows", level.PredecessorRows, problems)
		requirePointer(levelPath+".meeting_candidates", level.MeetingCandidates, problems)
		requireProvenance(levelPath, level.Provenance, problems)
	}
}

func validateAllShortestPathsCounters(counters *AllShortestPathsTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.all_shortest_paths is missing")
		return
	}

	validateShortestPathCounters("all_shortest_paths.search", &counters.Search, provenance, problems)
	requireCounters("all_shortest_paths", provenance, problems, map[string]*int64{
		"same_depth_predecessor_additions": counters.SameDepthPredecessorAdditions, "predecessor_peak": counters.PredecessorPeak,
		"meeting_nodes": counters.MeetingNodes, "cut_depth": counters.CutDepth, "path_count_estimate": counters.PathCountEstimate,
		"enumerated_candidates": counters.EnumeratedCandidates, "duplicate_rejects": counters.DuplicateRejects,
		"output_paths": counters.OutputPaths, "output_edge_cells": counters.OutputEdgeCells, "output_bytes": counters.OutputBytes,
	})
	requirePointerAndProvenance("all_shortest_paths.path_count_saturated", counters.PathCountSaturated, provenance, problems)
}

func validateHydrationCounters(counters *TraversalHydrationCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.hydration is missing")
		return
	}

	requireCounters("hydration", provenance, problems, map[string]*int64{
		"path_count": counters.PathCount, "node_lookups": counters.NodeLookups, "edge_lookups": counters.EdgeLookups,
		"loops": counters.Loops, "rows": counters.Rows, "time_ns": counters.TimeNS, "bytes": counters.Bytes,
	})
}

func validateWorkspaceCounters(counters *TraversalWorkspaceCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.workspace is missing")
		return
	}

	requireCounters("workspace", provenance, problems, map[string]*int64{
		"session_peak_bytes": counters.SessionPeakBytes,
		"pool_peak_bytes":    counters.PoolPeakBytes,
	})
}

func requireCounters(prefix string, provenance map[string]string, problems *[]string, counters map[string]*int64) {
	for name, value := range counters {
		requirePointerAndProvenance(prefix+"."+name, value, provenance, problems)
	}
}

func requirePointerAndProvenance[T any](path string, value *T, provenance map[string]string, problems *[]string) {
	requirePointer("diagnostic.counters."+path, value, problems)
	requireProvenance("diagnostic.counters."+path, provenance[path], problems)
}

func requirePointer[T any](path string, value *T, problems *[]string) {
	if value == nil {
		*problems = append(*problems, path+" is missing")
	}
}

func requireText(path, value string, problems *[]string) {
	if strings.TrimSpace(value) == "" {
		*problems = append(*problems, path+" is missing")
	}
}

func requireProvenance(path, value string, problems *[]string) {
	if strings.TrimSpace(value) == "" {
		*problems = append(*problems, path+" provenance is missing")
	}
}
