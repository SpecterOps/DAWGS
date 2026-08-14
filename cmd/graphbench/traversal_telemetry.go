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
	TraversalExecutionTelemetrySchemaVersion = 2

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
	// TraversalTelemetryFamilySuffixGuard identifies bounded reverse-first
	// fixed-suffix admission and its exact fallback boundary.
	TraversalTelemetryFamilySuffixGuard TraversalTelemetryFamily = "suffix_guard"
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
	// RequestedIdentity identifies the requested identity.
	RequestedIdentity string `json:"requested_identity"`
	// PlannedIdentities supplies the planned identities input to the TraversalExecutionSummary contract.
	PlannedIdentities []string `json:"planned_identities"`
	// EmittedIdentity identifies the emitted identity.
	EmittedIdentity string `json:"emitted_identity"`
	// RuntimeIdentity identifies the runtime identity.
	RuntimeIdentity string `json:"runtime_identity"`
	// AppliedIdentity identifies the applied identity.
	AppliedIdentity string `json:"applied_identity"`
	// SelectorVersion identifies the schema version for selector version.
	SelectorVersion string `json:"selector_version"`
	// SchedulerVersion identifies the schema version for scheduler version.
	SchedulerVersion string `json:"scheduler_version"`
	// ExecutionBoundary supplies the execution boundary input to the TraversalExecutionSummary contract.
	ExecutionBoundary string `json:"execution_boundary,omitempty"`
	// ObservationMode identifies whether the public boundary consumes scalar,
	// ordered-ID, or hydrated path values.
	ObservationMode string `json:"observation_mode,omitempty"`
	// Caps binds each guarded resource dimension to its enforced limit.
	Caps map[string]int64 `json:"caps"`
	// RuntimeOutcomeAvailable distinguishes executor evidence from a
	// translator prediction. When false, runtime-dependent facts stay unset.
	RuntimeOutcomeAvailable *bool `json:"runtime_outcome_available,omitempty"`
	// RuntimeBranch supplies the runtime branch input to the TraversalExecutionSummary contract.
	RuntimeBranch string `json:"runtime_branch"`
	// Overflow supplies the overflow input to the TraversalExecutionSummary contract.
	Overflow *bool `json:"overflow"`
	// FallbackExecuted supplies the fallback executed input to the TraversalExecutionSummary contract.
	FallbackExecuted *bool `json:"fallback_executed"`
	// FallbackIdentity identifies the fallback identity.
	FallbackIdentity string `json:"fallback_identity,omitempty"`
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
	RequiredFamilies []TraversalTelemetryFamily `json:"required_families"`
	// Counters supplies the counters input to the TraversalExecutionDiagnostic contract.
	Counters TraversalDiagnosticCounters `json:"counters"`
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
	// Ordinary supplies the ordinary input to the TraversalDiagnosticCounters contract.
	Ordinary *OrdinaryTraversalCounters `json:"ordinary,omitempty"`
	// Orientation supplies the orientation input to the TraversalDiagnosticCounters contract.
	Orientation *OrientationTraversalCounters `json:"orientation,omitempty"`
	// SuffixGuard records reverse-first fixed-suffix admission independently of
	// topology-scored orientation probes.
	SuffixGuard *SuffixGuardTraversalCounters `json:"suffix_guard,omitempty"`
	// ShortestPath identifies the filesystem shortest path.
	ShortestPath *ShortestPathTraversalCounters `json:"shortest_path,omitempty"`
	// AllShortestPaths identifies the filesystem all shortest paths.
	AllShortestPaths *AllShortestPathsTraversalCounters `json:"all_shortest_paths,omitempty"`
	// InlineASP supplies the inline asp input to the TraversalDiagnosticCounters contract.
	InlineASP *InlinePredecessorTraversalCounters `json:"inline_asp,omitempty"`
	// InlineShortestPath identifies the filesystem inline shortest path.
	InlineShortestPath *InlinePredecessorTraversalCounters `json:"inline_shortest_path,omitempty"`
	// InlineShortestDistance records guarded SP-I2 distance-only work.
	InlineShortestDistance *InlineDistanceTraversalCounters `json:"inline_shortest_distance,omitempty"`
	// Hydration supplies the hydration input to the TraversalDiagnosticCounters contract.
	Hydration *TraversalHydrationCounters `json:"hydration,omitempty"`
	// Workspace supplies the workspace input to the TraversalDiagnosticCounters contract.
	Workspace *TraversalWorkspaceCounters `json:"workspace,omitempty"`
}

// InlineDistanceTraversalCounters records the bounded reverse-physical
// distance relation and complementary candidate/fallback branch receipts.
type InlineDistanceTraversalCounters struct {
	StateRows              *int64 `json:"state_rows"`
	FrontierRows           *int64 `json:"frontier_rows"`
	OutputRows             *int64 `json:"output_rows"`
	CandidateMarkerRows    *int64 `json:"candidate_marker_rows"`
	FallbackMarkerRows     *int64 `json:"fallback_marker_rows"`
	CandidateBranchRows    *int64 `json:"candidate_branch_rows"`
	FallbackBranchRows     *int64 `json:"fallback_branch_rows"`
	CandidateExecutorLoops *int64 `json:"candidate_executor_loops"`
	FallbackExecutorLoops  *int64 `json:"fallback_executor_loops"`
}

// SuffixGuardTraversalCounters records the complete bounded relations and
// complementary branch markers exposed by suffix-reverse-guard-v1. Boolean
// overflow fields remain pointers so a measured false cannot be confused with
// missing evidence.
type SuffixGuardTraversalCounters struct {
	RootPresenceRows       *int64 `json:"root_presence_rows"`
	SuffixRows             *int64 `json:"suffix_rows"`
	DistinctBoundaryRows   *int64 `json:"distinct_boundary_rows"`
	StateRows              *int64 `json:"state_rows"`
	OutputRows             *int64 `json:"output_rows"`
	CandidateMarkerRows    *int64 `json:"candidate_marker_rows"`
	FallbackMarkerRows     *int64 `json:"fallback_marker_rows"`
	CandidateBranchRows    *int64 `json:"candidate_branch_rows"`
	FallbackBranchRows     *int64 `json:"fallback_branch_rows"`
	CandidateExecutorLoops *int64 `json:"candidate_executor_loops"`
	FallbackExecutorLoops  *int64 `json:"fallback_executor_loops"`
	SuffixOverflow         *bool  `json:"suffix_overflow"`
	StateOverflow          *bool  `json:"state_overflow"`
}

// InlinePredecessorTraversalCounters records the complete set of bounded
// relations and complementary branch markers exposed by an inline I1
// predecessor statement. ASP and canonical one-witness policies serialize
// into separate fields so their resource evidence cannot be interchanged.
type InlinePredecessorTraversalCounters struct {
	// DistanceRows records the number of distance rows.
	DistanceRows *int64 `json:"distance_rows"`
	// PredecessorRows records the number of predecessor rows.
	PredecessorRows *int64 `json:"predecessor_rows"`
	// EnumerationRows records the number of enumeration rows.
	EnumerationRows *int64 `json:"enumeration_rows"`
	// OutputPaths identifies the filesystem output paths.
	OutputPaths *int64 `json:"output_paths"`
	// OutputBytes supplies the output bytes input to the InlinePredecessorTraversalCounters contract.
	OutputBytes *int64 `json:"output_bytes"`
	// CandidateMarkerRows records the number of candidate marker rows.
	CandidateMarkerRows *int64 `json:"candidate_marker_rows"`
	// FallbackMarkerRows records the number of fallback marker rows.
	FallbackMarkerRows *int64 `json:"fallback_marker_rows"`
	// CandidateBranchRows records the number of candidate branch rows.
	CandidateBranchRows *int64 `json:"candidate_branch_rows"`
	// FallbackBranchRows records the number of fallback branch rows.
	FallbackBranchRows *int64 `json:"fallback_branch_rows"`
	// CandidateExecutorLoops supplies the candidate executor loops input to the InlinePredecessorTraversalCounters contract.
	CandidateExecutorLoops *int64 `json:"candidate_executor_loops"`
	// FallbackExecutorLoops supplies the fallback executor loops input to the InlinePredecessorTraversalCounters contract.
	FallbackExecutorLoops *int64 `json:"fallback_executor_loops"`
}

// InlineASPTraversalCounters preserves the source-level name used by existing
// ASP telemetry producers while sharing the exact bounded-relation schema.
type InlineASPTraversalCounters = InlinePredecessorTraversalCounters

// OrdinaryTraversalCounters records DFS or recursive-CTE discovery work.
type OrdinaryTraversalCounters struct {
	// Roots supplies the roots input to the OrdinaryTraversalCounters contract.
	Roots *int64 `json:"roots"`
	// EdgeCandidates supplies the edge candidates input to the OrdinaryTraversalCounters contract.
	EdgeCandidates *int64 `json:"edge_candidates"`
	// AdmittedStates supplies the admitted states input to the OrdinaryTraversalCounters contract.
	AdmittedStates *int64 `json:"admitted_states"`
	// RelationshipRepeatRejects supplies the relationship repeat rejects input to the OrdinaryTraversalCounters contract.
	RelationshipRepeatRejects *int64 `json:"relationship_repeat_rejects"`
	// RecursiveRows records the number of recursive rows.
	RecursiveRows *int64 `json:"recursive_rows"`
	// PeakState supplies the peak state input to the OrdinaryTraversalCounters contract.
	PeakState *int64 `json:"peak_state"`
	// EmittedTrails supplies the emitted trails input to the OrdinaryTraversalCounters contract.
	EmittedTrails *int64 `json:"emitted_trails"`
	// HydrationRows records the number of hydration rows.
	HydrationRows *int64 `json:"hydration_rows"`
}

// OrientationTraversalCounters records bounded policy probes and selected-branch work.
type OrientationTraversalCounters struct {
	// ForwardSeeds supplies the forward seeds input to the OrientationTraversalCounters contract.
	ForwardSeeds *int64 `json:"forward_seeds"`
	// ReverseSeeds supplies the reverse seeds input to the OrientationTraversalCounters contract.
	ReverseSeeds *int64 `json:"reverse_seeds"`
	// DuplicateSeeds supplies the duplicate seeds input to the OrientationTraversalCounters contract.
	DuplicateSeeds *int64 `json:"duplicate_seeds"`
	// SuffixRows records the number of suffix rows.
	SuffixRows *int64 `json:"suffix_rows"`
	// DistinctBoundaries supplies the distinct boundaries input to the OrientationTraversalCounters contract.
	DistinctBoundaries *int64 `json:"distinct_boundaries"`
	// TypedDirectionalDegreeSamples supplies the typed directional degree samples input to the OrientationTraversalCounters contract.
	TypedDirectionalDegreeSamples *int64 `json:"typed_directional_degree_samples"`
	// ForwardDegreeSamples supplies the forward degree samples input to the OrientationTraversalCounters contract.
	ForwardDegreeSamples *int64 `json:"forward_degree_samples"`
	// ReverseDegreeSamples supplies the reverse degree samples input to the OrientationTraversalCounters contract.
	ReverseDegreeSamples *int64 `json:"reverse_degree_samples"`
	// ShallowSurvivalRows records the number of shallow survival rows.
	ShallowSurvivalRows *int64 `json:"shallow_survival_rows"`
	// ShallowSurvival supplies the shallow survival input to the OrientationTraversalCounters contract.
	ShallowSurvival *float64 `json:"shallow_survival"`
	// ProbeRows records the number of probe rows.
	ProbeRows *int64 `json:"probe_rows"`
	// ProbeTimeNS supplies the probe time ns input to the OrientationTraversalCounters contract.
	ProbeTimeNS *int64 `json:"probe_time_ns"`
	// ProbeBufferHits supplies the probe buffer hits input to the OrientationTraversalCounters contract.
	ProbeBufferHits *int64 `json:"probe_buffer_hits"`
	// ProbeBufferReads supplies the probe buffer reads input to the OrientationTraversalCounters contract.
	ProbeBufferReads *int64 `json:"probe_buffer_reads"`
	// ForwardScore supplies the forward score input to the OrientationTraversalCounters contract.
	ForwardScore *float64 `json:"forward_score"`
	// ReverseScore supplies the reverse score input to the OrientationTraversalCounters contract.
	ReverseScore *float64 `json:"reverse_score"`
	// SelectedSide supplies the selected side input to the OrientationTraversalCounters contract.
	SelectedSide string `json:"selected_side"`
	// SentinelOverflow supplies the sentinel overflow input to the OrientationTraversalCounters contract.
	SentinelOverflow *bool `json:"sentinel_overflow"`
	// BranchLoops supplies the branch loops input to the OrientationTraversalCounters contract.
	BranchLoops *int64 `json:"branch_loops"`
}

// ShortestPathLevelCounters records one scheduler action and the two-sided frontier state it observed.
type ShortestPathLevelCounters struct {
	// SearchID identifies the search id.
	SearchID int64 `json:"search_id"`
	// ActionIndex supplies the action index input to the ShortestPathLevelCounters contract.
	ActionIndex int64 `json:"action_index"`
	// Side supplies the side input to the ShortestPathLevelCounters contract.
	Side string `json:"side"`
	// Action supplies the action input to the ShortestPathLevelCounters contract.
	Action string `json:"action"`
	// Depth supplies the depth input to the ShortestPathLevelCounters contract.
	Depth *int64 `json:"depth"`
	// FrontierRows records the number of frontier rows.
	FrontierRows *int64 `json:"frontier_rows"`
	// CandidateEdges supplies the candidate edges input to the ShortestPathLevelCounters contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the ShortestPathLevelCounters contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenRows records the number of seen rows.
	SeenRows *int64 `json:"seen_rows"`
	// QueueRows records the number of queue rows.
	QueueRows *int64 `json:"queue_rows"`
	// PredecessorRows records the number of predecessor rows.
	PredecessorRows *int64 `json:"predecessor_rows"`
	// MeetingCandidates supplies the meeting candidates input to the ShortestPathLevelCounters contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// Provenance names the invocation-local stage or executor metric that produced this level row.
	Provenance string `json:"provenance"`
}

// ShortestPathTraversalCounters records bidirectional scheduler, frontier, and witness work.
type ShortestPathTraversalCounters struct {
	// SchedulerActions supplies the scheduler actions input to the ShortestPathTraversalCounters contract.
	SchedulerActions *int64 `json:"scheduler_actions"`
	// Levels supplies the levels input to the ShortestPathTraversalCounters contract.
	Levels []ShortestPathLevelCounters `json:"levels"`
	// CandidateEdges supplies the candidate edges input to the ShortestPathTraversalCounters contract.
	CandidateEdges *int64 `json:"candidate_edges"`
	// DistinctNewNodes supplies the distinct new nodes input to the ShortestPathTraversalCounters contract.
	DistinctNewNodes *int64 `json:"distinct_new_nodes"`
	// SeenPeak supplies the seen peak input to the ShortestPathTraversalCounters contract.
	SeenPeak *int64 `json:"seen_peak"`
	// FrontierPeak supplies the frontier peak input to the ShortestPathTraversalCounters contract.
	FrontierPeak *int64 `json:"frontier_peak"`
	// QueuePeak supplies the queue peak input to the ShortestPathTraversalCounters contract.
	QueuePeak *int64 `json:"queue_peak"`
	// PredecessorPeak supplies the predecessor peak input to the ShortestPathTraversalCounters contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingCandidates supplies the meeting candidates input to the ShortestPathTraversalCounters contract.
	MeetingCandidates *int64 `json:"meeting_candidates"`
	// FrozenDistance supplies the frozen distance input to the ShortestPathTraversalCounters contract.
	FrozenDistance *int64 `json:"frozen_distance"`
	// WitnessRows records the number of witness rows.
	WitnessRows *int64 `json:"witness_rows"`
	// FallbackExecuted supplies the fallback executed input to the ShortestPathTraversalCounters contract.
	FallbackExecuted *bool `json:"fallback_executed"`
}

// AllShortestPathsTraversalCounters records SP search work plus predecessor and output enumeration work.
type AllShortestPathsTraversalCounters struct {
	// Search supplies the search input to the AllShortestPathsTraversalCounters contract.
	Search ShortestPathTraversalCounters `json:"search"`
	// SameDepthPredecessorAdditions supplies the same depth predecessor additions input to the AllShortestPathsTraversalCounters contract.
	SameDepthPredecessorAdditions *int64 `json:"same_depth_predecessor_additions"`
	// PredecessorPeak supplies the predecessor peak input to the AllShortestPathsTraversalCounters contract.
	PredecessorPeak *int64 `json:"predecessor_peak"`
	// MeetingNodes supplies the meeting nodes input to the AllShortestPathsTraversalCounters contract.
	MeetingNodes *int64 `json:"meeting_nodes"`
	// CutDepth supplies the cut depth input to the AllShortestPathsTraversalCounters contract.
	CutDepth *int64 `json:"cut_depth"`
	// PathCountEstimate supplies the path count estimate input to the AllShortestPathsTraversalCounters contract.
	PathCountEstimate *int64 `json:"path_count_estimate"`
	// PathCountSaturated supplies the path count saturated input to the AllShortestPathsTraversalCounters contract.
	PathCountSaturated *bool `json:"path_count_saturated"`
	// EnumeratedCandidates supplies the enumerated candidates input to the AllShortestPathsTraversalCounters contract.
	EnumeratedCandidates *int64 `json:"enumerated_candidates"`
	// DuplicateRejects supplies the duplicate rejects input to the AllShortestPathsTraversalCounters contract.
	DuplicateRejects *int64 `json:"duplicate_rejects"`
	// OutputPaths identifies the filesystem output paths.
	OutputPaths *int64 `json:"output_paths"`
	// OutputEdgeCells supplies the output edge cells input to the AllShortestPathsTraversalCounters contract.
	OutputEdgeCells *int64 `json:"output_edge_cells"`
	// OutputBytes supplies the output bytes input to the AllShortestPathsTraversalCounters contract.
	OutputBytes *int64 `json:"output_bytes"`
}

// TraversalHydrationCounters records post-discovery materialization separately from traversal work.
type TraversalHydrationCounters struct {
	// PathCount records the number of path count.
	PathCount *int64 `json:"path_count"`
	// NodeLookups supplies the node lookups input to the TraversalHydrationCounters contract.
	NodeLookups *int64 `json:"node_lookups"`
	// EdgeLookups supplies the edge lookups input to the TraversalHydrationCounters contract.
	EdgeLookups *int64 `json:"edge_lookups"`
	// Loops supplies the loops input to the TraversalHydrationCounters contract.
	Loops *int64 `json:"loops"`
	// Rows records the number of rows.
	Rows *int64 `json:"rows"`
	// TimeNS supplies the time ns input to the TraversalHydrationCounters contract.
	TimeNS *int64 `json:"time_ns"`
	// Bytes supplies the bytes input to the TraversalHydrationCounters contract.
	Bytes *int64 `json:"bytes"`
}

// TraversalWorkspaceCounters records measured high-water memory attributed to
// one diagnostic invocation and to all simultaneously active pool sessions.
type TraversalWorkspaceCounters struct {
	// SessionPeakBytes supplies the session peak bytes input to the TraversalWorkspaceCounters contract.
	SessionPeakBytes *int64 `json:"session_peak_bytes"`
	// PoolPeakBytes supplies the pool peak bytes input to the TraversalWorkspaceCounters contract.
	PoolPeakBytes *int64 `json:"pool_peak_bytes"`
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

// validateTraversalSummary validates traversal summary.
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

// validateTraversalDiagnostic validates traversal diagnostic.
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
		case TraversalTelemetryFamilySuffixGuard:
			validateSuffixGuardCounters(diagnostic.Counters.SuffixGuard, diagnostic.Provenance, problems)
		case TraversalTelemetryFamilySP:
			if diagnostic.Counters.InlineShortestDistance != nil {
				validateInlineDistanceCounters(diagnostic.Counters.InlineShortestDistance, diagnostic.Provenance, problems)
			} else if diagnostic.Counters.InlineShortestPath != nil {
				validateInlinePredecessorCounters("inline_shortest_path", diagnostic.Counters.InlineShortestPath, diagnostic.Provenance, problems)
			} else {
				validateShortestPathCounters("shortest_path", diagnostic.Counters.ShortestPath, diagnostic.Provenance, problems)
			}
		case TraversalTelemetryFamilyASP:
			if diagnostic.Counters.InlineASP != nil {
				validateInlinePredecessorCounters("inline_asp", diagnostic.Counters.InlineASP, diagnostic.Provenance, problems)
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
		TraversalTelemetryFamilySuffixGuard: diagnostic.Counters.SuffixGuard != nil,
		TraversalTelemetryFamilySP:          diagnostic.Counters.ShortestPath != nil || diagnostic.Counters.InlineShortestPath != nil || diagnostic.Counters.InlineShortestDistance != nil,
		TraversalTelemetryFamilyASP:         diagnostic.Counters.AllShortestPaths != nil || diagnostic.Counters.InlineASP != nil,
		TraversalTelemetryFamilyHydration:   diagnostic.Counters.Hydration != nil,
		TraversalTelemetryFamilyWorkspace:   diagnostic.Counters.Workspace != nil,
	} {
		if present && !slices.Contains(diagnostic.RequiredFamilies, family) {
			*problems = append(*problems, fmt.Sprintf("diagnostic counter family %q is present but not declared", family))
		}
	}
}

// validateSuffixGuardCounters validates the reverse-first guard's complete
// admission, branch, and sentinel evidence without requiring orientation-only
// degree samples or scores.
func validateSuffixGuardCounters(counters *SuffixGuardTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.suffix_guard is missing")
		return
	}
	requireCounters("suffix_guard", provenance, problems, map[string]*int64{
		"root_presence_rows":       counters.RootPresenceRows,
		"suffix_rows":              counters.SuffixRows,
		"distinct_boundary_rows":   counters.DistinctBoundaryRows,
		"state_rows":               counters.StateRows,
		"output_rows":              counters.OutputRows,
		"candidate_marker_rows":    counters.CandidateMarkerRows,
		"fallback_marker_rows":     counters.FallbackMarkerRows,
		"candidate_branch_rows":    counters.CandidateBranchRows,
		"fallback_branch_rows":     counters.FallbackBranchRows,
		"candidate_executor_loops": counters.CandidateExecutorLoops,
		"fallback_executor_loops":  counters.FallbackExecutorLoops,
	})
	requirePointerAndProvenance("suffix_guard.suffix_overflow", counters.SuffixOverflow, provenance, problems)
	requirePointerAndProvenance("suffix_guard.state_overflow", counters.StateOverflow, provenance, problems)
}

func validateInlineDistanceCounters(counters *InlineDistanceTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters.inline_shortest_distance is missing")
		return
	}
	requireCounters("inline_shortest_distance", provenance, problems, map[string]*int64{
		"state_rows": counters.StateRows, "frontier_rows": counters.FrontierRows, "output_rows": counters.OutputRows,
		"candidate_marker_rows": counters.CandidateMarkerRows, "fallback_marker_rows": counters.FallbackMarkerRows,
		"candidate_branch_rows": counters.CandidateBranchRows, "fallback_branch_rows": counters.FallbackBranchRows,
		"candidate_executor_loops": counters.CandidateExecutorLoops, "fallback_executor_loops": counters.FallbackExecutorLoops,
	})
}

// validateInlinePredecessorCounters validates inline predecessor counters.
func validateInlinePredecessorCounters(prefix string, counters *InlinePredecessorTraversalCounters, provenance map[string]string, problems *[]string) {
	if counters == nil {
		*problems = append(*problems, "diagnostic.counters."+prefix+" is missing")
		return
	}
	requireCounters(prefix, provenance, problems, map[string]*int64{
		"distance_rows": counters.DistanceRows, "predecessor_rows": counters.PredecessorRows,
		"enumeration_rows": counters.EnumerationRows, "output_paths": counters.OutputPaths,
		"output_bytes": counters.OutputBytes, "candidate_marker_rows": counters.CandidateMarkerRows,
		"fallback_marker_rows": counters.FallbackMarkerRows, "candidate_branch_rows": counters.CandidateBranchRows,
		"fallback_branch_rows": counters.FallbackBranchRows, "candidate_executor_loops": counters.CandidateExecutorLoops,
		"fallback_executor_loops": counters.FallbackExecutorLoops,
	})
}

// validateTraversalPlanReplay validates traversal plan replay.
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

// validateOrdinaryCounters validates ordinary counters.
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

// validateOrientationCounters validates orientation counters.
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

// validateShortestPathCounters validates shortest path counters.
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

// validateAllShortestPathsCounters validates all shortest paths counters.
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

// validateHydrationCounters validates hydration counters.
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

// validateWorkspaceCounters validates workspace counters.
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

// requireCounters supports benchmark evidence processing for require counters.
func requireCounters(prefix string, provenance map[string]string, problems *[]string, counters map[string]*int64) {
	for name, value := range counters {
		requirePointerAndProvenance(prefix+"."+name, value, provenance, problems)
	}
}

// requirePointerAndProvenance supports benchmark evidence processing for require pointer and provenance.
func requirePointerAndProvenance[T any](path string, value *T, provenance map[string]string, problems *[]string) {
	requirePointer("diagnostic.counters."+path, value, problems)
	requireProvenance("diagnostic.counters."+path, provenance[path], problems)
}

// requirePointer returns an addressable representation of require.
func requirePointer[T any](path string, value *T, problems *[]string) {
	if value == nil {
		*problems = append(*problems, path+" is missing")
	}
}

// requireText supports benchmark evidence processing for require text.
func requireText(path, value string, problems *[]string) {
	if strings.TrimSpace(value) == "" {
		*problems = append(*problems, path+" is missing")
	}
}

// requireProvenance supports benchmark evidence processing for require provenance.
func requireProvenance(path, value string, problems *[]string) {
	if strings.TrimSpace(value) == "" {
		*problems = append(*problems, path+" provenance is missing")
	}
}
