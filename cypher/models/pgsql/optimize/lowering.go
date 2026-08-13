package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

const (
	// LoweringProjectionPruning identifies removal of traversal fields that downstream clauses do not consume.
	LoweringProjectionPruning = "ProjectionPruning"

	// LoweringLatePathMaterialization identifies deferral of path hydration until a consumer requires it.
	LoweringLatePathMaterialization = "LatePathMaterialization"

	// LoweringExpandIntoDetection identifies traversal steps whose two endpoints are already bound.
	LoweringExpandIntoDetection = "ExpandIntoDetection"

	// LoweringTraversalDirection identifies selection of the lower-cost logical traversal direction.
	LoweringTraversalDirection = "TraversalDirectionSelection"

	// LoweringShortestPathStrategy identifies unidirectional or bidirectional shortest-path selection.
	LoweringShortestPathStrategy = "ShortestPathStrategySelection"

	// LoweringShortestPathFilter identifies materialization of reusable shortest-path endpoint filters.
	LoweringShortestPathFilter = "ShortestPathFilterMaterialization"

	// LoweringLimitPushdown identifies limits moved into a traversal or shortest-path harness.
	LoweringLimitPushdown = "LimitPushdown"

	// LoweringExpansionSuffixPushdown identifies fixed-suffix predicates moved closer to variable expansion.
	LoweringExpansionSuffixPushdown = "ExpansionSuffixPushdown"

	// LoweringPredicatePlacement identifies attachment of predicates to the earliest safe traversal step.
	LoweringPredicatePlacement = "PredicatePlacement"

	// LoweringCountStoreFastPath identifies count queries satisfied directly from graph statistics.
	LoweringCountStoreFastPath = "CountStoreFastPath"

	// LoweringCollectIDMembership identifies membership checks rewritten over collected scalar IDs.
	LoweringCollectIDMembership = "CollectIDMembership"

	// LoweringAggregateTraversalCount identifies traversal counts lowered without materializing result rows.
	LoweringAggregateTraversalCount = "AggregateTraversalCount"

	// LoweringExactRangeExpansion identifies short exact ranges expanded into fixed traversal steps.
	LoweringExactRangeExpansion = "ExactRangeExpansion"

	// LoweringPathRelationshipPredicate identifies relationship quantifiers attached to path state.
	LoweringPathRelationshipPredicate = "PathRelationshipPredicate"

	// LoweringFieldRequirements identifies analysis that records which representation each binding consumer needs.
	LoweringFieldRequirements = "FieldRequirements"

	// LoweringShortestPathExecutor identifies selection of a physical shortest-path executor.
	LoweringShortestPathExecutor = "ShortestPathExecutorDecision"

	// LoweringExpansionSearchStrategy identifies selection of a physical variable-expansion search strategy.
	LoweringExpansionSearchStrategy = "ExpansionSearchStrategyDecision"

	// LoweringEndpointResolution identifies planned bounded endpoint-resolution analysis.
	LoweringEndpointResolution = "EndpointResolutionDecision"

	// LoweringTraversalPredicateClassification identifies planned traversal-predicate locality analysis.
	LoweringTraversalPredicateClassification = "TraversalPredicateClassificationDecision"
)

type LoweringDecision struct {
	Name string `json:"name"`
}

type PatternTarget struct {
	QueryPartIndex int  `json:"query_part_index"`
	ClauseIndex    int  `json:"clause_index"`
	PatternIndex   int  `json:"pattern_index"`
	Predicate      bool `json:"predicate,omitempty"`
	PredicateIndex int  `json:"predicate_index,omitempty"`
}

func (s PatternTarget) TraversalStep(stepIndex int) TraversalStepTarget {
	return TraversalStepTarget{
		QueryPartIndex: s.QueryPartIndex,
		ClauseIndex:    s.ClauseIndex,
		PatternIndex:   s.PatternIndex,
		Predicate:      s.Predicate,
		PredicateIndex: s.PredicateIndex,
		StepIndex:      stepIndex,
	}
}

type TraversalStepTarget struct {
	QueryPartIndex int  `json:"query_part_index"`
	ClauseIndex    int  `json:"clause_index"`
	PatternIndex   int  `json:"pattern_index"`
	Predicate      bool `json:"predicate,omitempty"`
	PredicateIndex int  `json:"predicate_index,omitempty"`
	StepIndex      int  `json:"step_index"`
}

type QuantifierTarget struct {
	QueryPartIndex  int `json:"query_part_index"`
	QuantifierIndex int `json:"quantifier_index"`
}

type ProjectionPruningDecision struct {
	Target                   TraversalStepTarget `json:"target"`
	ReferencedSymbols        []string            `json:"referenced_symbols,omitempty"`
	PatternBindingReferenced bool                `json:"pattern_binding_referenced,omitempty"`
	OmitLeftNode             bool                `json:"omit_left_node,omitempty"`
	OmitRelationship         bool                `json:"omit_relationship,omitempty"`
	OmitRightNode            bool                `json:"omit_right_node,omitempty"`
	OmitPathBinding          bool                `json:"omit_path_binding,omitempty"`
}

type LatePathMaterializationMode string

const (
	// LatePathMaterializationPathEdgeID carries a path as ordered edge IDs until hydration.
	LatePathMaterializationPathEdgeID LatePathMaterializationMode = "path_edge_id"

	// LatePathMaterializationExpansionPath carries recursive expansion path state until hydration.
	LatePathMaterializationExpansionPath LatePathMaterializationMode = "expansion_path"

	// LatePathMaterializationEdgeComposite defers hydration of an edge composite.
	LatePathMaterializationEdgeComposite LatePathMaterializationMode = "edge_composite"
)

type LatePathMaterializationDecision struct {
	Target TraversalStepTarget         `json:"target"`
	Mode   LatePathMaterializationMode `json:"mode"`
}

type ExpandIntoDecision struct {
	Target TraversalStepTarget `json:"target"`
}

type TraversalDirectionDecision struct {
	Target TraversalStepTarget `json:"target"`
	Flip   bool                `json:"flip,omitempty"`
	Reason string              `json:"reason,omitempty"`
}

type ShortestPathStrategy string

const (
	// ShortestPathStrategyBidirectional searches simultaneously from both endpoints.
	ShortestPathStrategyBidirectional ShortestPathStrategy = "bidirectional"

	// ShortestPathStrategyUnidirectional searches from one endpoint toward the other.
	ShortestPathStrategyUnidirectional ShortestPathStrategy = "unidirectional"
)

type ShortestPathStrategyDecision struct {
	Target   TraversalStepTarget  `json:"target"`
	Strategy ShortestPathStrategy `json:"strategy"`
	Reason   string               `json:"reason,omitempty"`
}

type ShortestPathExecutor string

const (
	// ShortestPathExecutorIncumbentWorkspace selects the existing workspace-table executor.
	ShortestPathExecutorIncumbentWorkspace ShortestPathExecutor = "SP-S0"

	// ShortestPathExecutorS1ArrayBFS selects breadth-first search with path state held in arrays.
	ShortestPathExecutorS1ArrayBFS ShortestPathExecutor = "SP-S1"

	// ShortestPathExecutorS2TraceRelation selects breadth-first search backed by a trace relation.
	ShortestPathExecutorS2TraceRelation ShortestPathExecutor = "SP-S2"

	// ShortestPathExecutorS3Unidirectional selects the unidirectional scalar-distance executor.
	ShortestPathExecutorS3Unidirectional ShortestPathExecutor = "SP-S3-U-D"

	// ShortestPathExecutorS3EdgeM0 selects unidirectional edge-trail search with deferred path materialization.
	ShortestPathExecutorS3EdgeM0 ShortestPathExecutor = "SP-S3-U-E+MAT-M0"

	// ShortestPathExecutorS0Direct selects the direct preflight executor with workspace fallback.
	ShortestPathExecutorS0Direct ShortestPathExecutor = "SP-S0-DIRECT"

	// ShortestPathExecutorS4CanonicalDistance selects canonical compact search for distance-only observations.
	ShortestPathExecutorS4CanonicalDistance ShortestPathExecutor = "SP-S4-C-D"

	// ShortestPathExecutorS4CanonicalWitness selects canonical compact search with witness materialization.
	ShortestPathExecutorS4CanonicalWitness ShortestPathExecutor = "SP-S4-C-WE+MAT-M0"

	// ShortestPathExecutorASPA1DAG selects all-shortest-path enumeration from a predecessor DAG.
	ShortestPathExecutorASPA1DAG ShortestPathExecutor = "ASP-A1-DAG"

	// ShortestPathExecutorI1CanonicalDistance selects an inline recursive SQL
	// distance search. The distinct identity prevents evidence collected at an
	// inline statement boundary from being attributed to a helper function.
	ShortestPathExecutorI1CanonicalDistance ShortestPathExecutor = "SP-I1-C-D"

	// ShortestPathExecutorI1CanonicalWitness selects inline recursive SQL with
	// ordered edge-ID witness state and late M0 path materialization.
	ShortestPathExecutorI1CanonicalWitness ShortestPathExecutor = "SP-I1-U-E+MAT-M0"

	// ShortestPathExecutorI1CanonicalPredecessorWitness selects guarded inline
	// minimum-distance/predecessor discovery, one deterministic witness, and an
	// exact compact S4 fallback. It is intentionally distinct from the legacy
	// unguarded relationship-trail I1 identity above.
	ShortestPathExecutorI1CanonicalPredecessorWitness ShortestPathExecutor = "SP-I1-C-WE+MAT-M0"

	// ShortestPathExecutorASPI1DAG selects inline predecessor-DAG discovery and
	// late M0 materialization for all shortest paths.
	ShortestPathExecutorASPI1DAG ShortestPathExecutor = "ASP-I1-U-DAG+MAT-M0"

	// ShortestPathExecutorB1AlternatingNodeDistance reserves compact bidirectional
	// distance search with strict node-at-a-time alternation.
	ShortestPathExecutorB1AlternatingNodeDistance ShortestPathExecutor = "SP-B1-C-ALT-NODE-D"

	// ShortestPathExecutorB1AlternatingNodeWitness reserves compact bidirectional
	// witness search with strict node-at-a-time alternation and deferred materialization.
	ShortestPathExecutorB1AlternatingNodeWitness ShortestPathExecutor = "SP-B1-C-ALT-NODE-WE+MAT-M0"

	// ShortestPathExecutorB2SmallerCurrentLevelDistance reserves compact bidirectional
	// distance search that expands the smaller current level.
	ShortestPathExecutorB2SmallerCurrentLevelDistance ShortestPathExecutor = "SP-B2-C-MIN-LEVEL-D"

	// ShortestPathExecutorB2SmallerCurrentLevelWitness reserves compact bidirectional
	// witness search that expands the smaller current level and defers materialization.
	ShortestPathExecutorB2SmallerCurrentLevelWitness ShortestPathExecutor = "SP-B2-C-MIN-LEVEL-WE+MAT-M0"

	// ShortestPathExecutorASPB1AlternatingNodeDAG reserves all-shortest-path DAG
	// enumeration with strict node-at-a-time alternation.
	ShortestPathExecutorASPB1AlternatingNodeDAG ShortestPathExecutor = "ASP-B1-DAG-ALT-NODE"

	// ShortestPathExecutorASPB2SmallerCurrentLevelDAG reserves all-shortest-path DAG
	// enumeration that expands the smaller current level.
	ShortestPathExecutorASPB2SmallerCurrentLevelDAG ShortestPathExecutor = "ASP-B2-DAG-MIN-LEVEL"
)

// ShortestPathScheduler identifies the frontier scheduling policy used by a
// shortest-path executor independently of its result-observation contract.
type ShortestPathScheduler string

const (
	// ShortestPathSchedulerSingleEndedLevel expands one complete level from a single frontier.
	ShortestPathSchedulerSingleEndedLevel ShortestPathScheduler = "single_ended_level"

	// ShortestPathSchedulerStrictAlternatingNode alternates one node expansion from each frontier.
	ShortestPathSchedulerStrictAlternatingNode ShortestPathScheduler = "strict_alternating_node"

	// ShortestPathSchedulerSmallerCurrentLevel expands the smaller of the two current frontier levels.
	ShortestPathSchedulerSmallerCurrentLevel ShortestPathScheduler = "smaller_current_level"
)

// Scheduler reports the stable frontier scheduler associated with this executor.
func (s ShortestPathExecutor) Scheduler() ShortestPathScheduler {
	switch s {
	case ShortestPathExecutorS3Unidirectional,
		ShortestPathExecutorS3EdgeM0,
		ShortestPathExecutorS4CanonicalDistance,
		ShortestPathExecutorS4CanonicalWitness,
		ShortestPathExecutorASPA1DAG,
		ShortestPathExecutorI1CanonicalDistance,
		ShortestPathExecutorI1CanonicalWitness,
		ShortestPathExecutorI1CanonicalPredecessorWitness,
		ShortestPathExecutorASPI1DAG:
		return ShortestPathSchedulerSingleEndedLevel
	case ShortestPathExecutorB1AlternatingNodeDistance,
		ShortestPathExecutorB1AlternatingNodeWitness,
		ShortestPathExecutorASPB1AlternatingNodeDAG:
		return ShortestPathSchedulerStrictAlternatingNode
	case ShortestPathExecutorB2SmallerCurrentLevelDistance,
		ShortestPathExecutorB2SmallerCurrentLevelWitness,
		ShortestPathExecutorASPB2SmallerCurrentLevelDAG:
		return ShortestPathSchedulerSmallerCurrentLevel
	default:
		return ""
	}
}

// ExecutionBoundary reports the SQL boundary represented by the executor
// identity. Benchmark and promotion artifacts must match this value.
func (s ShortestPathExecutor) ExecutionBoundary() string {
	switch s {
	case ShortestPathExecutorS3Unidirectional,
		ShortestPathExecutorS3EdgeM0,
		ShortestPathExecutorI1CanonicalDistance,
		ShortestPathExecutorI1CanonicalWitness,
		ShortestPathExecutorI1CanonicalPredecessorWitness,
		ShortestPathExecutorASPI1DAG:
		return "inline_statement"
	default:
		return "stored_helper"
	}
}

type ShortestPathObservationMode string

const (
	// ShortestPathObservationDistance indicates that only shortest-path length is consumed.
	ShortestPathObservationDistance ShortestPathObservationMode = "distance"

	// ShortestPathObservationOnePath indicates that one shortest-path witness is consumed.
	ShortestPathObservationOnePath ShortestPathObservationMode = "one_path"

	// ShortestPathObservationAllPaths indicates that every shortest-path witness is consumed.
	ShortestPathObservationAllPaths ShortestPathObservationMode = "all_paths"

	// ShortestPathObservationUnknown indicates that analysis could not classify downstream path use.
	ShortestPathObservationUnknown ShortestPathObservationMode = "unknown"
)

const (
	// ShortestPathFallbackAllShortestPaths records an all-shortest-path query lacking singleton endpoints required by specialized execution.
	ShortestPathFallbackAllShortestPaths = "all_shortest_paths"

	// ShortestPathFallbackCorrelatedEndpoints rejects endpoint sources not proven uncorrelated, such as UNWIND or later query parts.
	ShortestPathFallbackCorrelatedEndpoints = "correlated_endpoints"

	// ShortestPathFallbackMultipleEndpointPairs rejects specialized execution when additional row sources prevent proving one endpoint pair.
	ShortestPathFallbackMultipleEndpointPairs = "multiple_endpoint_pairs"

	// ShortestPathFallbackNonSingletonID rejects an endpoint whose ID is not statically singleton.
	ShortestPathFallbackNonSingletonID = "non_singleton_id"

	// ShortestPathFallbackMultipleIDEqualities rejects an endpoint constrained by competing ID equalities.
	ShortestPathFallbackMultipleIDEqualities = "multiple_id_equalities"

	// ShortestPathFallbackPathPredicate rejects a predicate that observes the materialized path.
	ShortestPathFallbackPathPredicate = "path_predicate"

	// ShortestPathFallbackRelationshipPredicate rejects a predicate on the traversed relationship.
	ShortestPathFallbackRelationshipPredicate = "relationship_predicate"

	// ShortestPathFallbackRelationshipVariable rejects an observed relationship binding.
	ShortestPathFallbackRelationshipVariable = "relationship_variable"

	// ShortestPathFallbackDirectionless rejects a directionless shortest-path expansion.
	ShortestPathFallbackDirectionless = "directionless"

	// ShortestPathFallbackOptionalMatch rejects shortest-path work under OPTIONAL MATCH semantics.
	ShortestPathFallbackOptionalMatch = "optional_match"

	// ShortestPathFallbackUnsupportedDepth rejects a depth range unsupported by the candidate executor.
	ShortestPathFallbackUnsupportedDepth = "unsupported_depth"

	// ShortestPathFallbackMutation rejects specialized execution for a statement containing updates.
	ShortestPathFallbackMutation = "mutation"

	// ShortestPathFallbackMultiplePathCalls rejects statements containing more than one shortest-path pattern.
	ShortestPathFallbackMultiplePathCalls = "multiple_path_calls"

	// ShortestPathFallbackDeepInboundUnqualified rejects an unqualified deep inbound traversal.
	ShortestPathFallbackDeepInboundUnqualified = "deep_inbound_unqualified"

	// ShortestPathFallbackNonSingleKindPathState rejects compact path state without one relationship kind.
	ShortestPathFallbackNonSingleKindPathState = "non_single_kind_path_state_unqualified"

	// ShortestPathFallbackTournamentUnqualified records that no experimental candidate won qualification.
	ShortestPathFallbackTournamentUnqualified = "tournament_unqualified"
)

type ShortestPathPhysicalExpansion string

const (
	// ShortestPathPhysicalExpansionStartID joins recursive expansion through each edge's start ID.
	ShortestPathPhysicalExpansionStartID ShortestPathPhysicalExpansion = "start_id"

	// ShortestPathPhysicalExpansionEndID joins recursive expansion through each edge's end ID.
	ShortestPathPhysicalExpansionEndID ShortestPathPhysicalExpansion = "end_id"
)

type ShortestPathTopologyClassification string

const (
	// ShortestPathTopologyPhysicalOutbound classifies traversal aligned with stored edge direction.
	ShortestPathTopologyPhysicalOutbound ShortestPathTopologyClassification = "physical_outbound"

	// ShortestPathTopologyPhysicalInboundShallow classifies a shallow traversal against stored edge direction.
	ShortestPathTopologyPhysicalInboundShallow ShortestPathTopologyClassification = "physical_inbound_shallow"

	// ShortestPathTopologyPhysicalInboundDeep classifies a deep traversal against stored edge direction.
	ShortestPathTopologyPhysicalInboundDeep ShortestPathTopologyClassification = "physical_inbound_deep"

	// ShortestPathTopologyDirectionless classifies traversal that may follow either stored direction.
	ShortestPathTopologyDirectionless ShortestPathTopologyClassification = "directionless"
)

// ShortestPathEligibilityFact records one named qualification check for an executor candidate.
type ShortestPathEligibilityFact struct {
	// Name identifies the qualification check.
	Name string `json:"name"`
	// Eligible reports whether the candidate passed the named check.
	Eligible bool `json:"eligible"`
}

// ShortestPathExecutorDecision records either a qualified static executor or
// the incumbent fallback, keeping every eligibility and fallback fact visible.
type ShortestPathExecutorDecision struct {
	// Target locates the traversal step governed by this decision.
	Target TraversalStepTarget `json:"target"`
	// Family names the executor-selection family that produced the decision.
	Family string `json:"family"`
	// PlannedCandidates lists the executors considered in preference order.
	PlannedCandidates []ShortestPathExecutor `json:"planned_candidates"`
	// SelectedExecutor is the executor chosen after qualification.
	SelectedExecutor ShortestPathExecutor `json:"selected_executor"`
	// ExecutionBoundary distinguishes inline statement SQL from stored helper
	// execution. Promotion evidence must match this boundary exactly.
	ExecutionBoundary string `json:"execution_boundary"`
	// Scheduler identifies the selected executor's frontier scheduling policy.
	Scheduler ShortestPathScheduler `json:"scheduler,omitempty"`
	// ObservationMode describes how downstream clauses consume the shortest path.
	ObservationMode ShortestPathObservationMode `json:"observation_mode"`
	// Direction is the logical direction of the traversal.
	Direction graph.Direction `json:"direction"`
	// PhysicalExpansion identifies which stored edge endpoint advances the search.
	PhysicalExpansion ShortestPathPhysicalExpansion `json:"physical_expansion"`
	// RelationshipKindCount is the number of statically resolved relationship kinds.
	RelationshipKindCount int `json:"relationship_kind_count"`
	// UntypedRelationship reports whether the pattern omitted relationship kinds.
	UntypedRelationship bool `json:"untyped_relationship"`
	// TopologyClassification summarizes logical direction, physical direction, and depth.
	TopologyClassification ShortestPathTopologyClassification `json:"topology_classification"`
	// Eligibility records each qualification check and its result.
	Eligibility []ShortestPathEligibilityFact `json:"eligibility"`
	// StructurallyEligible reports whether the query shape can use the candidate executor.
	StructurallyEligible bool `json:"structurally_eligible"`
	// StaticallyEligible reports whether known literals and kinds satisfy executor constraints.
	StaticallyEligible bool `json:"statically_eligible"`
	// MinimumDepth is the inclusive lower traversal-depth bound.
	MinimumDepth int64 `json:"minimum_depth"`
	// MaximumDepth is the inclusive upper traversal-depth bound.
	MaximumDepth int64 `json:"maximum_depth"`
	// StateLimit caps state admitted by bounded experimental executors.
	StateLimit int64 `json:"state_limit,omitempty"`
	// FrontierLimit caps current and queued frontier rows independently of seen state.
	FrontierLimit int64 `json:"frontier_limit,omitempty"`
	// PredecessorLimit caps retained witness predecessor rows independently of discovery state.
	PredecessorLimit int64 `json:"predecessor_limit,omitempty"`
	// EnumerationLimit caps distinct ordered all-shortest-path arrays before exact fallback.
	EnumerationLimit int64 `json:"enumeration_limit,omitempty"`
	// OutputBytesLimit caps staged all-shortest-path array bytes before exact fallback.
	OutputBytesLimit int64 `json:"output_bytes_limit,omitempty"`
	// SelectorVersion identifies the policy version that ranked the candidates.
	SelectorVersion string `json:"selector_version"`
	// SelectionMode records whether selection was automatic or forced by tooling.
	SelectionMode string `json:"selection_mode"`
	// FallbackExecutor is used when the preferred candidate cannot be applied.
	FallbackExecutor ShortestPathExecutor `json:"fallback_executor"`
	// FallbackReason explains why the preferred candidate was not selected.
	FallbackReason string `json:"fallback_reason"`
	// ExperimentalWinner reports whether an experimental candidate beat the incumbent.
	ExperimentalWinner bool `json:"experimental_winner,omitempty"`
}

type ShortestPathFilterMode string

const (
	// ShortestPathFilterTerminal materializes candidate terminal IDs independently of roots.
	ShortestPathFilterTerminal ShortestPathFilterMode = "terminal"

	// ShortestPathFilterEndpointPair materializes admissible root-terminal ID pairs.
	ShortestPathFilterEndpointPair ShortestPathFilterMode = "endpoint_pair"
)

type ShortestPathFilterDecision struct {
	Target TraversalStepTarget    `json:"target"`
	Mode   ShortestPathFilterMode `json:"mode"`
	Reason string                 `json:"reason,omitempty"`
}

type LimitPushdownMode string

const (
	// LimitPushdownTraversalCTE applies a row limit inside an ordinary traversal CTE.
	LimitPushdownTraversalCTE LimitPushdownMode = "traversal_cte"

	// LimitPushdownShortestPathHarness applies a row limit inside a shortest-path harness.
	LimitPushdownShortestPathHarness LimitPushdownMode = "shortest_path_harness"
)

type LimitPushdownDecision struct {
	Target TraversalStepTarget `json:"target"`
	Mode   LimitPushdownMode   `json:"mode"`
}

// ExpansionSuffixPushdownDecision describes a fixed traversal suffix evaluated for supplemental search.
type ExpansionSuffixPushdownDecision struct {
	// Target locates the variable expansion followed by the fixed suffix.
	Target TraversalStepTarget `json:"target"`
	// SuffixLength is the number of fixed traversal steps eligible for pushdown.
	SuffixLength int `json:"suffix_length"`
	// SuffixStartStep identifies the first fixed traversal step in the suffix.
	SuffixStartStep int `json:"suffix_start_step"`
	// SuffixEndStep identifies the final fixed traversal step in the suffix.
	SuffixEndStep int `json:"suffix_end_step"`
	// ApplySupplemental reports whether translation should emit the supplemental suffix-search branch.
	ApplySupplemental bool `json:"apply_supplemental"`
	// Reason explains why supplemental suffix search was enabled or withheld.
	Reason string `json:"reason,omitempty"`
	// PredicateAttachments lists predicates assigned to scopes within the fixed suffix.
	PredicateAttachments []PredicateAttachment `json:"predicate_attachments,omitempty"`
}

type ExpansionSearchStrategy string

const (
	// ExpansionSearchStepwiseForward selects the incumbent left-to-right expansion plan.
	ExpansionSearchStepwiseForward ExpansionSearchStrategy = "EXPANSION-STEPWISE-FORWARD"

	// ExpansionSearchLateHydratedForward selects forward search with deferred entity hydration.
	ExpansionSearchLateHydratedForward ExpansionSearchStrategy = "EXPANSION-LATE-HYDRATED-FORWARD"

	// ExpansionSearchFactoredSuffixForward selects forward search with a factored fixed suffix.
	ExpansionSearchFactoredSuffixForward ExpansionSearchStrategy = "EXPANSION-FACTORED-SUFFIX-FORWARD"

	// ExpansionSearchSuffixSeededReverse selects reverse probing seeded from a selective fixed suffix.
	ExpansionSearchSuffixSeededReverse ExpansionSearchStrategy = "EXPANSION-SUFFIX-SEEDED-REVERSE"

	// ExpansionSearchEndpointSeededReverse selects reverse probing seeded from selective terminal endpoints.
	ExpansionSearchEndpointSeededReverse ExpansionSearchStrategy = "EXPANSION-ENDPOINT-SEEDED-REVERSE"

	// ExpansionSearchBackwardViabilityForward selects forward expansion gated by backward reachability.
	ExpansionSearchBackwardViabilityForward ExpansionSearchStrategy = "EXPANSION-BACKWARD-VIABILITY-FORWARD"
)

// ExpansionSearchPolicy identifies a runtime policy independently of the
// expansion arm that the policy may execute.
type ExpansionSearchPolicy string

const (
	// ExpansionSearchPolicyEndpointGuardV1 identifies the shipped endpoint and
	// reverse-state sentinel policy. It is distinct from topology orientation,
	// which requires root, suffix, and directional-degree probes.
	ExpansionSearchPolicyEndpointGuardV1 ExpansionSearchPolicy = "endpoint-state-guard-v1"

	// ExpansionSearchPolicyOrientationProbeV1 selects an ordinary-expansion
	// orientation from bounded, same-statement topology probes.
	ExpansionSearchPolicyOrientationProbeV1 ExpansionSearchPolicy = "orientation-probe-v1"

	// ExpansionSearchPolicyOrientationProbeV2 selects an ordinary-expansion
	// orientation using depth-weighted forward work and the same bounded,
	// same-statement topology probes as v1.
	ExpansionSearchPolicyOrientationProbeV2 ExpansionSearchPolicy = "orientation-probe-v2"

	// ExpansionSearchOrientationRootRowLimit caps complete forward-root evidence
	// for the initial fixed-suffix orientation tournament.
	ExpansionSearchOrientationRootRowLimit int64 = 512

	// ExpansionSearchOrientationReverseSeedRowLimit caps complete fixed-suffix
	// row evidence while preserving duplicate suffix paths.
	ExpansionSearchOrientationReverseSeedRowLimit int64 = 512

	// ExpansionSearchOrientationDirectionalDegreeRowLimit caps each typed
	// directional adjacency probe independently.
	ExpansionSearchOrientationDirectionalDegreeRowLimit int64 = 16_384

	// ExpansionSearchOrientationStateLimit caps admitted reverse recursive state.
	ExpansionSearchOrientationStateLimit int64 = 4_096

	// ExpansionSearchOrientationReverseScoreMultiplier is the reverse side of
	// orientation-probe-v1's strict 3/4 hysteresis comparison.
	ExpansionSearchOrientationReverseScoreMultiplier int64 = 4

	// ExpansionSearchOrientationForwardScoreMultiplier is the incumbent side
	// of orientation-probe-v1's strict 3/4 hysteresis comparison.
	ExpansionSearchOrientationForwardScoreMultiplier int64 = 3

	// ExpansionSearchOrientationV2ReverseScoreMultiplier is the reverse side
	// of orientation-probe-v2's strict 3/4 hysteresis comparison.
	ExpansionSearchOrientationV2ReverseScoreMultiplier int64 = 4

	// ExpansionSearchOrientationV2ForwardScoreMultiplier is the incumbent side
	// of orientation-probe-v2's strict 3/4 hysteresis comparison.
	ExpansionSearchOrientationV2ForwardScoreMultiplier int64 = 3

	// ExpansionSearchExecutionBoundaryInlineStatement identifies one emitted
	// expansion traversal arm in the translated statement.
	ExpansionSearchExecutionBoundaryInlineStatement = "inline_statement"

	// ExpansionSearchExecutionBoundaryGuardedDualArm identifies a
	// same-statement expansion policy with exact candidate and fallback arms.
	ExpansionSearchExecutionBoundaryGuardedDualArm = "guarded_dual_arm"
)

// ExpansionSearchProbeCaps records the maximum complete evidence admitted by
// an orientation policy. SQL probes use cap+1 sentinels to detect overflow.
type ExpansionSearchProbeCaps struct {
	// RootRowLimit caps forward-root evidence.
	RootRowLimit int64 `json:"root_row_limit,omitempty"`
	// ReverseSeedRowLimit caps terminal or fixed-suffix seed evidence.
	ReverseSeedRowLimit int64 `json:"reverse_seed_row_limit,omitempty"`
	// DirectionalDegreeRowLimit caps typed first-hop adjacency evidence.
	DirectionalDegreeRowLimit int64 `json:"directional_degree_row_limit,omitempty"`
	// SurvivalRowLimit caps optional one-level survival evidence.
	SurvivalRowLimit int64 `json:"survival_row_limit,omitempty"`
}

// ExpansionSearchAdmission records the exact gate and fallback for a
// specialized orientation arm.
type ExpansionSearchAdmission struct {
	// StateLimit caps specialized search state before incumbent fallback.
	StateLimit int64 `json:"state_limit,omitempty"`
	// RequiresCompleteProbes requires every candidate input probe to remain at
	// or below its declared cap before specialized rows may be exposed.
	RequiresCompleteProbes bool `json:"requires_complete_probes,omitempty"`
	// FallbackStrategy names the exact incumbent used when admission fails.
	FallbackStrategy ExpansionSearchStrategy `json:"fallback_strategy,omitempty"`
}

type ExpansionSearchObservationMode string

const (
	// ExpansionSearchObservationEndpointIDs indicates that downstream clauses consume only endpoint IDs.
	ExpansionSearchObservationEndpointIDs ExpansionSearchObservationMode = "endpoint_ids"

	// ExpansionSearchObservationOrderedPathIDs indicates that downstream clauses consume ordered path IDs.
	ExpansionSearchObservationOrderedPathIDs ExpansionSearchObservationMode = "ordered_path_ids"

	// ExpansionSearchObservationFullPath indicates that downstream clauses consume hydrated path values.
	ExpansionSearchObservationFullPath ExpansionSearchObservationMode = "full_path"

	// ExpansionSearchObservationUnsupported indicates an observation pattern unsupported by specialized search.
	ExpansionSearchObservationUnsupported ExpansionSearchObservationMode = "unsupported"
)

// ExpansionSearchEligibilityFact records one named qualification check for a search strategy.
type ExpansionSearchEligibilityFact struct {
	// Name identifies the qualification check.
	Name string `json:"name"`
	// Eligible reports whether the strategy passed the named check.
	Eligible bool `json:"eligible"`
}

const (
	// ExpansionSearchFallbackNoFixedSuffix rejects a strategy that requires a fixed suffix when none exists.
	ExpansionSearchFallbackNoFixedSuffix = "no_fixed_suffix"

	// ExpansionSearchFallbackSuffixTooShort rejects a fixed suffix below the strategy's minimum length.
	ExpansionSearchFallbackSuffixTooShort = "suffix_too_short"

	// ExpansionSearchFallbackOptionalMatch rejects a rewrite that would alter OPTIONAL MATCH behavior.
	ExpansionSearchFallbackOptionalMatch = "optional_match"

	// ExpansionSearchFallbackShortestPath rejects ordinary-expansion strategies for shortestPath patterns.
	ExpansionSearchFallbackShortestPath = "shortest_path"

	// ExpansionSearchFallbackAllShortestPaths rejects ordinary-expansion strategies for allShortestPaths patterns.
	ExpansionSearchFallbackAllShortestPaths = "all_shortest_paths"

	// ExpansionSearchFallbackDirectionlessExpansion rejects a directionless variable expansion.
	ExpansionSearchFallbackDirectionlessExpansion = "directionless_expansion"

	// ExpansionSearchFallbackDirectionlessSuffix rejects a directionless edge in the fixed suffix.
	ExpansionSearchFallbackDirectionlessSuffix = "directionless_suffix"

	// ExpansionSearchFallbackUnboundedDepth rejects an expansion without a finite maximum depth.
	ExpansionSearchFallbackUnboundedDepth = "unbounded_depth"

	// ExpansionSearchFallbackUnsupportedDepth rejects a depth range the candidate cannot preserve.
	ExpansionSearchFallbackUnsupportedDepth = "unsupported_depth"

	// ExpansionSearchFallbackMultipleVariableExpansions rejects regions containing more than one variable expansion.
	ExpansionSearchFallbackMultipleVariableExpansions = "multiple_variable_expansions"

	// ExpansionSearchFallbackCorrelatedSuffix rejects a fixed suffix that reuses an outer binding.
	ExpansionSearchFallbackCorrelatedSuffix = "correlated_suffix"

	// ExpansionSearchFallbackCrossRegionPredicate rejects predicates spanning the variable and fixed regions.
	ExpansionSearchFallbackCrossRegionPredicate = "cross_region_predicate"

	// ExpansionSearchFallbackPathDependentPredicate rejects predicates that depend on accumulated path state.
	ExpansionSearchFallbackPathDependentPredicate = "path_dependent_predicate"

	// ExpansionSearchFallbackRelationshipVariable rejects an observed relationship binding in the variable expansion or fixed suffix.
	ExpansionSearchFallbackRelationshipVariable = "relationship_variable"

	// ExpansionSearchFallbackRelationshipPredicate rejects relationship predicates in the variable expansion or fixed suffix.
	ExpansionSearchFallbackRelationshipPredicate = "relationship_predicate"

	// ExpansionSearchFallbackLimitPushdownConflict rejects a rewrite that conflicts with an existing limit pushdown.
	ExpansionSearchFallbackLimitPushdownConflict = "limit_pushdown_conflict"

	// ExpansionSearchFallbackUnsupportedObservation rejects downstream uses the candidate cannot reconstruct.
	ExpansionSearchFallbackUnsupportedObservation = "unsupported_observation"

	// ExpansionSearchFallbackMutation rejects specialized search for a statement containing updates.
	ExpansionSearchFallbackMutation = "mutation"

	// ExpansionSearchFallbackNonDeterministicPredicate rejects a seed predicate that cannot be safely reordered.
	ExpansionSearchFallbackNonDeterministicPredicate = "non_deterministic_predicate"

	// ExpansionSearchFallbackUnboundRoot rejects a strategy that requires a previously bound expansion root.
	ExpansionSearchFallbackUnboundRoot = "unbound_root"

	// ExpansionSearchFallbackTournamentUnqualified records that no specialized strategy passed qualification.
	ExpansionSearchFallbackTournamentUnqualified = "tournament_unqualified"

	// ExpansionSearchFallbackNoFixedPrefix rejects a strategy that requires a fixed prefix when none exists.
	ExpansionSearchFallbackNoFixedPrefix = "no_fixed_prefix"

	// ExpansionSearchFallbackExpansionNotTerminal rejects endpoint seeding when the expansion is not terminal.
	ExpansionSearchFallbackExpansionNotTerminal = "expansion_not_terminal"

	// ExpansionSearchFallbackPrefixTooLong rejects a prefix that is not exactly one fixed hop.
	ExpansionSearchFallbackPrefixTooLong = "prefix_too_long"

	// ExpansionSearchFallbackDirectionlessPrefix rejects a directionless edge in the fixed prefix.
	ExpansionSearchFallbackDirectionlessPrefix = "directionless_prefix"

	// ExpansionSearchFallbackTerminalNotSelective rejects endpoint seeding without a selective terminal predicate.
	ExpansionSearchFallbackTerminalNotSelective = "terminal_not_selective"

	// ExpansionSearchFallbackCorrelatedTerminal rejects a pre-bound terminal or a terminal predicate that depends on another binding.
	ExpansionSearchFallbackCorrelatedTerminal = "correlated_terminal"

	// ExpansionSearchFallbackZeroDepth rejects a rewrite that cannot preserve zero-length paths.
	ExpansionSearchFallbackZeroDepth = "zero_depth"
)

// ExpansionSearchStrategyDecision records qualification and selection details for one variable expansion.
type ExpansionSearchStrategyDecision struct {
	// Target locates the variable-expansion step governed by this decision.
	Target TraversalStepTarget `json:"target"`
	// Family names the search-strategy family that produced the decision.
	Family string `json:"family"`
	// PlannedPolicy identifies the runtime policy intended for this candidate
	// family, whether or not translation currently emits it.
	PlannedPolicy ExpansionSearchPolicy `json:"planned_policy,omitempty"`
	// EmittedPolicy identifies the runtime policy actually present in emitted
	// SQL. It remains empty for a single forced arm or incumbent-only SQL.
	EmittedPolicy ExpansionSearchPolicy `json:"emitted_policy,omitempty"`
	// PlannedCandidates lists the strategies considered in preference order.
	PlannedCandidates []ExpansionSearchStrategy `json:"planned_candidates"`
	// EmittedCandidates lists the arms present in the translated statement.
	// Runtime telemetry, not this field, records which arm executed.
	EmittedCandidates []ExpansionSearchStrategy `json:"emitted_candidates,omitempty"`
	// ExecutionBoundary describes the SQL boundary that contains the emitted
	// expansion arm or guarded policy.
	ExecutionBoundary string `json:"execution_boundary,omitempty"`
	// ProbeCaps records bounded evidence inputs for the planned policy.
	ProbeCaps ExpansionSearchProbeCaps `json:"probe_caps"`
	// Admission records the exact specialized-state gate and fallback chain.
	Admission ExpansionSearchAdmission `json:"admission"`
	// CandidateStrategy is the specialized strategy proposed by structural analysis.
	CandidateStrategy ExpansionSearchStrategy `json:"candidate_strategy,omitempty"`
	// SelectedStrategy is the strategy chosen after all qualification checks.
	SelectedStrategy ExpansionSearchStrategy `json:"selected_strategy"`
	// StructurallyEligible reports whether the traversal shape supports the candidate.
	StructurallyEligible bool `json:"structurally_eligible"`
	// StaticallyEligible reports whether known bounds and predicates support the candidate.
	StaticallyEligible bool `json:"statically_eligible"`
	// EligibilityFacts records each qualification check and its result.
	EligibilityFacts []ExpansionSearchEligibilityFact `json:"eligibility_facts"`
	// SuffixStartStep is the first traversal step in the fixed suffix.
	SuffixStartStep int `json:"suffix_start_step,omitempty"`
	// SuffixEndStep is the last traversal step in the fixed suffix.
	SuffixEndStep int `json:"suffix_end_step,omitempty"`
	// SuffixLength is the number of traversal steps in the fixed suffix.
	SuffixLength int `json:"suffix_length,omitempty"`
	// PrefixStartStep is the first traversal step in the fixed prefix.
	PrefixStartStep int `json:"prefix_start_step,omitempty"`
	// PrefixEndStep is the last traversal step in the fixed prefix.
	PrefixEndStep int `json:"prefix_end_step,omitempty"`
	// PrefixLength is the number of traversal steps in the fixed prefix.
	PrefixLength int `json:"prefix_length,omitempty"`
	// SeedPredicateClass describes the predicate used to bound reverse search seeds.
	SeedPredicateClass string `json:"seed_predicate_class,omitempty"`
	// EndpointLimit caps terminal endpoints admitted into endpoint-seeded search.
	EndpointLimit int64 `json:"endpoint_limit,omitempty"`
	// StateLimit caps reverse-search states admitted before falling back.
	StateLimit int64 `json:"state_limit,omitempty"`
	// HasFinalLimit reports whether the terminal projection has a row limit.
	HasFinalLimit bool `json:"has_final_limit,omitempty"`
	// ObservationMode describes the representation required by downstream consumers.
	ObservationMode ExpansionSearchObservationMode `json:"observation_mode"`
	// LogicalDirection records the variable expansion's Cypher direction.
	LogicalDirection string `json:"logical_direction"`
	// MinimumDepth is the inclusive lower expansion-depth bound.
	MinimumDepth int64 `json:"minimum_depth"`
	// MaximumDepth is the inclusive upper expansion-depth bound, or zero when unbounded.
	MaximumDepth int64 `json:"maximum_depth,omitempty"`
	// SelectionMode records whether selection was automatic or forced by tooling.
	SelectionMode string `json:"selection_mode"`
	// SelectorVersion identifies the policy version that ranked the candidates.
	SelectorVersion string `json:"selector_version"`
	// FallbackStrategy is used when the specialized candidate cannot be applied.
	FallbackStrategy ExpansionSearchStrategy `json:"fallback_strategy"`
	// FallbackReason explains why the specialized candidate was not selected.
	FallbackReason string `json:"fallback_reason"`
}

type PredicatePlacementDecision struct {
	Target     TraversalStepTarget      `json:"target"`
	Attachment PredicateAttachment      `json:"attachment"`
	Placement  PredicateAttachmentScope `json:"placement"`
}

type PatternPredicatePlacementMode string

const (
	// PatternPredicatePlacementExistence lowers a pattern predicate as an existence test.
	PatternPredicatePlacementExistence PatternPredicatePlacementMode = "existence"
)

type PatternPredicatePlacementDecision struct {
	Target TraversalStepTarget           `json:"target"`
	Mode   PatternPredicatePlacementMode `json:"mode"`
}

type CountStoreFastPathTarget string

const (
	// CountStoreFastPathNode reads a node count directly from graph statistics.
	CountStoreFastPathNode CountStoreFastPathTarget = "node"

	// CountStoreFastPathEdge reads a relationship count directly from graph statistics.
	CountStoreFastPathEdge CountStoreFastPathTarget = "edge"
)

type CountStoreFastPathDecision struct {
	QueryPartIndex int                      `json:"query_part_index"`
	ClauseIndex    int                      `json:"clause_index"`
	PatternIndex   int                      `json:"pattern_index"`
	BindingSymbol  string                   `json:"binding_symbol,omitempty"`
	Target         CountStoreFastPathTarget `json:"target"`
	KindSymbols    []string                 `json:"kind_symbols,omitempty"`
}

type ExactRangeExpansionDecision struct {
	Target TraversalStepTarget `json:"target"`
	Depth  int64               `json:"depth"`
}

type PathRelationshipPredicateDecision struct {
	Target        QuantifierTarget `json:"target"`
	PathSymbol    string           `json:"path_symbol"`
	BindingSymbol string           `json:"binding_symbol"`
}

type AggregateTraversalCountDecision struct {
	QueryPartIndex int                 `json:"query_part_index"`
	SourceSymbol   string              `json:"source_symbol"`
	TerminalSymbol string              `json:"terminal_symbol"`
	CountAlias     string              `json:"count_alias"`
	Limit          int64               `json:"limit,omitempty"`
	Target         TraversalStepTarget `json:"target"`
}

type FieldRequirement string

const (
	// FieldRequirementEntityID requires only the scalar entity identifier.
	FieldRequirementEntityID FieldRequirement = "entity_id"

	// FieldRequirementKinds requires the entity kind array in addition to identity.
	FieldRequirementKinds FieldRequirement = "kinds"

	// FieldRequirementProperties requires the entity property document in addition to identity.
	FieldRequirementProperties FieldRequirement = "properties"

	// FieldRequirementFullEntity requires the complete node or relationship composite.
	FieldRequirementFullEntity FieldRequirement = "full_entity"

	// FieldRequirementRelationshipIDs requires relationship IDs without hydrated relationship composites.
	FieldRequirementRelationshipIDs FieldRequirement = "relationship_ids"

	// FieldRequirementOrderedPathEdgeIDs requires edge IDs in path traversal order.
	FieldRequirementOrderedPathEdgeIDs FieldRequirement = "ordered_path_edge_ids"

	// FieldRequirementFullPath requires the complete hydrated path composite.
	FieldRequirementFullPath FieldRequirement = "full_path"
)

// FieldRequirementUse records the representation required at one ordered use of a binding.
type FieldRequirementUse struct {
	// Ordinal orders this use relative to the other uses in its query part.
	Ordinal int `json:"ordinal"`
	// Fields lists the binding components consumed at this use.
	Fields []FieldRequirement `json:"fields"`
	// Internal reports whether the requirement is internal to translation rather than an external consumer.
	Internal bool `json:"internal,omitempty"`
}

// FieldRequirementDecision is analysis metadata only. Phase 6B consumes this
// staged information when it is safe to lower a composite binding to scalar
// state; recording it here intentionally does not change SQL semantics.
type FieldRequirementDecision struct {
	// QueryPartIndex identifies the query part containing the analyzed binding.
	QueryPartIndex int `json:"query_part_index"`
	// Symbol is the Cypher binding whose representation requirements were analyzed.
	Symbol string `json:"symbol"`
	// Fields is the union of binding components required by all uses.
	Fields []FieldRequirement `json:"fields"`
	// Uses preserves the ordered evidence contributing to Fields.
	Uses []FieldRequirementUse `json:"uses"`
	// LastUse is the greatest use ordinal observed for the binding.
	LastUse int `json:"last_use"`
}

type AggregateTraversalCountShape struct {
	QueryPartIndex    int
	SourceSymbol      string
	TerminalSymbol    string
	CountAlias        string
	ReturnSourceAlias string
	ReturnCountAlias  string
	ReturnCount       bool
	Limit             int64
	SourceMatch       *cypher.Match
	TerminalMatch     *cypher.Match
	SourceKinds       graph.Kinds
	TerminalKinds     graph.Kinds
	RelationshipKinds graph.Kinds
	Direction         graph.Direction
	MinDepth          int64
	MaxDepth          int64
	Target            TraversalStepTarget
}

// LoweringPlan records lowering analyses and semantic or physical decisions for a query.
type LoweringPlan struct {
	// ProjectionPruning records traversal fields that downstream clauses do not require.
	ProjectionPruning []ProjectionPruningDecision `json:"projection_pruning,omitempty"`
	// LatePathMaterialization records path values whose hydration can be deferred.
	LatePathMaterialization []LatePathMaterializationDecision `json:"late_path_materialization,omitempty"`
	// ExpandInto records traversal steps whose endpoints are both already bound.
	ExpandInto []ExpandIntoDecision `json:"expand_into,omitempty"`
	// TraversalDirection records planned logical direction changes.
	TraversalDirection []TraversalDirectionDecision `json:"traversal_direction,omitempty"`
	// ShortestPathStrategy records directional search choices for shortest-path steps.
	ShortestPathStrategy []ShortestPathStrategyDecision `json:"shortest_path_strategy,omitempty"`
	// ShortestPathFilter records endpoint filters selected for materialization.
	ShortestPathFilter []ShortestPathFilterDecision `json:"shortest_path_filter,omitempty"`
	// LimitPushdown records row limits that may safely constrain traversal work.
	LimitPushdown []LimitPushdownDecision `json:"limit_pushdown,omitempty"`
	// ExpansionSuffixPushdown records fixed suffixes considered for supplemental filtering, including withheld candidates.
	ExpansionSuffixPushdown []ExpansionSuffixPushdownDecision `json:"expansion_suffix_pushdown,omitempty"`
	// PredicatePlacement records the earliest safe traversal scope for attached predicates.
	PredicatePlacement []PredicatePlacementDecision `json:"predicate_placement,omitempty"`
	// PatternPredicate records existence lowering selected for pattern predicates.
	PatternPredicate []PatternPredicatePlacementDecision `json:"pattern_predicate_placement,omitempty"`
	// CountStoreFastPath records counts answerable directly from graph statistics.
	CountStoreFastPath []CountStoreFastPathDecision `json:"count_store_fast_path,omitempty"`
	// ExactRangeExpansion records short fixed-depth ranges selected for unrolling.
	ExactRangeExpansion []ExactRangeExpansionDecision `json:"exact_range_expansion,omitempty"`
	// PathRelationshipPredicate records relationship quantifiers attached to carried path state.
	PathRelationshipPredicate []PathRelationshipPredicateDecision `json:"path_relationship_predicate,omitempty"`
	// AggregateTraversalCount records traversals lowered directly to aggregate counts.
	AggregateTraversalCount []AggregateTraversalCountDecision `json:"aggregate_traversal_count,omitempty"`
	// FieldRequirements records downstream representation needs for each analyzed binding.
	FieldRequirements []FieldRequirementDecision `json:"field_requirements,omitempty"`
	// ShortestPathExecutor records physical executor choices for shortest-path steps.
	ShortestPathExecutor []ShortestPathExecutorDecision `json:"shortest_path_executor,omitempty"`
	// ExpansionSearchStrategy records physical search choices for variable expansions.
	ExpansionSearchStrategy []ExpansionSearchStrategyDecision `json:"expansion_search_strategy,omitempty"`
	// EndpointResolution records planned-only bounded endpoint materialization for SP/ASP traversals.
	EndpointResolution []EndpointResolutionDecision `json:"endpoint_resolution,omitempty"`
	// TraversalPredicate records conservative locality and universality classifications.
	TraversalPredicate []TraversalPredicateDecision `json:"traversal_predicate,omitempty"`
}

// Empty reports whether the plan contains no lowering-analysis or decision entries.
func (s LoweringPlan) Empty() bool {
	return len(s.ProjectionPruning) == 0 &&
		len(s.LatePathMaterialization) == 0 &&
		len(s.ExpandInto) == 0 &&
		len(s.TraversalDirection) == 0 &&
		len(s.ShortestPathStrategy) == 0 &&
		len(s.ShortestPathFilter) == 0 &&
		len(s.LimitPushdown) == 0 &&
		len(s.ExpansionSuffixPushdown) == 0 &&
		len(s.PredicatePlacement) == 0 &&
		len(s.PatternPredicate) == 0 &&
		len(s.CountStoreFastPath) == 0 &&
		len(s.ExactRangeExpansion) == 0 &&
		len(s.PathRelationshipPredicate) == 0 &&
		len(s.AggregateTraversalCount) == 0 &&
		len(s.FieldRequirements) == 0 &&
		len(s.ShortestPathExecutor) == 0 &&
		len(s.ExpansionSearchStrategy) == 0 &&
		len(s.EndpointResolution) == 0 &&
		len(s.TraversalPredicate) == 0
}

// Decisions returns one summary entry for each lowering category present in the plan.
func (s LoweringPlan) Decisions() []LoweringDecision {
	var decisions []LoweringDecision
	add := func(name string, applied bool) {
		if applied {
			decisions = append(decisions, LoweringDecision{Name: name})
		}
	}

	add(LoweringProjectionPruning, len(s.ProjectionPruning) > 0)
	add(LoweringLatePathMaterialization, len(s.LatePathMaterialization) > 0)
	add(LoweringExpandIntoDetection, len(s.ExpandInto) > 0)
	add(LoweringTraversalDirection, len(s.TraversalDirection) > 0)
	add(LoweringShortestPathStrategy, len(s.ShortestPathStrategy) > 0)
	add(LoweringShortestPathFilter, len(s.ShortestPathFilter) > 0)
	add(LoweringLimitPushdown, len(s.LimitPushdown) > 0)
	add(LoweringExpansionSuffixPushdown, len(s.ExpansionSuffixPushdown) > 0)
	add(LoweringPredicatePlacement, len(s.PredicatePlacement) > 0 || len(s.PatternPredicate) > 0)
	add(LoweringCountStoreFastPath, len(s.CountStoreFastPath) > 0)
	add(LoweringExactRangeExpansion, len(s.ExactRangeExpansion) > 0)
	add(LoweringPathRelationshipPredicate, len(s.PathRelationshipPredicate) > 0)
	add(LoweringAggregateTraversalCount, len(s.AggregateTraversalCount) > 0)
	add(LoweringFieldRequirements, len(s.FieldRequirements) > 0)
	add(LoweringShortestPathExecutor, len(s.ShortestPathExecutor) > 0)
	add(LoweringExpansionSearchStrategy, len(s.ExpansionSearchStrategy) > 0)
	add(LoweringEndpointResolution, len(s.EndpointResolution) > 0)
	add(LoweringTraversalPredicateClassification, len(s.TraversalPredicate) > 0)

	return decisions
}

func IndexPatternTargets(query *cypher.RegularQuery) map[*cypher.PatternPart]PatternTarget {
	targets := map[*cypher.PatternPart]PatternTarget{}

	if query == nil || query.SingleQuery == nil {
		return targets
	}

	if query.SingleQuery.MultiPartQuery != nil {
		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			indexReadingClauseTargets(targets, queryPartIndex, part.ReadingClauses)
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			indexReadingClauseTargets(targets, len(query.SingleQuery.MultiPartQuery.Parts), finalPart.ReadingClauses)
		}
	} else if query.SingleQuery.SinglePartQuery != nil {
		indexReadingClauseTargets(targets, 0, query.SingleQuery.SinglePartQuery.ReadingClauses)
	}

	return targets
}

func IndexPatternPredicateTargets(query *cypher.RegularQuery) map[*cypher.PatternPredicate]PatternTarget {
	targets := map[*cypher.PatternPredicate]PatternTarget{}

	if query == nil || query.SingleQuery == nil {
		return targets
	}

	if query.SingleQuery.MultiPartQuery != nil {
		for queryPartIndex, part := range query.SingleQuery.MultiPartQuery.Parts {
			if part == nil {
				continue
			}

			indexQueryPartPatternPredicateTargets(targets, queryPartIndex, part)
		}

		if finalPart := query.SingleQuery.MultiPartQuery.SinglePartQuery; finalPart != nil {
			indexQueryPartPatternPredicateTargets(targets, len(query.SingleQuery.MultiPartQuery.Parts), finalPart)
		}
	} else if query.SingleQuery.SinglePartQuery != nil {
		indexQueryPartPatternPredicateTargets(targets, 0, query.SingleQuery.SinglePartQuery)
	}

	return targets
}

// indexReadingClauseTargets maps each pattern in readingClauses to stable source coordinates.
func indexReadingClauseTargets(targets map[*cypher.PatternPart]PatternTarget, queryPartIndex int, readingClauses []*cypher.ReadingClause) {
	for clauseIndex, readingClause := range readingClauses {
		if readingClause == nil || readingClause.Match == nil {
			continue
		}

		for patternIndex, patternPart := range readingClause.Match.Pattern {
			targets[patternPart] = PatternTarget{
				QueryPartIndex: queryPartIndex,
				ClauseIndex:    clauseIndex,
				PatternIndex:   patternIndex,
			}
		}
	}
}

// indexQueryPartPatternPredicateTargets assigns stable target coordinates to pattern predicates in one query part.
func indexQueryPartPatternPredicateTargets(targets map[*cypher.PatternPredicate]PatternTarget, queryPartIndex int, queryPart cypher.SyntaxNode) {
	for _, indexedPredicate := range indexedPatternPredicatesInQueryPart(queryPart) {
		targets[indexedPredicate.Predicate] = PatternTarget{
			QueryPartIndex: queryPartIndex,
			ClauseIndex:    indexedPredicate.ClauseIndex,
			PatternIndex:   indexedPredicate.PredicateIndex,
			Predicate:      true,
			PredicateIndex: indexedPredicate.PredicateIndex,
		}
	}
}
