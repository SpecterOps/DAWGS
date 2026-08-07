package optimize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

const (
	LoweringProjectionPruning         = "ProjectionPruning"
	LoweringLatePathMaterialization   = "LatePathMaterialization"
	LoweringExpandIntoDetection       = "ExpandIntoDetection"
	LoweringTraversalDirection        = "TraversalDirectionSelection"
	LoweringShortestPathStrategy      = "ShortestPathStrategySelection"
	LoweringShortestPathFilter        = "ShortestPathFilterMaterialization"
	LoweringLimitPushdown             = "LimitPushdown"
	LoweringExpansionSuffixPushdown   = "ExpansionSuffixPushdown"
	LoweringPredicatePlacement        = "PredicatePlacement"
	LoweringCountStoreFastPath        = "CountStoreFastPath"
	LoweringCollectIDMembership       = "CollectIDMembership"
	LoweringAggregateTraversalCount   = "AggregateTraversalCount"
	LoweringExactRangeExpansion       = "ExactRangeExpansion"
	LoweringPathRelationshipPredicate = "PathRelationshipPredicate"
	LoweringFieldRequirements         = "FieldRequirements"
	LoweringShortestPathExecutor      = "ShortestPathExecutorDecision"
	LoweringExpansionSearchStrategy   = "ExpansionSearchStrategyDecision"
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
	LatePathMaterializationPathEdgeID    LatePathMaterializationMode = "path_edge_id"
	LatePathMaterializationExpansionPath LatePathMaterializationMode = "expansion_path"
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
	ShortestPathStrategyBidirectional  ShortestPathStrategy = "bidirectional"
	ShortestPathStrategyUnidirectional ShortestPathStrategy = "unidirectional"
)

type ShortestPathStrategyDecision struct {
	Target   TraversalStepTarget  `json:"target"`
	Strategy ShortestPathStrategy `json:"strategy"`
	Reason   string               `json:"reason,omitempty"`
}

type ShortestPathExecutor string

const (
	ShortestPathExecutorIncumbentWorkspace ShortestPathExecutor = "SP-S0"
	ShortestPathExecutorS1ArrayBFS         ShortestPathExecutor = "SP-S1"
	ShortestPathExecutorS2TraceRelation    ShortestPathExecutor = "SP-S2"
	ShortestPathExecutorS3Unidirectional   ShortestPathExecutor = "SP-S3-U-D"
	ShortestPathExecutorS3EdgeM0           ShortestPathExecutor = "SP-S3-U-E+MAT-M0"
)

type ShortestPathObservationMode string

const (
	ShortestPathObservationDistance ShortestPathObservationMode = "distance"
	ShortestPathObservationOnePath  ShortestPathObservationMode = "one_path"
	ShortestPathObservationUnknown  ShortestPathObservationMode = "unknown"
)

const (
	ShortestPathFallbackAllShortestPaths      = "all_shortest_paths"
	ShortestPathFallbackCorrelatedEndpoints   = "correlated_endpoints"
	ShortestPathFallbackMultipleEndpointPairs = "multiple_endpoint_pairs"
	ShortestPathFallbackNonSingletonID        = "non_singleton_id"
	ShortestPathFallbackMultipleIDEqualities  = "multiple_id_equalities"
	ShortestPathFallbackPathPredicate         = "path_predicate"
	ShortestPathFallbackRelationshipPredicate = "relationship_predicate"
	ShortestPathFallbackRelationshipVariable  = "relationship_variable"
	ShortestPathFallbackDirectionless         = "directionless"
	ShortestPathFallbackOptionalMatch         = "optional_match"
	ShortestPathFallbackUnsupportedDepth      = "unsupported_depth"
	ShortestPathFallbackMutation              = "mutation"
	ShortestPathFallbackMultiplePathCalls     = "multiple_path_calls"
	ShortestPathFallbackTournamentUnqualified = "tournament_unqualified"
)

type ShortestPathEligibilityFact struct {
	Name     string `json:"name"`
	Eligible bool   `json:"eligible"`
}

// ShortestPathExecutorDecision records either a qualified static executor or
// the incumbent fallback, keeping every eligibility and fallback fact visible.
type ShortestPathExecutorDecision struct {
	Target               TraversalStepTarget           `json:"target"`
	Family               string                        `json:"family"`
	PlannedCandidates    []ShortestPathExecutor        `json:"planned_candidates"`
	SelectedExecutor     ShortestPathExecutor          `json:"selected_executor"`
	ObservationMode      ShortestPathObservationMode   `json:"observation_mode"`
	Eligibility          []ShortestPathEligibilityFact `json:"eligibility"`
	StructurallyEligible bool                          `json:"structurally_eligible"`
	MinimumDepth         int64                         `json:"minimum_depth"`
	MaximumDepth         int64                         `json:"maximum_depth"`
	StateLimit           int64                         `json:"state_limit,omitempty"`
	SelectorVersion      string                        `json:"selector_version"`
	SelectionMode        string                        `json:"selection_mode"`
	FallbackExecutor     ShortestPathExecutor          `json:"fallback_executor"`
	FallbackReason       string                        `json:"fallback_reason"`
	ExperimentalWinner   bool                          `json:"experimental_winner,omitempty"`
}

type ShortestPathFilterMode string

const (
	ShortestPathFilterTerminal     ShortestPathFilterMode = "terminal"
	ShortestPathFilterEndpointPair ShortestPathFilterMode = "endpoint_pair"
)

type ShortestPathFilterDecision struct {
	Target TraversalStepTarget    `json:"target"`
	Mode   ShortestPathFilterMode `json:"mode"`
	Reason string                 `json:"reason,omitempty"`
}

type LimitPushdownMode string

const (
	LimitPushdownTraversalCTE        LimitPushdownMode = "traversal_cte"
	LimitPushdownShortestPathHarness LimitPushdownMode = "shortest_path_harness"
)

type LimitPushdownDecision struct {
	Target TraversalStepTarget `json:"target"`
	Mode   LimitPushdownMode   `json:"mode"`
}

type ExpansionSuffixPushdownDecision struct {
	Target               TraversalStepTarget   `json:"target"`
	SuffixLength         int                   `json:"suffix_length"`
	SuffixStartStep      int                   `json:"suffix_start_step"`
	SuffixEndStep        int                   `json:"suffix_end_step"`
	ApplySupplemental    bool                  `json:"apply_supplemental"`
	Reason               string                `json:"reason,omitempty"`
	PredicateAttachments []PredicateAttachment `json:"predicate_attachments,omitempty"`
}

type ExpansionSearchStrategy string

const (
	ExpansionSearchStepwiseForward          ExpansionSearchStrategy = "ADCS-INCUMBENT-STEPWISE"
	ExpansionSearchLateHydratedForward      ExpansionSearchStrategy = "ADCS-A0"
	ExpansionSearchFactoredSuffixForward    ExpansionSearchStrategy = "ADCS-A2"
	ExpansionSearchSuffixSeededReverse      ExpansionSearchStrategy = "ADCS-A3"
	ExpansionSearchBackwardViabilityForward ExpansionSearchStrategy = "ADCS-A4"
	ExpansionSearchBoundedReverseForward    ExpansionSearchStrategy = "ADCS-A5"
)

type ExpansionSearchObservationMode string

const (
	ExpansionSearchObservationEndpointIDs    ExpansionSearchObservationMode = "endpoint_ids"
	ExpansionSearchObservationOrderedPathIDs ExpansionSearchObservationMode = "ordered_path_ids"
	ExpansionSearchObservationFullPath       ExpansionSearchObservationMode = "full_path"
	ExpansionSearchObservationUnsupported    ExpansionSearchObservationMode = "unsupported"
)

type ExpansionSearchEligibilityFact struct {
	Name     string `json:"name"`
	Eligible bool   `json:"eligible"`
}

const (
	ExpansionSearchFallbackNoFixedSuffix              = "no_fixed_suffix"
	ExpansionSearchFallbackSuffixTooShort             = "suffix_too_short"
	ExpansionSearchFallbackOptionalMatch              = "optional_match"
	ExpansionSearchFallbackShortestPath               = "shortest_path"
	ExpansionSearchFallbackAllShortestPaths           = "all_shortest_paths"
	ExpansionSearchFallbackDirectionlessExpansion     = "directionless_expansion"
	ExpansionSearchFallbackDirectionlessSuffix        = "directionless_suffix"
	ExpansionSearchFallbackUnboundedDepth             = "unbounded_depth"
	ExpansionSearchFallbackUnsupportedDepth           = "unsupported_depth"
	ExpansionSearchFallbackMultipleVariableExpansions = "multiple_variable_expansions"
	ExpansionSearchFallbackCorrelatedSuffix           = "correlated_suffix"
	ExpansionSearchFallbackCrossRegionPredicate       = "cross_region_predicate"
	ExpansionSearchFallbackPathDependentPredicate     = "path_dependent_predicate"
	ExpansionSearchFallbackRelationshipVariable       = "relationship_variable"
	ExpansionSearchFallbackRelationshipPredicate      = "relationship_predicate"
	ExpansionSearchFallbackLimitPushdownConflict      = "limit_pushdown_conflict"
	ExpansionSearchFallbackUnsupportedObservation     = "unsupported_observation"
	ExpansionSearchFallbackMutation                   = "mutation"
	ExpansionSearchFallbackUnboundRoot                = "unbound_root"
	ExpansionSearchFallbackTournamentUnqualified      = "tournament_unqualified"
)

type ExpansionSearchStrategyDecision struct {
	Target               TraversalStepTarget              `json:"target"`
	Family               string                           `json:"family"`
	PlannedCandidates    []ExpansionSearchStrategy        `json:"planned_candidates"`
	SelectedStrategy     ExpansionSearchStrategy          `json:"selected_strategy"`
	StructurallyEligible bool                             `json:"structurally_eligible"`
	EligibilityFacts     []ExpansionSearchEligibilityFact `json:"eligibility_facts"`
	SuffixStartStep      int                              `json:"suffix_start_step,omitempty"`
	SuffixEndStep        int                              `json:"suffix_end_step,omitempty"`
	SuffixLength         int                              `json:"suffix_length,omitempty"`
	ObservationMode      ExpansionSearchObservationMode   `json:"observation_mode"`
	LogicalDirection     string                           `json:"logical_direction"`
	MinimumDepth         int64                            `json:"minimum_depth"`
	MaximumDepth         int64                            `json:"maximum_depth,omitempty"`
	SelectionMode        string                           `json:"selection_mode"`
	SelectorVersion      string                           `json:"selector_version"`
	SuffixProbeLimit     int64                            `json:"suffix_probe_limit,omitempty"`
	ReverseStateLimit    int64                            `json:"reverse_state_limit,omitempty"`
	FallbackStrategy     ExpansionSearchStrategy          `json:"fallback_strategy"`
	FallbackReason       string                           `json:"fallback_reason"`
}

type PredicatePlacementDecision struct {
	Target     TraversalStepTarget      `json:"target"`
	Attachment PredicateAttachment      `json:"attachment"`
	Placement  PredicateAttachmentScope `json:"placement"`
}

type PatternPredicatePlacementMode string

const (
	PatternPredicatePlacementExistence PatternPredicatePlacementMode = "existence"
)

type PatternPredicatePlacementDecision struct {
	Target TraversalStepTarget           `json:"target"`
	Mode   PatternPredicatePlacementMode `json:"mode"`
}

type CountStoreFastPathTarget string

const (
	CountStoreFastPathNode CountStoreFastPathTarget = "node"
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
	FieldRequirementEntityID           FieldRequirement = "entity_id"
	FieldRequirementKinds              FieldRequirement = "kinds"
	FieldRequirementProperties         FieldRequirement = "properties"
	FieldRequirementFullEntity         FieldRequirement = "full_entity"
	FieldRequirementRelationshipIDs    FieldRequirement = "relationship_ids"
	FieldRequirementOrderedPathEdgeIDs FieldRequirement = "ordered_path_edge_ids"
	FieldRequirementFullPath           FieldRequirement = "full_path"
)

type FieldRequirementUse struct {
	Ordinal  int                `json:"ordinal"`
	Fields   []FieldRequirement `json:"fields"`
	Internal bool               `json:"internal,omitempty"`
}

// FieldRequirementDecision is analysis metadata only. Phase 6B consumes this
// staged information when it is safe to lower a composite binding to scalar
// state; recording it here intentionally does not change SQL semantics.
type FieldRequirementDecision struct {
	QueryPartIndex int                   `json:"query_part_index"`
	Symbol         string                `json:"symbol"`
	Fields         []FieldRequirement    `json:"fields"`
	Uses           []FieldRequirementUse `json:"uses"`
	LastUse        int                   `json:"last_use"`
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

type LoweringPlan struct {
	ProjectionPruning         []ProjectionPruningDecision         `json:"projection_pruning,omitempty"`
	LatePathMaterialization   []LatePathMaterializationDecision   `json:"late_path_materialization,omitempty"`
	ExpandInto                []ExpandIntoDecision                `json:"expand_into,omitempty"`
	TraversalDirection        []TraversalDirectionDecision        `json:"traversal_direction,omitempty"`
	ShortestPathStrategy      []ShortestPathStrategyDecision      `json:"shortest_path_strategy,omitempty"`
	ShortestPathFilter        []ShortestPathFilterDecision        `json:"shortest_path_filter,omitempty"`
	LimitPushdown             []LimitPushdownDecision             `json:"limit_pushdown,omitempty"`
	ExpansionSuffixPushdown   []ExpansionSuffixPushdownDecision   `json:"expansion_suffix_pushdown,omitempty"`
	PredicatePlacement        []PredicatePlacementDecision        `json:"predicate_placement,omitempty"`
	PatternPredicate          []PatternPredicatePlacementDecision `json:"pattern_predicate_placement,omitempty"`
	CountStoreFastPath        []CountStoreFastPathDecision        `json:"count_store_fast_path,omitempty"`
	ExactRangeExpansion       []ExactRangeExpansionDecision       `json:"exact_range_expansion,omitempty"`
	PathRelationshipPredicate []PathRelationshipPredicateDecision `json:"path_relationship_predicate,omitempty"`
	AggregateTraversalCount   []AggregateTraversalCountDecision   `json:"aggregate_traversal_count,omitempty"`
	FieldRequirements         []FieldRequirementDecision          `json:"field_requirements,omitempty"`
	ShortestPathExecutor      []ShortestPathExecutorDecision      `json:"shortest_path_executor,omitempty"`
	ExpansionSearchStrategy   []ExpansionSearchStrategyDecision   `json:"expansion_search_strategy,omitempty"`
}

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
		len(s.ExpansionSearchStrategy) == 0
}

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
