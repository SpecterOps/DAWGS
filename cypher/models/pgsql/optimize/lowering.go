package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

const (
	LoweringProjectionPruning       = "ProjectionPruning"
	LoweringLatePathMaterialization = "LatePathMaterialization"
	LoweringExpandIntoDetection     = "ExpandIntoDetection"
	LoweringTraversalDirection      = "TraversalDirectionSelection"
	LoweringShortestPathStrategy    = "ShortestPathStrategySelection"
	LoweringShortestPathFilter      = "ShortestPathFilterMaterialization"
	LoweringLimitPushdown           = "LimitPushdown"
	LoweringExpansionSuffixPushdown = "ExpansionSuffixPushdown"
	LoweringPredicatePlacement      = "PredicatePlacement"
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
	PredicateAttachments []PredicateAttachment `json:"predicate_attachments,omitempty"`
}

type PredicatePlacementDecision struct {
	Target     TraversalStepTarget      `json:"target"`
	Attachment PredicateAttachment      `json:"attachment"`
	Placement  PredicateAttachmentScope `json:"placement"`
}

type LoweringPlan struct {
	ProjectionPruning       []ProjectionPruningDecision       `json:"projection_pruning,omitempty"`
	LatePathMaterialization []LatePathMaterializationDecision `json:"late_path_materialization,omitempty"`
	ExpandInto              []ExpandIntoDecision              `json:"expand_into,omitempty"`
	TraversalDirection      []TraversalDirectionDecision      `json:"traversal_direction,omitempty"`
	ShortestPathStrategy    []ShortestPathStrategyDecision    `json:"shortest_path_strategy,omitempty"`
	ShortestPathFilter      []ShortestPathFilterDecision      `json:"shortest_path_filter,omitempty"`
	LimitPushdown           []LimitPushdownDecision           `json:"limit_pushdown,omitempty"`
	ExpansionSuffixPushdown []ExpansionSuffixPushdownDecision `json:"expansion_suffix_pushdown,omitempty"`
	PredicatePlacement      []PredicatePlacementDecision      `json:"predicate_placement,omitempty"`
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
		len(s.PredicatePlacement) == 0
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
	add(LoweringPredicatePlacement, len(s.PredicatePlacement) > 0)

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
	for predicateIndex, predicate := range patternPredicatesInQueryPart(queryPart) {
		targets[predicate] = PatternTarget{
			QueryPartIndex: queryPartIndex,
			PatternIndex:   predicateIndex,
			Predicate:      true,
			PredicateIndex: predicateIndex,
		}
	}
}
