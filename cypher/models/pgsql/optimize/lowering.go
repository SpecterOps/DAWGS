package optimize

import "github.com/specterops/dawgs/cypher/models/cypher"

const (
	LoweringProjectionPruning       = "ProjectionPruning"
	LoweringLatePathMaterialization = "LatePathMaterialization"
	LoweringExpandIntoDetection     = "ExpandIntoDetection"
	LoweringExpansionSuffixPushdown = "ExpansionSuffixPushdown"
	LoweringPredicatePlacement      = "PredicatePlacement"
)

type LoweringDecision struct {
	Name string `json:"name"`
}

type PatternTarget struct {
	QueryPartIndex int `json:"query_part_index"`
	ClauseIndex    int `json:"clause_index"`
	PatternIndex   int `json:"pattern_index"`
}

func (s PatternTarget) TraversalStep(stepIndex int) TraversalStepTarget {
	return TraversalStepTarget{
		QueryPartIndex: s.QueryPartIndex,
		ClauseIndex:    s.ClauseIndex,
		PatternIndex:   s.PatternIndex,
		StepIndex:      stepIndex,
	}
}

type TraversalStepTarget struct {
	QueryPartIndex int `json:"query_part_index"`
	ClauseIndex    int `json:"clause_index"`
	PatternIndex   int `json:"pattern_index"`
	StepIndex      int `json:"step_index"`
}

type ProjectionPruningDecision struct {
	Target              TraversalStepTarget `json:"target"`
	UnexportLeftNode    bool                `json:"unexport_left_node,omitempty"`
	UnexportEdge        bool                `json:"unexport_edge,omitempty"`
	UnexportRightNode   bool                `json:"unexport_right_node,omitempty"`
	UnexportExpansionID bool                `json:"unexport_expansion_id,omitempty"`
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

type ExpansionSuffixPushdownDecision struct {
	Target       TraversalStepTarget `json:"target"`
	SuffixLength int                 `json:"suffix_length"`
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
	ExpansionSuffixPushdown []ExpansionSuffixPushdownDecision `json:"expansion_suffix_pushdown,omitempty"`
	PredicatePlacement      []PredicatePlacementDecision      `json:"predicate_placement,omitempty"`
}

func (s LoweringPlan) Empty() bool {
	return len(s.ProjectionPruning) == 0 &&
		len(s.LatePathMaterialization) == 0 &&
		len(s.ExpandInto) == 0 &&
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
