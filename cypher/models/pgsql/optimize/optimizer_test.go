package optimize

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type testRule struct {
	name string
}

func (s testRule) Name() string {
	return s.name
}

func (s testRule) Apply(plan *Plan) (bool, error) {
	return false, nil
}

type testBindingLookup map[pgsql.Identifier]pgsql.DataType

func (s testBindingLookup) LookupDataType(identifier pgsql.Identifier) (pgsql.DataType, bool) {
	dataType, found := s[identifier]
	return dataType, found
}

func TestOptimizeCopiesAndAnalyzesQuery(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), adcsQuery)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotSame(t, regularQuery, plan.Query)
	require.Len(t, plan.Analysis.QueryParts, 1)
	require.Len(t, plan.Analysis.QueryParts[0].Regions, 1)
	require.Equal(t, []string{"p1", "p2"}, plan.Analysis.QueryParts[0].ProjectionDependencies)
	require.Equal(t, []RuleResult{
		{Name: "ConservativePatternReordering", Applied: false},
		{Name: "PredicateAttachment", Applied: true},
	}, plan.Rules)
	require.Len(t, plan.PredicateAttachments, 2)
}

func TestFieldRequirementAnalysisDistinguishesObservationBoundaries(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n:Group)-[r:MemberOf*1..]->(ca:EnterpriseCA)
		WHERE n.objectid = 'source'
		RETURN id(ca), labels(n), length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringFieldRequirements})

	bySymbol := map[string]FieldRequirementDecision{}
	for _, decision := range plan.LoweringPlan.FieldRequirements {
		bySymbol[decision.Symbol] = decision
	}

	require.Contains(t, bySymbol["ca"].Fields, FieldRequirementEntityID)
	require.NotContains(t, bySymbol["ca"].Fields, FieldRequirementFullEntity)
	require.Contains(t, bySymbol["n"].Fields, FieldRequirementKinds)
	require.Contains(t, bySymbol["n"].Fields, FieldRequirementProperties)
	require.Contains(t, bySymbol["p"].Fields, FieldRequirementOrderedPathEdgeIDs)
	require.NotContains(t, bySymbol["p"].Fields, FieldRequirementFullPath)
}

func TestOptimizePlansADCSFanoutRewrite(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), adcsQuery)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)

	ctPredicate := PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     2,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeBinding,
		BindingSymbols:  []string{"ct"},
		Dependencies:    []string{"ct"},
	}

	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpansionSuffixPushdown})
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringPredicatePlacement})
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpandIntoDetection})
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringLatePathMaterialization})

	require.Contains(t, plan.LoweringPlan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		SuffixLength:    3,
		SuffixStartStep: 1,
		SuffixEndStep:   3,
		Reason:          "immediate observed continuation produces suffix rows",
	})
	require.Contains(t, plan.LoweringPlan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      0,
		},
		SuffixLength:         2,
		SuffixStartStep:      1,
		SuffixEndStep:        2,
		ApplySupplemental:    true,
		Reason:               "supplemental suffix prefilter retained for unobserved continuation",
		PredicateAttachments: []PredicateAttachment{ctPredicate},
	})
	require.Contains(t, plan.LoweringPlan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      3,
		},
		SuffixLength:      1,
		SuffixStartStep:   4,
		SuffixEndStep:     4,
		ApplySupplemental: true,
		Reason:            "supplemental suffix prefilter retained for unobserved continuation",
	})

	require.Contains(t, plan.LoweringPlan.ExpandInto, ExpandIntoDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      2,
		},
	})
	require.Contains(t, plan.LoweringPlan.ExpandInto, ExpandIntoDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      4,
		},
	})
	require.Contains(t, plan.LoweringPlan.PredicatePlacement, PredicatePlacementDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      1,
		},
		Attachment: ctPredicate,
		Placement:  PredicateAttachmentScopeBinding,
	})
}

func TestOptimizerRunsRulesAndRefreshesAnalysis(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n`)
	require.NoError(t, err)

	plan, err := NewOptimizer(testRule{name: "test"}).Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{{Name: "test", Applied: false}}, plan.Rules)
	require.Len(t, plan.Analysis.QueryParts, 1)
	require.Len(t, plan.Analysis.QueryParts[0].Regions, 1)
}

func TestDefaultPredicateAttachmentRuleReportsSkippedWhenNoPredicatesExist(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{Name: "ConservativePatternReordering", Applied: false},
		{Name: "PredicateAttachment", Applied: false},
	}, plan.Rules)
	require.Empty(t, plan.PredicateAttachments)
}

func TestLoweringPlanReportsProjectionPruning(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n)-[r:MemberOf]->(m)
		RETURN m
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []LoweringDecision{
		{Name: LoweringProjectionPruning},
		{Name: LoweringFieldRequirements},
	}, plan.LoweringPlan.Decisions())
	require.Equal(t, []ProjectionPruningDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		ReferencedSymbols: []string{"m"},
		OmitLeftNode:      true,
		OmitRelationship:  true,
	}}, plan.LoweringPlan.ProjectionPruning)
}

func TestLoweringPlanProjectionPruningKeepsUpdateTargets(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)-[r:MemberOf]->(m)
		SET a.name = 'updated', r.seen = true
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []ProjectionPruningDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		ReferencedSymbols: []string{"a", "r"},
		OmitRightNode:     true,
	}}, plan.LoweringPlan.ProjectionPruning)
}

func TestLoweringPlanReportsPatternPredicateProjectionPruning(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s)
		WHERE (s)-[]->()
		RETURN s
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.ProjectionPruning, ProjectionPruningDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			Predicate:      true,
			StepIndex:      0,
		},
		ReferencedSymbols: []string{"s"},
		OmitRelationship:  true,
		OmitRightNode:     true,
	})
}

func TestLoweringPlanReportsPatternPredicateExistencePlacement(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s)
		WHERE NOT (s)-[]-()
		RETURN s
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringPredicatePlacement})
	require.Equal(t, []PatternPredicatePlacementDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			Predicate:      true,
			StepIndex:      0,
		},
		Mode: PatternPredicatePlacementExistence,
	}}, plan.LoweringPlan.PatternPredicate)
}

func TestLoweringPlanReportsTypedPatternPredicateExistencePlacement(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:Domain), (m:Domain)
		WHERE (n)-[:SpoofSIDHistory|AbuseTGTDelegation]-(m)
		RETURN n
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringPredicatePlacement})
	require.Equal(t, []PatternPredicatePlacementDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			Predicate:      true,
			StepIndex:      0,
		},
		Mode: PatternPredicatePlacementExistence,
	}}, plan.LoweringPlan.PatternPredicate)
}

func TestLoweringPlanReportsPatternPredicateClauseIndex(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)
		MATCH (s)
		WHERE NOT (s)-[]-()
		RETURN s
	`)
	require.NoError(t, err)

	plan, err := BuildLoweringPlan(regularQuery, nil)
	require.NoError(t, err)
	require.Equal(t, []PatternPredicatePlacementDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			Predicate:      true,
			StepIndex:      0,
		},
		Mode: PatternPredicatePlacementExistence,
	}}, plan.PatternPredicate)
	require.Contains(t, plan.ProjectionPruning, ProjectionPruningDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			Predicate:      true,
			StepIndex:      0,
		},
		ReferencedSymbols: []string{"s"},
		OmitRelationship:  true,
		OmitRightNode:     true,
	})
}

func TestSelectivityModelPlansTraversalDirection(t *testing.T) {
	t.Parallel()

	var (
		model         = NewSelectivityModel(testBindingLookup{})
		rightIDLookup = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{pgsql.Identifier("n1"), pgsql.ColumnID},
			pgsql.OperatorEquals,
			pgsql.NewLiteral(1, pgsql.Int),
		)
	)

	shouldFlip, err := model.ShouldFlipTraversalDirection(false, false, nil, rightIDLookup)
	require.NoError(t, err)
	require.True(t, shouldFlip)

	shouldFlip, err = model.ShouldFlipTraversalDirection(true, false, nil, rightIDLookup)
	require.NoError(t, err)
	require.False(t, shouldFlip)

	shouldFlip, err = model.ShouldFlipTraversalDirection(false, true, nil, nil)
	require.NoError(t, err)
	require.True(t, shouldFlip)
}

func TestLoweringPlanReportsLatePathMaterialization(t *testing.T) {
	t.Parallel()

	t.Run("path edge id", func(t *testing.T) {
		t.Parallel()

		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
			MATCH p = (n)-[r:MemberOf]->(m)
			RETURN p
		`)
		require.NoError(t, err)

		plan, err := Optimize(regularQuery)
		require.NoError(t, err)
		require.Equal(t, []LatePathMaterializationDecision{{
			Target: TraversalStepTarget{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				StepIndex:      0,
			},
			Mode: LatePathMaterializationPathEdgeID,
		}}, plan.LoweringPlan.LatePathMaterialization)
	})

	t.Run("relationship composite", func(t *testing.T) {
		t.Parallel()

		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
			MATCH p = (n)-[r:MemberOf]->(m)
			RETURN p, r
		`)
		require.NoError(t, err)

		plan, err := Optimize(regularQuery)
		require.NoError(t, err)
		require.Equal(t, LatePathMaterializationEdgeComposite, plan.LoweringPlan.LatePathMaterialization[0].Mode)
	})

	t.Run("continuation relationship id", func(t *testing.T) {
		t.Parallel()

		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
			MATCH (n)-[:MemberOf]->(m)-[:Enroll]->(ca)
			RETURN ca
		`)
		require.NoError(t, err)

		plan, err := Optimize(regularQuery)
		require.NoError(t, err)
		require.Contains(t, plan.LoweringPlan.LatePathMaterialization, LatePathMaterializationDecision{
			Target: TraversalStepTarget{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				StepIndex:      0,
			},
			Mode: LatePathMaterializationPathEdgeID,
		})
	})

	t.Run("pattern predicate continuation relationship id", func(t *testing.T) {
		t.Parallel()

		regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
			MATCH (s)
			WHERE (s)-[]->()-[]->()
			RETURN s
		`)
		require.NoError(t, err)

		plan, err := Optimize(regularQuery)
		require.NoError(t, err)
		require.Contains(t, plan.LoweringPlan.LatePathMaterialization, LatePathMaterializationDecision{
			Target: TraversalStepTarget{
				QueryPartIndex: 0,
				Predicate:      true,
				StepIndex:      0,
			},
			Mode: LatePathMaterializationPathEdgeID,
		})
	})
}

func TestLoweringPlanReportsExactOneHopRangeExpansion(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..1]->(m)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Equal(t, []ExactRangeExpansionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Depth: 1,
	}}, plan.LoweringPlan.ExactRangeExpansion)
	require.Contains(t, plan.LoweringPlan.LatePathMaterialization, LatePathMaterializationDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode: LatePathMaterializationPathEdgeID,
	})
}

func TestLoweringPlanReportsExactTwoHopRangeExpansion(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*2..2]->(m)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Equal(t, []ExactRangeExpansionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Depth: 2,
	}}, plan.LoweringPlan.ExactRangeExpansion)
}

func TestExactRangeDependentPlanningRequiresDecision(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..1]->(m)-[:Enroll]->(ca)
		RETURN p
	`)
	require.NoError(t, err)

	var (
		readingClauses   = regularQuery.SingleQuery.SinglePartQuery.ReadingClauses
		patternPart      = readingClauses[0].Match.Pattern[0]
		steps            = traversalStepsForPattern(patternPart)
		target           = PatternTarget{}
		sourceReferences = map[string]struct{}{
			"p": {},
		}
	)

	t.Run("without decision", func(t *testing.T) {
		plan := LoweringPlan{}

		appendPatternProjectionPruningDecisions(&plan, target, patternPart, steps, sourceReferences)
		require.Equal(t, []ProjectionPruningDecision{
			{
				Target:                   target.TraversalStep(0),
				ReferencedSymbols:        []string{"p"},
				PatternBindingReferenced: true,
				OmitRelationship:         true,
			},
		}, plan.ProjectionPruning)

		appendPatternLatePathMaterializationDecisions(&plan, target, patternPart, steps, sourceReferences)
		require.Contains(t, plan.LatePathMaterialization, LatePathMaterializationDecision{
			Target: target.TraversalStep(0),
			Mode:   LatePathMaterializationExpansionPath,
		})

		appendExpansionSuffixPushdownDecisions(&plan, 0, readingClauses, nil)
		require.Contains(t, plan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
			Target:            target.TraversalStep(0),
			SuffixLength:      1,
			SuffixStartStep:   1,
			SuffixEndStep:     1,
			ApplySupplemental: true,
			Reason:            "supplemental suffix prefilter retained for unobserved continuation",
		})
	})

	t.Run("with decision", func(t *testing.T) {
		plan := LoweringPlan{
			ExactRangeExpansion: []ExactRangeExpansionDecision{
				{
					Target: target.TraversalStep(0),
					Depth:  1,
				},
			},
		}

		appendPatternProjectionPruningDecisions(&plan, target, patternPart, steps, sourceReferences)
		require.Empty(t, plan.ProjectionPruning)

		appendPatternLatePathMaterializationDecisions(&plan, target, patternPart, steps, sourceReferences)
		require.Contains(t, plan.LatePathMaterialization, LatePathMaterializationDecision{
			Target: target.TraversalStep(0),
			Mode:   LatePathMaterializationPathEdgeID,
		})

		appendExpansionSuffixPushdownDecisions(&plan, 0, readingClauses, nil)
		require.Empty(t, plan.ExpansionSuffixPushdown)
	})
}

func TestLoweringPlanSkipsExactRangeExpansionBeyondDepthCap(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*3..3]->(m)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Empty(t, plan.LoweringPlan.ExactRangeExpansion)
}

func TestLoweringPlanSkipsUndirectedExactRangeExpansion(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*2..2]-(m)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Empty(t, plan.LoweringPlan.ExactRangeExpansion)
}

func TestLoweringPlanSkipsExactOneHopRangeExpansionForNamedRelationshipBinding(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[r:MemberOf*1..1]->(m)
		RETURN p, r
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Empty(t, plan.LoweringPlan.ExactRangeExpansion)
	require.Contains(t, plan.LoweringPlan.LatePathMaterialization, LatePathMaterializationDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode: LatePathMaterializationExpansionPath,
	})
}

func TestLoweringPlanSkipsExactOneHopRangeExpansionForShortestPath(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((n)-[:MemberOf*1..1]->(m))
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringExactRangeExpansion,
	})
	require.Empty(t, plan.LoweringPlan.ExactRangeExpansion)
}

func TestLoweringPlanReportsPathRelationshipPredicate(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(m)
		WHERE any(r in relationships(p) WHERE type(r) STARTS WITH 'Member')
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringPathRelationshipPredicate,
	})
	require.Equal(t, []PathRelationshipPredicateDecision{{
		Target: QuantifierTarget{
			QueryPartIndex:  0,
			QuantifierIndex: 0,
		},
		PathSymbol:    "p",
		BindingSymbol: "r",
	}}, plan.LoweringPlan.PathRelationshipPredicate)
}

func TestLoweringPlanReportsNonePathRelationshipPredicate(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(m)
		WHERE none(r in relationships(p) WHERE type(r) = 'AdminTo')
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringPathRelationshipPredicate,
	})
	require.Equal(t, []PathRelationshipPredicateDecision{{
		Target: QuantifierTarget{
			QueryPartIndex:  0,
			QuantifierIndex: 0,
		},
		PathSymbol:    "p",
		BindingSymbol: "r",
	}}, plan.LoweringPlan.PathRelationshipPredicate)
}

func TestLoweringPlanSkipsPathRelationshipPredicateForAllQuantifier(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(m)
		WHERE all(r in relationships(p) WHERE type(r) = 'MemberOf')
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringPathRelationshipPredicate,
	})
	require.Empty(t, plan.LoweringPlan.PathRelationshipPredicate)
}

func TestLoweringPlanSkipsPathRelationshipPredicateAfterWithProjection(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(m)
		WITH p
		WHERE none(r in relationships(p) WHERE type(r) = 'AdminTo')
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{
		Name: LoweringPathRelationshipPredicate,
	})
	require.Empty(t, plan.LoweringPlan.PathRelationshipPredicate)
}

func TestLoweringPlanReportsExpansionSuffixPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ca:EnterpriseCA)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpansionSuffixPushdown})
	require.Equal(t, []ExpansionSuffixPushdownDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		SuffixLength:      1,
		SuffixStartStep:   1,
		SuffixEndStep:     1,
		ApplySupplemental: true,
		Reason:            "supplemental suffix prefilter retained for unobserved continuation",
	}}, plan.LoweringPlan.ExpansionSuffixPushdown)
}

func TestLoweringPlanReportsConservativeADCSSearchStrategy(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:Group)
		WHERE n.objectid = $objectid
		MATCH p = (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpansionSearchStrategy})
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, "ADCS", decision.Family)
	require.Equal(t, "incumbent_default", decision.SelectionMode)
	require.Equal(t, "adcs-static-v1", decision.SelectorVersion)
	require.Equal(t, []ExpansionSearchStrategy{
		ExpansionSearchStepwiseForward,
		ExpansionSearchLateHydratedForward,
		ExpansionSearchFactoredSuffixForward,
		ExpansionSearchSuffixSeededReverse,
		ExpansionSearchBackwardViabilityForward,
	}, decision.PlannedCandidates)
	require.True(t, decision.StructurallyEligible)
	require.Equal(t, ExpansionSearchStepwiseForward, decision.SelectedStrategy)
	require.Equal(t, ExpansionSearchStepwiseForward, decision.FallbackStrategy)
	require.Equal(t, ExpansionSearchFallbackTournamentUnqualified, decision.FallbackReason)
	require.Equal(t, ExpansionSearchObservationFullPath, decision.ObservationMode)
	require.Equal(t, int64(0), decision.MinimumDepth)
	require.Equal(t, int64(16), decision.MaximumDepth)
	require.Equal(t, 3, decision.SuffixLength)
	require.Equal(t, "outbound", decision.LogicalDirection)
}

func TestExpansionSearchObservationUsesExternalFieldRequirements(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		projection  string
		observation ExpansionSearchObservationMode
	}{
		{name: "endpoint IDs", projection: "id(ca), id(d)", observation: ExpansionSearchObservationEndpointIDs},
		{name: "ordered IDs", projection: "length(p)", observation: ExpansionSearchObservationOrderedPathIDs},
		{name: "full path", projection: "p", observation: ExpansionSearchObservationFullPath},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
				MATCH p = (n:Group)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
				RETURN `+testCase.projection)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
			require.Equal(t, testCase.observation, plan.LoweringPlan.ExpansionSearchStrategy[0].ObservationMode)
		})
	}
}

func TestExpansionSearchFinalizationRejectsVariableExpansionAcrossWith(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:Group)-[:MemberOf*0..16]->()-[:Enroll]->(:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)
		WITH n, d
		MATCH (n)-[:MemberOf*0..4]->(x)
		RETURN id(d), id(x)
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 2)
	require.Equal(t, ExpansionSearchFallbackMultipleVariableExpansions, plan.LoweringPlan.ExpansionSearchStrategy[0].FallbackReason)
	require.False(t, plan.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
}

func TestLoweringPlanReportsStableADCSSearchFallbackCodes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		query  string
		reason string
	}{
		{name: "no fixed suffix", query: `MATCH (n)-[:MemberOf*0..16]->(ca) RETURN id(ca)`, reason: ExpansionSearchFallbackNoFixedSuffix},
		{name: "unbounded", query: `MATCH (n)-[:MemberOf*0..]->()-[:Enroll]->(ca) RETURN id(ca)`, reason: ExpansionSearchFallbackUnboundedDepth},
		{name: "short suffix", query: `MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca) RETURN id(ca)`, reason: ExpansionSearchFallbackSuffixTooShort},
		{name: "directionless", query: `MATCH (n)-[:MemberOf*0..16]-()-[:Enroll]->(ca)-[:A]->()-[:B]->(d) RETURN id(ca)`, reason: ExpansionSearchFallbackDirectionlessExpansion},
		{name: "directionless suffix", query: `MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]-(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca)`, reason: ExpansionSearchFallbackDirectionlessSuffix},
		{name: "optional", query: `OPTIONAL MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca)`, reason: ExpansionSearchFallbackOptionalMatch},
		{name: "shortest path", query: `MATCH p = shortestPath((n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)) RETURN p`, reason: ExpansionSearchFallbackShortestPath},
		{name: "all shortest paths", query: `MATCH p = allShortestPaths((n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain)) RETURN p`, reason: ExpansionSearchFallbackAllShortestPaths},
		{name: "unbound root", query: `MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca), id(d)`, reason: ExpansionSearchFallbackUnboundRoot},
		{name: "unsupported depth", query: `MATCH (n)-[:MemberOf*0..65]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca)`, reason: ExpansionSearchFallbackUnsupportedDepth},
		{name: "relationship variable", query: `MATCH (n)-[r:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca)`, reason: ExpansionSearchFallbackRelationshipVariable},
		{name: "relationship predicate", query: `MATCH (n)-[r:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) WHERE r.enabled = true RETURN id(ca)`, reason: ExpansionSearchFallbackRelationshipPredicate},
		{name: "correlated suffix", query: `MATCH (ca:EnterpriseCA) MATCH p = (n:Group)-[:MemberOf*0..16]->()-[:Enroll]->(ca)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN p`, reason: ExpansionSearchFallbackCorrelatedSuffix},
		{name: "cross-region predicate", query: `MATCH p = (n:Group)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) WHERE n.tenant = ca.tenant RETURN p`, reason: ExpansionSearchFallbackCrossRegionPredicate},
		{name: "path predicate", query: `MATCH p = (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) WHERE length(p) > 0 RETURN p`, reason: ExpansionSearchFallbackPathDependentPredicate},
		{name: "unsupported observation", query: `MATCH p = (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(p)`, reason: ExpansionSearchFallbackUnsupportedObservation},
		{name: "mutation", query: `MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) CREATE (x) RETURN id(ca)`, reason: ExpansionSearchFallbackMutation},
		{name: "limit pushdown conflict", query: `MATCH (n)-[:MemberOf*0..16]->()-[:Enroll]->(ca:EnterpriseCA)-[:TrustedForNTAuth]->(:NTAuthStore)-[:NTAuthStoreFor]->(d:Domain) RETURN id(ca) LIMIT 10`, reason: ExpansionSearchFallbackLimitPushdownConflict},
		{name: "tournament unqualified", query: `MATCH (n)-[:Other*0..16]->()-[:A]->(ca:X)-[:B]->(:Y)-[:C]->(d:Z) RETURN id(ca)`, reason: ExpansionSearchFallbackTournamentUnqualified},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
			require.Equal(t, testCase.reason, plan.LoweringPlan.ExpansionSearchStrategy[0].FallbackReason)
			require.False(t, plan.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
		})
	}
}

func TestLoweringPlanIncludesConstrainedBoundEndpointInExpansionSuffix(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (ca)
		MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ct:CertTemplate)-[:PublishedTo]->(ca:EnterpriseCA)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpansionSuffixPushdown})
	require.Contains(t, plan.LoweringPlan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		SuffixLength:      2,
		SuffixStartStep:   1,
		SuffixEndStep:     2,
		ApplySupplemental: true,
		Reason:            "supplemental suffix prefilter retained for unobserved continuation",
	})
}

func TestLoweringPlanReportsCountStoreFastPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		query    string
		expected CountStoreFastPathDecision
	}{
		{
			name:  "node count",
			query: "MATCH (n:Group) RETURN count(n)",
			expected: CountStoreFastPathDecision{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				BindingSymbol:  "n",
				Target:         CountStoreFastPathNode,
				KindSymbols:    []string{"Group"},
			},
		},
		{
			name:  "node count star",
			query: "MATCH (:Group) RETURN count(*)",
			expected: CountStoreFastPathDecision{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				Target:         CountStoreFastPathNode,
				KindSymbols:    []string{"Group"},
			},
		},
		{
			name:  "edge count",
			query: "MATCH ()-[r:MemberOf]->() RETURN count(r)",
			expected: CountStoreFastPathDecision{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				BindingSymbol:  "r",
				Target:         CountStoreFastPathEdge,
				KindSymbols:    []string{"MemberOf"},
			},
		},
		{
			name:  "edge count star",
			query: "MATCH ()-[:MemberOf]->() RETURN count(*)",
			expected: CountStoreFastPathDecision{
				QueryPartIndex: 0,
				ClauseIndex:    0,
				PatternIndex:   0,
				Target:         CountStoreFastPathEdge,
				KindSymbols:    []string{"MemberOf"},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			require.NoError(t, err)

			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringCountStoreFastPath})
			require.Equal(t, []CountStoreFastPathDecision{testCase.expected}, plan.LoweringPlan.CountStoreFastPath)
		})
	}
}

func TestLoweringPlanPlacesBindingPredicates(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]->(ca:EnterpriseCA)
		WHERE ca.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringPredicatePlacement})
	require.Len(t, plan.LoweringPlan.PredicatePlacement, 1)
	require.Equal(t, TraversalStepTarget{
		QueryPartIndex: 0,
		ClauseIndex:    0,
		PatternIndex:   0,
		StepIndex:      1,
	}, plan.LoweringPlan.PredicatePlacement[0].Target)
	require.Equal(t, []string{"ca"}, plan.LoweringPlan.PredicatePlacement[0].Attachment.BindingSymbols)
	require.Equal(t, []PredicateAttachment{plan.LoweringPlan.PredicatePlacement[0].Attachment}, plan.LoweringPlan.ExpansionSuffixPushdown[0].PredicateAttachments)
}

func TestLoweringPlanDoesNotPlaceCrossClauseBindingPredicates(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:Group)
		WHERE n.objectid = 'S-1-5-21-1'
		MATCH p = (n)-[:MemberOf*1..]->(ca:EnterpriseCA)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotEmpty(t, plan.PredicateAttachments)
	require.Empty(t, plan.LoweringPlan.PredicatePlacement)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringPredicatePlacement})
}

func TestLoweringPlanReportsExpandInto(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a:Group)
		MATCH (b:Group)
		MATCH p = (a)-[:MemberOf]->(b)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpandIntoDetection})
	require.Equal(t, []ExpandIntoDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      0,
		},
	}}, plan.LoweringPlan.ExpandInto)
}

func TestLoweringPlanReportsExpandIntoForAnonymousContinuationEndpoint(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (d:Domain)
		MATCH p = (ca:EnterpriseCA)-[:IssuedSignedBy|EnterpriseCAFor*1..]->(:RootCA)-[:RootCAFor]->(d)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.ExpandInto, ExpandIntoDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      1,
		},
	})
}

func TestLoweringPlanReportsTraversalDirectionForConstrainedRightEndpoint(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(ca:EnterpriseCA)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Flip:   true,
		Reason: traversalDirectionReasonRightConstrained,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanReportsTraversalDirectionForBoundRightEndpoint(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (ca:EnterpriseCA)
		MATCH p = (n)-[:MemberOf*1..]->(ca)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Flip:   true,
		Reason: traversalDirectionReasonRightBound,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanSkipsTraversalDirectionWhenLeftEndpointHasBindingPredicate(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(ca:EnterpriseCA)
		WHERE n.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanSkipsTraversalDirectionWhenLeftEndpointHasRegionPredicate(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		WITH 'target' AS name
		MATCH p = (n)-[:MemberOf]->(ca:EnterpriseCA)
		WHERE n.name STARTS WITH name
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanReportsTraversalDirectionForRightEndpointPredicate(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(ca)
		WHERE ca.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Flip:   true,
		Reason: traversalDirectionReasonRightPredicate,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanReportsTraversalDirectionForBoundLeftExpansionToConstrainedRightEndpoint(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true AND u.enabled = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
		RETURN c
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Flip:   true,
		Reason: traversalDirectionReasonRightConstrained,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanSkipsBoundLeftDirectionForSelectiveSource(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.objectid = 'S-1-5-21-1-1100'
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
		RETURN c
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Reason: traversalDirectionReasonBoundSourceSelective,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanSkipsBoundLeftDirectionAfterPriorLimit(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		WITH u
		LIMIT 10
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
		RETURN c
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Reason: traversalDirectionReasonBoundSourceSelective,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanSkipsBoundLeftDirectionAfterGreedyProjectionLimit(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		WITH *
		LIMIT 10
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
		RETURN c
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Reason: traversalDirectionReasonBoundSourceSelective,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanCarriesBindingsAcrossNilWithPart(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User {objectid: 'S-1-5-21-1-1000'})
		WITH u
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {name: 'target'})
		RETURN c
	`)
	require.NoError(t, err)
	require.NotNil(t, regularQuery.SingleQuery.MultiPartQuery)
	require.NotEmpty(t, regularQuery.SingleQuery.MultiPartQuery.Parts)
	regularQuery.SingleQuery.MultiPartQuery.Parts[0].With = nil

	plan, err := BuildLoweringPlan(regularQuery, nil)
	require.NoError(t, err)
	require.Contains(t, plan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Contains(t, plan.TraversalDirection, TraversalDirectionDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Reason: traversalDirectionReasonBoundSourceSelective,
	})
}

func TestLoweringPlanAllowsUniqueRightEndpointAfterPriorLimit(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		WITH u
		LIMIT 10
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer {objectid: 'S-1-5-21-1-2000'})
		RETURN c
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Flip:   true,
		Reason: traversalDirectionReasonRightConstrained,
	}}, plan.LoweringPlan.TraversalDirection)
}

func TestLoweringPlanReportsAggregateTraversalCountForBoundExpansionCount(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true AND u.enabled = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
		WITH DISTINCT u, COUNT(c) AS adminCount
		RETURN u
		ORDER BY adminCount DESC
		LIMIT 100
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Equal(t, []TraversalDirectionDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Reason: traversalDirectionReasonTerminalKindOnlyEstimateWide,
	}}, plan.LoweringPlan.TraversalDirection)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringAggregateTraversalCount})
	require.Equal(t, []AggregateTraversalCountDecision{{
		QueryPartIndex: 0,
		SourceSymbol:   "u",
		TerminalSymbol: "c",
		CountAlias:     "adminCount",
		Limit:          100,
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
	}}, plan.LoweringPlan.AggregateTraversalCount)
}

func TestLoweringPlanReportsAggregateTraversalCountForRowCount(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true AND u.enabled = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
		WITH DISTINCT u, COUNT(*) AS adminCount
		RETURN u
		ORDER BY adminCount DESC
		LIMIT 100
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringAggregateTraversalCount})
	require.Equal(t, "adminCount", plan.LoweringPlan.AggregateTraversalCount[0].CountAlias)
}

func TestLoweringPlanReportsAggregateTraversalCountWhenReturningCountAlias(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
		WITH DISTINCT u, COUNT(c) AS adminCount
		RETURN u AS user, adminCount AS privileges
		ORDER BY privileges DESC
		LIMIT 100
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringAggregateTraversalCount})

	shape, ok := AggregateTraversalCountShapeForQuery(plan.Query)
	require.True(t, ok)
	require.Equal(t, "user", shape.ReturnSourceAlias)
	require.True(t, shape.ReturnCount)
	require.Equal(t, "privileges", shape.ReturnCountAlias)
}

func TestLoweringPlanReportsAggregateTraversalCountWithTerminalFilter(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
		WHERE c.enabled = true
		WITH DISTINCT u, COUNT(c) AS adminCount
		RETURN u
		ORDER BY adminCount DESC
		LIMIT 100
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringAggregateTraversalCount})
}

func TestLoweringPlanSkipsAggregateTraversalCountWithCorrelatedTerminalFilter(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.hasspn = true
		MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
		WHERE c.name = u.name
		WITH DISTINCT u, COUNT(c) AS adminCount
		RETURN u
		ORDER BY adminCount DESC
		LIMIT 100
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringAggregateTraversalCount})
}

func TestLoweringPlanSkipsSuffixPushdownAfterRightEndpointPredicateDirectionFlip(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf*1..]->(ca)-[:TrustedForNTAuth]->(d:Domain)
		WHERE ca.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringTraversalDirection})
	require.Empty(t, plan.LoweringPlan.ExpansionSuffixPushdown)
}

func TestLoweringPlanReportsShortestPathStrategyForEndpointPredicates(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s)-[:MemberOf*1..]->(e))
		WHERE s.name = 'source' AND e.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathStrategy})
	require.Equal(t, []ShortestPathStrategyDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Strategy: ShortestPathStrategyBidirectional,
		Reason:   shortestPathStrategyReasonEndpointPredicates,
	}}, plan.LoweringPlan.ShortestPathStrategy)
	require.Equal(t, []ShortestPathFilterDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode:   ShortestPathFilterEndpointPair,
		Reason: shortestPathFilterReasonEndpointPairPredicates,
	}}, plan.LoweringPlan.ShortestPathFilter)
}

func TestLoweringPlanSelectsQualifiedSingletonDistanceExecutor(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..16]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.Equal(t, "SP", decision.Family)
	require.Equal(t, "static", decision.SelectionMode)
	require.Equal(t, "sp-static-v3", decision.SelectorVersion)
	require.Equal(t, []ShortestPathExecutor{
		ShortestPathExecutorIncumbentWorkspace,
		ShortestPathExecutorS1ArrayBFS,
		ShortestPathExecutorS2TraceRelation,
		ShortestPathExecutorS3Unidirectional,
		ShortestPathExecutorS3EdgeM0,
	}, decision.PlannedCandidates)
	require.Equal(t, ShortestPathExecutorS3Unidirectional, decision.SelectedExecutor)
	require.Equal(t, ShortestPathExecutorIncumbentWorkspace, decision.FallbackExecutor)
	require.Empty(t, decision.FallbackReason)
	require.Equal(t, ShortestPathObservationDistance, decision.ObservationMode)
	require.True(t, decision.StructurallyEligible)
	require.Equal(t, int64(1), decision.MinimumDepth)
	require.Equal(t, int64(16), decision.MaximumDepth)
	require.True(t, decision.ExperimentalWinner)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathExecutor})
}

func TestLoweringPlanShortestExecutorV3ContainmentMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, pattern, observation string
		executor                   ShortestPathExecutor
		reason                     string
		direction                  graph.Direction
		physicalExpansion          ShortestPathPhysicalExpansion
		topology                   ShortestPathTopologyClassification
		kindCount                  int
		untyped                    bool
		staticEligible             bool
	}{
		{name: "outbound distance depth 64 two kinds", pattern: `(s)-[:MemberOf|Contains*1..64]->(e)`, observation: `length(p)`, executor: ShortestPathExecutorS3Unidirectional, direction: graph.DirectionOutbound, physicalExpansion: ShortestPathPhysicalExpansionStartID, topology: ShortestPathTopologyPhysicalOutbound, kindCount: 2, staticEligible: true},
		{name: "outbound one path one kind", pattern: `(s)-[:MemberOf*1..16]->(e)`, observation: `p`, executor: ShortestPathExecutorS3EdgeM0, direction: graph.DirectionOutbound, physicalExpansion: ShortestPathPhysicalExpansionStartID, topology: ShortestPathTopologyPhysicalOutbound, kindCount: 1, staticEligible: true},
		{name: "outbound one path two kinds", pattern: `(s)-[:MemberOf|Contains*1..16]->(e)`, observation: `p`, executor: ShortestPathExecutorIncumbentWorkspace, reason: ShortestPathFallbackNonSingleKindPathState, direction: graph.DirectionOutbound, physicalExpansion: ShortestPathPhysicalExpansionStartID, topology: ShortestPathTopologyPhysicalOutbound, kindCount: 2},
		{name: "outbound one path wildcard", pattern: `(s)-[*1..16]->(e)`, observation: `p`, executor: ShortestPathExecutorIncumbentWorkspace, reason: ShortestPathFallbackNonSingleKindPathState, direction: graph.DirectionOutbound, physicalExpansion: ShortestPathPhysicalExpansionStartID, topology: ShortestPathTopologyPhysicalOutbound, untyped: true},
		{name: "inbound distance depth one", pattern: `(s)<-[:MemberOf*0..1]-(e)`, observation: `length(p)`, executor: ShortestPathExecutorS3Unidirectional, direction: graph.DirectionInbound, physicalExpansion: ShortestPathPhysicalExpansionEndID, topology: ShortestPathTopologyPhysicalInboundShallow, kindCount: 1, staticEligible: true},
		{name: "inbound path depth one", pattern: `(s)<-[:MemberOf*1..1]-(e)`, observation: `p`, executor: ShortestPathExecutorS3EdgeM0, direction: graph.DirectionInbound, physicalExpansion: ShortestPathPhysicalExpansionEndID, topology: ShortestPathTopologyPhysicalInboundShallow, kindCount: 1, staticEligible: true},
		{name: "inbound distance depth two", pattern: `(s)<-[:MemberOf*1..2]-(e)`, observation: `length(p)`, executor: ShortestPathExecutorIncumbentWorkspace, reason: ShortestPathFallbackDeepInboundUnqualified, direction: graph.DirectionInbound, physicalExpansion: ShortestPathPhysicalExpansionEndID, topology: ShortestPathTopologyPhysicalInboundDeep, kindCount: 1},
		{name: "inbound path depth 64 two kinds uses direction reason", pattern: `(s)<-[:MemberOf|Contains*1..64]-(e)`, observation: `p`, executor: ShortestPathExecutorIncumbentWorkspace, reason: ShortestPathFallbackDeepInboundUnqualified, direction: graph.DirectionInbound, physicalExpansion: ShortestPathPhysicalExpansionEndID, topology: ShortestPathTopologyPhysicalInboundDeep, kindCount: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fmt.Sprintf(`
				MATCH p = shortestPath(%s)
				WHERE id(s) = $start_id AND id(e) = $end_id
				RETURN %s
			`, test.pattern, test.observation))
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
			decision := plan.LoweringPlan.ShortestPathExecutor[0]
			require.Equal(t, "sp-static-v3", decision.SelectorVersion)
			require.True(t, decision.StructurallyEligible)
			require.Equal(t, test.staticEligible, decision.StaticallyEligible)
			require.Equal(t, test.executor, decision.SelectedExecutor)
			require.Equal(t, test.reason, decision.FallbackReason)
			require.Equal(t, test.direction, decision.Direction)
			require.Equal(t, test.physicalExpansion, decision.PhysicalExpansion)
			require.Equal(t, test.topology, decision.TopologyClassification)
			require.Equal(t, test.kindCount, decision.RelationshipKindCount)
			require.Equal(t, test.untyped, decision.UntypedRelationship)
		})
	}
}

func TestLoweringPlanShortestExecutorV3PreservesStructuralReasonPrecedence(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf|Contains*1..64]-(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.False(t, decision.StructurallyEligible)
	require.False(t, decision.StaticallyEligible)
	require.Equal(t, ShortestPathFallbackDirectionless, decision.FallbackReason)
}

func TestLoweringPlanShortestExecutorRejectsUnsupportedMinimumDepth(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*2..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.False(t, decision.StructurallyEligible)
	require.Equal(t, int64(2), decision.MinimumDepth)
	require.Equal(t, int64(4), decision.MaximumDepth)
	require.Equal(t, ShortestPathFallbackUnsupportedDepth, decision.FallbackReason)
}

func TestLoweringPlanShortestExecutorRetainsZeroMaximumDepthInDiagnostics(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*0..0]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.True(t, decision.StructurallyEligible)
	require.Zero(t, decision.MinimumDepth)
	require.Zero(t, decision.MaximumDepth)

	diagnostic, err := json.Marshal(decision)
	require.NoError(t, err)
	require.Contains(t, string(diagnostic), `"maximum_depth":0`)
}

func TestLoweringPlanShortestExecutorUsesStatementWideCallCount(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH p
		MATCH q = shortestPath((x)-[:MemberOf*1..4]->(y))
		WHERE id(x) = $other_start_id AND id(y) = $other_end_id
		RETURN length(p), length(q)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 2)
	for _, decision := range plan.LoweringPlan.ShortestPathExecutor {
		require.False(t, decision.StructurallyEligible)
		require.Equal(t, ShortestPathFallbackMultiplePathCalls, decision.FallbackReason)
	}
}

func TestLoweringPlanShortestExecutorUsesStatementWideReadOnlyFact(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH p
		CREATE (:Group {name: 'updated'})
		RETURN length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.False(t, decision.StructurallyEligible)
	require.Equal(t, ShortestPathFallbackMutation, decision.FallbackReason)
}

func TestLoweringPlanShortestExecutorObservationModeRequiresPathForNodes(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		RETURN nodes(p)
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, ShortestPathObservationOnePath, plan.LoweringPlan.ShortestPathExecutor[0].ObservationMode)
}

func TestLoweringPlanShortestExecutorRequiresKnownObservationMode(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN s
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.Equal(t, ShortestPathObservationUnknown, decision.ObservationMode)
	require.False(t, decision.StructurallyEligible)
}

func TestLoweringPlanShortestExecutorRejectsAdditionalRowSources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, query, reason string
	}{
		{
			name: "unwind source",
			query: `
				UNWIND [1, 2] AS source
				MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = $start_id AND id(e) = $end_id
				RETURN length(p)
			`,
			reason: ShortestPathFallbackCorrelatedEndpoints,
		},
		{
			name: "additional match pattern",
			query: `
				MATCH (source), p = shortestPath((s)-[:MemberOf*1..4]->(e))
				WHERE id(s) = $start_id AND id(e) = $end_id
				RETURN length(p)
			`,
			reason: ShortestPathFallbackMultipleEndpointPairs,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), test.query)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
			decision := plan.LoweringPlan.ShortestPathExecutor[0]
			require.False(t, decision.StructurallyEligible)
			require.Equal(t, test.reason, decision.FallbackReason)
		})
	}
}

func TestLoweringPlanRecordsStableShortestExecutorFallbackCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, query, reason string
	}{
		{name: "all shortest", query: `MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) RETURN p`, reason: ShortestPathFallbackAllShortestPaths},
		{name: "directionless", query: `MATCH p = shortestPath((s)-[:MemberOf*1..4]-(e)) RETURN p`, reason: ShortestPathFallbackDirectionless},
		{name: "relationship variable", query: `MATCH p = shortestPath((s)-[r:MemberOf*1..4]->(e)) RETURN p`, reason: ShortestPathFallbackRelationshipVariable},
		{name: "open depth", query: `MATCH p = shortestPath((s)-[:MemberOf*1..]->(e)) RETURN p`, reason: ShortestPathFallbackUnsupportedDepth},
		{name: "non singleton", query: `MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e)) RETURN p`, reason: ShortestPathFallbackNonSingletonID},
		{name: "multiple id equalities", query: `MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = 1 AND id(s) = 2 AND id(e) = 3 RETURN p`, reason: ShortestPathFallbackMultipleIDEqualities},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), test.query)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.NotEmpty(t, plan.LoweringPlan.ShortestPathExecutor)
			require.Equal(t, test.reason, plan.LoweringPlan.ShortestPathExecutor[0].FallbackReason)
		})
	}
}

func TestLoweringPlanReportsShortestPathStrategyForBoundEndpointPairs(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a:Group)
		MATCH (b:EnterpriseCA)
		MATCH p = shortestPath((a)-[:MemberOf*1..]->(b))
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathStrategy})
	require.Equal(t, []ShortestPathStrategyDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Strategy: ShortestPathStrategyBidirectional,
		Reason:   shortestPathStrategyReasonBoundEndpointPairs,
	}}, plan.LoweringPlan.ShortestPathStrategy)
}

func TestLoweringPlanSkipsShortestPathStrategyForLabelOnlyEndpoints(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s:Group)-[:MemberOf*1..]->(e:EnterpriseCA))
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.ShortestPathStrategy)
}

func TestLoweringPlanReportsShortestPathTerminalFilter(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s:Group {name: 'source'})
		MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
		WHERE e.name = 'target'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathFilter})
	require.Equal(t, []ShortestPathFilterDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode:   ShortestPathFilterTerminal,
		Reason: shortestPathFilterReasonTerminalPredicate,
	}}, plan.LoweringPlan.ShortestPathFilter)
}

func TestLoweringPlanReportsShortestPathTerminalFilterForKindOnlyTerminal(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s:Group)-[:MemberOf|GenericAll|AdminTo*1..]->(t:Tag_Tier_Zero))
		WHERE s.objectid ENDS WITH '-513' AND s <> t
		RETURN p
		LIMIT 1000
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathStrategy})
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathFilter})
	require.Equal(t, []ShortestPathFilterDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode:   ShortestPathFilterTerminal,
		Reason: shortestPathFilterReasonTerminalPredicate,
	}}, plan.LoweringPlan.ShortestPathFilter)
}

func TestLoweringPlanReportsTraversalLimitPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n:Group)-[:MemberOf]->(m:Group)
		RETURN p
		LIMIT 1
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringLimitPushdown})
	require.Equal(t, []LimitPushdownDecision{{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode: LimitPushdownTraversalCTE,
	}}, plan.LoweringPlan.LimitPushdown)
}

func TestLoweringPlanReportsShortestPathLimitPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..]->(e))
		WHERE s.name = 'source' AND e.name = 'target'
		RETURN p
		LIMIT 1
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringLimitPushdown})
	require.Contains(t, plan.LoweringPlan.LimitPushdown, LimitPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    0,
			PatternIndex:   0,
			StepIndex:      0,
		},
		Mode: LimitPushdownShortestPathHarness,
	})
}

func TestLoweringPlanSkipsAllShortestPathLimitPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s)-[:MemberOf*1..]->(e))
		WHERE s.name = 'source' AND e.name = 'target'
		RETURN p
		LIMIT 1
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.LimitPushdown)
}

func TestLoweringPlanSkipsOptionalMatchLimitPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n)-[:MemberOf]->(m:Group)
		RETURN p
		LIMIT 1
	`)
	require.NoError(t, err)
	require.Len(t, regularQuery.SingleQuery.SinglePartQuery.ReadingClauses, 1)
	regularQuery.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Optional = true

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.LimitPushdown)
}

func TestDeclareReadingClauseSelectivitySkipsOptionalMatch(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n {objectid: 'S-1-5-21-1-1000'})
		OPTIONAL MATCH (m {objectid: 'S-1-5-21-1-2000'})
		RETURN n, m
	`)
	require.NoError(t, err)

	selectivity := map[string]boundSourceSelectivity{}
	declareReadingClauseSelectivity(selectivity, regularQuery.SingleQuery.SinglePartQuery.ReadingClauses)

	require.Equal(t, boundSourceSelectivityUnique, selectivity["n"])
	require.NotContains(t, selectivity, "m")
}

func TestSelectReferencesOnlyLocalIdentifiersValidatesJoinConstraintsIncrementally(t *testing.T) {
	t.Parallel()

	var (
		tableRef = func(alias pgsql.Identifier) pgsql.TableReference {
			return pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(alias),
			}
		}
		selectBody = pgsql.Select{
			Projection: []pgsql.SelectItem{
				pgsql.CompoundIdentifier{pgsql.Identifier("a"), pgsql.ColumnID},
			},
			From: []pgsql.FromClause{{
				Source: tableRef(pgsql.Identifier("a")),
				Joins: []pgsql.Join{{
					Table: tableRef(pgsql.Identifier("b")),
					JoinOperator: pgsql.JoinOperator{
						Constraint: pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{pgsql.Identifier("b"), pgsql.ColumnID},
							pgsql.OperatorEquals,
							pgsql.CompoundIdentifier{pgsql.Identifier("c"), pgsql.ColumnID},
						),
					},
				}, {
					Table: tableRef(pgsql.Identifier("c")),
				}},
			}},
		}
	)

	require.False(t, SelectReferencesOnlyLocalIdentifiers(selectBody, pgsql.NewIdentifierSet()))
}

func TestFlattenConjunctionHandlesValueBinaryExpressions(t *testing.T) {
	t.Parallel()

	var (
		left  = pgsql.NewLiteral(true, pgsql.Boolean)
		right = pgsql.NewLiteral(false, pgsql.Boolean)
		expr  = pgsql.BinaryExpression{
			LOperand: left,
			Operator: pgsql.OperatorAnd,
			ROperand: right,
		}
	)

	terms := FlattenConjunction(expr)

	require.Len(t, terms, 2)
	require.Equal(t, left, terms[0])
	require.Equal(t, right, terms[1])
}

func TestQueryReferencesOnlyLocalIdentifiersAllowsEmptyWith(t *testing.T) {
	t.Parallel()

	query := pgsql.Query{
		CommonTableExpressions: &pgsql.With{},
		Body: pgsql.Select{
			Projection: []pgsql.SelectItem{
				pgsql.CompoundIdentifier{pgsql.Identifier("n0"), pgsql.ColumnID},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(pgsql.Identifier("n0")),
				},
			}},
		},
	}

	require.True(t, QueryReferencesOnlyLocalIdentifiers(query, pgsql.NewIdentifierSet()))
}

func TestFromExpressionReferencesOnlyLocalIdentifiersHandlesLateralSubquery(t *testing.T) {
	t.Parallel()

	lateralSubquery := pgsql.LateralSubquery{
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.CompoundIdentifier{pgsql.Identifier("outer"), pgsql.ColumnID},
				},
			},
		},
	}

	require.True(t, FromExpressionReferencesOnlyLocalIdentifiers(lateralSubquery, pgsql.AsIdentifierSet(pgsql.Identifier("outer"))))
	require.False(t, FromExpressionReferencesOnlyLocalIdentifiers(lateralSubquery, pgsql.NewIdentifierSet()))
}

func TestMeasureSelectivityPopReturnsTopFrame(t *testing.T) {
	t.Parallel()

	visitor := newMeasureSelectivityVisitor(NewSelectivityModel(nil))
	visitor.addSelectivity(7)
	visitor.pushSelectivity(11)
	visitor.addSelectivity(13)

	require.Equal(t, 24, visitor.popSelectivity())
	require.Equal(t, 7, visitor.Selectivity())
}

func TestMeasureSelectivityScoresIDBonusOnlyForPointPredicates(t *testing.T) {
	t.Parallel()

	var (
		model    = NewSelectivityModel(nil)
		idRef    = pgsql.CompoundIdentifier{pgsql.Identifier("n0"), pgsql.ColumnID}
		literal  = pgsql.NewLiteral(1, pgsql.Int)
		equality = pgsql.NewBinaryExpression(idRef, pgsql.OperatorEquals, literal)
		rangeOp  = pgsql.NewBinaryExpression(idRef, pgsql.OperatorGreaterThan, literal)
		notEqual = pgsql.NewBinaryExpression(idRef, pgsql.OperatorNotEquals, literal)
	)

	equalityScore, err := model.Measure(equality)
	require.NoError(t, err)
	require.Equal(t, selectivityWeightNarrowSearch+selectivityWeightEntityIDReference, equalityScore)

	rangeScore, err := model.Measure(rangeOp)
	require.NoError(t, err)
	require.Equal(t, selectivityWeightRangeComparison, rangeScore)

	notEqualScore, err := model.Measure(notEqual)
	require.NoError(t, err)
	require.Equal(t, selectivityWeightNotEquals, notEqualScore)
}

func TestCollectReferencedSourceIdentifiersIgnoresMatchDeclarations(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n)-[r:MemberOf]->(m)
		RETURN m
	`)
	require.NoError(t, err)

	references, err := collectReferencedSourceIdentifiers(regularQuery)
	require.NoError(t, err)
	require.NotContains(t, references, "n")
	require.NotContains(t, references, "r")
	require.Contains(t, references, "m")
}

func TestLoweringPlanSkipsDirectionlessExpansionSuffixPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (n:Group)-[:MemberOf*0..]->(m)-[:Enroll]-(ca:EnterpriseCA)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Empty(t, plan.LoweringPlan.ExpansionSuffixPushdown)
}

func TestPredicateAttachmentRuleAssignsSingleBindingPredicates(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), adcsQuery)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.PredicateAttachments, 2)

	require.Equal(t, PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     0,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeBinding,
		BindingSymbols:  []string{"n"},
		Dependencies:    []string{"n"},
	}, plan.PredicateAttachments[0])

	require.Equal(t, PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     2,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeBinding,
		BindingSymbols:  []string{"ct"},
		Dependencies:    []string{"ct"},
	}, plan.PredicateAttachments[1])
}

func TestPredicateAttachmentRuleKeepsMultiBindingPredicatesAtRegionScope(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)-[:MemberOf]->(b)
		WHERE a.objectid = b.objectid
		RETURN a
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.PredicateAttachments, 1)

	require.Equal(t, PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     0,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeRegion,
		BindingSymbols:  []string{"a", "b"},
		Dependencies:    []string{"a", "b"},
	}, plan.PredicateAttachments[0])
}

func firstNodeSymbol(readingClause *cypher.ReadingClause) string {
	if readingClause == nil || readingClause.Match == nil || len(readingClause.Match.Pattern) == 0 {
		return ""
	}

	nodePattern, ok := singleNodePattern(readingClause.Match.Pattern[0])
	if !ok || nodePattern.Variable == nil {
		return ""
	}

	return nodePattern.Variable.Symbol
}

func TestConservativePatternReorderingMovesIndependentNodeAnchorsEarlier(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)
		MATCH (b:Group {objectid: 'target'})
		MATCH p = (a)-[:MemberOf]->(b)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: true,
		},
		{
			Name:    "PredicateAttachment",
			Applied: false,
		},
	}, plan.Rules)

	readingClauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
	require.Equal(t, "b", firstNodeSymbol(readingClauses[0]))
	require.Equal(t, "a", firstNodeSymbol(readingClauses[1]))
	require.Len(t, readingClauses[2].Match.Pattern[0].PatternElements, 3)
}

func TestConservativePatternReorderingKeepsDependentAnchorsInPlace(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)
		MATCH (b:Group)
		WHERE b.name = a.name
		RETURN b
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: false,
		},
		{
			Name:    "PredicateAttachment",
			Applied: true,
		},
	}, plan.Rules)

	readingClauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
	require.Equal(t, "a", firstNodeSymbol(readingClauses[0]))
	require.Equal(t, "b", firstNodeSymbol(readingClauses[1]))
}

func TestConservativePatternReorderingUsesSelectivityWithinDependencySafeRegion(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a:Group)
		MATCH (b:User {objectid: 'target'})
		MATCH p = (a)-[:MemberOf]->(b)
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: true,
		},
		{
			Name:    "PredicateAttachment",
			Applied: false,
		},
	}, plan.Rules)

	readingClauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
	require.Equal(t, "b", firstNodeSymbol(readingClauses[0]))
	require.Equal(t, "a", firstNodeSymbol(readingClauses[1]))
	require.Len(t, readingClauses[2].Match.Pattern[0].PatternElements, 3)
}

func TestConservativePatternReorderingPinsUnresolvedExternalDependencies(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a)
		WHERE a.name = external.name
		MATCH (b:User {objectid: 'target'})
		RETURN b
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: false,
		},
		{
			Name:    "PredicateAttachment",
			Applied: true,
		},
	}, plan.Rules)

	readingClauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
	require.Equal(t, "a", firstNodeSymbol(readingClauses[0]))
	require.Equal(t, "b", firstNodeSymbol(readingClauses[1]))
}
