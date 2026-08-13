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

// testRule is a configurable optimizer rule used to assert rewrite ordering and error propagation.
type testRule struct {
	// name is the stable rule name returned to the optimizer.
	name string
}

// Name evaluates planner state needed for name.
func (s testRule) Name() string {
	return s.name
}

// Apply evaluates planner state needed for apply.
func (s testRule) Apply(plan *Plan) (bool, error) {
	return false, nil
}

// testBindingLookup supplies deterministic binding resolution to optimizer tests.
type testBindingLookup map[pgsql.Identifier]pgsql.DataType

// LookupDataType evaluates planner state needed for lookup data type.
func (s testBindingLookup) LookupDataType(identifier pgsql.Identifier) (pgsql.DataType, bool) {
	dataType, found := s[identifier]
	return dataType, found
}

// TestOptimizeCopiesAndAnalyzesQuery verifies that optimization preserves the input AST and records query-part metadata.
func TestOptimizeCopiesAndAnalyzesQuery(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fixedSuffixExpansionQuery)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.NotSame(t, regularQuery, plan.Query)
	require.Len(t, plan.Analysis.QueryParts, 1)
	require.Len(t, plan.Analysis.QueryParts[0].Regions, 1)
	require.Equal(t, []string{"p1", "p2"}, plan.Analysis.QueryParts[0].ProjectionDependencies)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: false,
		},
		{
			Name:    "InboundTraversalReversal",
			Applied: false,
		},
		{
			Name:    "PredicateAttachment",
			Applied: true,
		},
	}, plan.Rules)
	require.Len(t, plan.PredicateAttachments, 2)
}

// TestFieldRequirementAnalysisDistinguishesObservationBoundaries verifies that each consumer requests only the binding fields it observes.
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

// TestFieldRequirementAnalysisExpandsGreedyProjection verifies that RETURN * requires complete representations of visible bindings.
func TestFieldRequirementAnalysisExpandsGreedyProjection(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[r:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN *
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)

	bySymbol := map[string]FieldRequirementDecision{}
	for _, decision := range plan.LoweringPlan.FieldRequirements {
		bySymbol[decision.Symbol] = decision
	}

	require.NotContains(t, bySymbol, cypher.TokenLiteralAsterisk)
	require.Contains(t, bySymbol["p"].Fields, FieldRequirementFullPath)
	require.Contains(t, bySymbol["s"].Fields, FieldRequirementFullEntity)
	require.Contains(t, bySymbol["e"].Fields, FieldRequirementFullEntity)
	require.Contains(t, bySymbol["r"].Fields, FieldRequirementFullEntity)
	require.Contains(t, bySymbol["r"].Fields, FieldRequirementRelationshipIDs)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	require.Equal(t, ShortestPathObservationOnePath, plan.LoweringPlan.ShortestPathExecutor[0].ObservationMode)
}

// TestFieldRequirementAnalysisTreatsWithGreedyProjectionAsFullObservation verifies that WITH * prevents scalar-only path state.
func TestFieldRequirementAnalysisTreatsWithGreedyProjectionAsFullObservation(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		WITH *
		RETURN length(p)
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)

	for _, decision := range plan.LoweringPlan.FieldRequirements {
		if decision.QueryPartIndex == 0 && decision.Symbol == "p" {
			require.Contains(t, decision.Fields, FieldRequirementFullPath)
			return
		}
	}
	require.Fail(t, "missing path field-requirement decision")
}

// TestOptimizePlansFixedSuffixFanoutRewrite verifies that an eligible terminal suffix receives supplemental pushdown metadata.
func TestOptimizePlansFixedSuffixFanoutRewrite(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fixedSuffixExpansionQuery)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)

	predicateAttachment := PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     2,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeBinding,
		BindingSymbols:  []string{"predicate"},
		Dependencies:    []string{"predicate"},
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
		PredicateAttachments: []PredicateAttachment{predicateAttachment},
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
		Attachment: predicateAttachment,
		Placement:  PredicateAttachmentScopeBinding,
	})
}

// TestOptimizerRunsRulesAndRefreshesAnalysis verifies optimizer runs rules and refreshes analysis behavior.
func TestOptimizerRunsRulesAndRefreshesAnalysis(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n`)
	require.NoError(t, err)

	plan, err := NewOptimizer(testRule{name: "test"}).Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{{
		Name:    "test",
		Applied: false,
	}}, plan.Rules)
	require.Len(t, plan.Analysis.QueryParts, 1)
	require.Len(t, plan.Analysis.QueryParts[0].Regions, 1)
}

// TestDefaultPredicateAttachmentRuleReportsSkippedWhenNoPredicatesExist verifies default predicate attachment rule reports skipped when no predicates exist behavior.
func TestDefaultPredicateAttachmentRuleReportsSkippedWhenNoPredicatesExist(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{
		{
			Name:    "ConservativePatternReordering",
			Applied: false,
		},
		{
			Name:    "InboundTraversalReversal",
			Applied: false,
		},
		{
			Name:    "PredicateAttachment",
			Applied: false,
		},
	}, plan.Rules)
	require.Empty(t, plan.PredicateAttachments)
}

// TestLoweringPlanReportsProjectionPruning verifies that unused traversal bindings produce explicit pruning decisions.
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

// TestLoweringPlanProjectionPruningKeepsUpdateTargets verifies lowering plan projection pruning keeps update targets behavior.
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

// TestLoweringPlanReportsPatternPredicateProjectionPruning verifies lowering plan reports pattern predicate projection pruning behavior.
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

// TestLoweringPlanReportsPatternPredicateExistencePlacement verifies lowering plan reports pattern predicate existence placement behavior.
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

// TestLoweringPlanReportsTypedPatternPredicateExistencePlacement verifies lowering plan reports typed pattern predicate existence placement behavior.
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

// TestLoweringPlanReportsPatternPredicateClauseIndex verifies lowering plan reports pattern predicate clause index behavior.
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

// TestSelectivityModelPlansTraversalDirection verifies selectivity model plans traversal direction behavior.
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

// TestLoweringPlanReportsLatePathMaterialization verifies lowering plan reports late path materialization behavior.
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

// TestLoweringPlanReportsExactOneHopRangeExpansion verifies lowering plan reports exact one hop range expansion behavior.
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

// TestLoweringPlanReportsExactTwoHopRangeExpansion verifies lowering plan reports exact two hop range expansion behavior.
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

// TestExactRangeDependentPlanningRequiresDecision verifies that downstream planning changes only after exact-range expansion is selected.
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

// TestLoweringPlanSkipsExactRangeExpansionBeyondDepthCap verifies lowering plan skips exact range expansion beyond depth cap behavior.
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

// TestLoweringPlanSkipsUndirectedExactRangeExpansion verifies lowering plan skips undirected exact range expansion behavior.
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

// TestLoweringPlanSkipsExactOneHopRangeExpansionForNamedRelationshipBinding verifies lowering plan skips exact one hop range expansion for named relationship binding behavior.
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

// TestLoweringPlanSkipsExactOneHopRangeExpansionForShortestPath verifies lowering plan skips exact one hop range expansion for shortest path behavior.
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

// TestLoweringPlanReportsPathRelationshipPredicate verifies lowering plan reports path relationship predicate behavior.
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

// TestLoweringPlanReportsNonePathRelationshipPredicate verifies lowering plan reports none path relationship predicate behavior.
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

// TestLoweringPlanSkipsPathRelationshipPredicateForAllQuantifier verifies lowering plan skips path relationship predicate for all quantifier behavior.
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

// TestLoweringPlanSkipsPathRelationshipPredicateAfterWithProjection verifies lowering plan skips path relationship predicate after with projection behavior.
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

// TestLoweringPlanReportsExpansionSuffixPushdown verifies that an eligible fixed suffix produces a supplemental-search decision.
func TestLoweringPlanReportsExpansionSuffixPushdown(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH path = (root:ExpansionRoot)-[:Expand*0..16]->(boundary:ExpansionNode)-[:EnterSuffix]->(head:SuffixHead)
		RETURN path
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

// TestLoweringPlanReportsConservativeFixedSuffixSearchStrategy verifies that eligible suffix topology remains on the incumbent strategy unless qualified.
func TestLoweringPlanReportsConservativeFixedSuffixSearchStrategy(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = $root_key
		MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN path
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringExpansionSearchStrategy})
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, "fixed_suffix_expansion", decision.Family)
	require.Equal(t, ExpansionSearchPolicyOrientationProbeV1, decision.PlannedPolicy)
	require.Empty(t, decision.EmittedPolicy)
	require.Equal(t, "incumbent_default", decision.SelectionMode)
	require.Equal(t, "fixed-suffix-static-v1", decision.SelectorVersion)
	require.Equal(t, []ExpansionSearchStrategy{
		ExpansionSearchStepwiseForward,
		ExpansionSearchLateHydratedForward,
		ExpansionSearchFactoredSuffixForward,
		ExpansionSearchSuffixSeededReverse,
		ExpansionSearchBackwardViabilityForward,
	}, decision.PlannedCandidates)
	require.Equal(t, []ExpansionSearchStrategy{ExpansionSearchStepwiseForward}, decision.EmittedCandidates)
	require.Equal(t, ExpansionSearchExecutionBoundaryInlineStatement, decision.ExecutionBoundary)
	require.Equal(t, ExpansionSearchProbeCaps{
		RootRowLimit:              ExpansionSearchOrientationRootRowLimit,
		ReverseSeedRowLimit:       ExpansionSearchOrientationReverseSeedRowLimit,
		DirectionalDegreeRowLimit: ExpansionSearchOrientationDirectionalDegreeRowLimit,
	}, decision.ProbeCaps)
	require.Equal(t, ExpansionSearchAdmission{
		StateLimit:             ExpansionSearchOrientationStateLimit,
		RequiresCompleteProbes: true,
		FallbackStrategy:       ExpansionSearchStepwiseForward,
	}, decision.Admission)
	require.True(t, decision.StructurallyEligible)
	require.Contains(t, decision.EligibilityFacts, ExpansionSearchEligibilityFact{
		Name:     "qualified_fixed_suffix_topology",
		Eligible: true,
	})
	require.Equal(t, ExpansionSearchStepwiseForward, decision.SelectedStrategy)
	require.Equal(t, ExpansionSearchStepwiseForward, decision.FallbackStrategy)
	require.Equal(t, ExpansionSearchFallbackTournamentUnqualified, decision.FallbackReason)
	require.Equal(t, ExpansionSearchObservationFullPath, decision.ObservationMode)
	require.Equal(t, int64(0), decision.MinimumDepth)
	require.Equal(t, int64(16), decision.MaximumDepth)
	require.Equal(t, 3, decision.SuffixLength)
	require.Equal(t, "outbound", decision.LogicalDirection)
}

// TestLoweringPlanSelectsGuardedEndpointSeededExpansion verifies guarded endpoint seeding for one statement-wide variable expansion.
func TestLoweringPlanSelectsGuardedEndpointSeededExpansion(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)
		WHERE g.objectid ENDS WITH $suffix
		RETURN p
		LIMIT 1000
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.Equal(t, "fixed_prefix_terminal_expansion", decision.Family)
	require.Equal(t, ExpansionSearchPolicyEndpointGuardV1, decision.PlannedPolicy)
	require.Equal(t, ExpansionSearchPolicyEndpointGuardV1, decision.EmittedPolicy)
	require.Equal(t, []ExpansionSearchStrategy{ExpansionSearchStepwiseForward, ExpansionSearchEndpointSeededReverse}, decision.EmittedCandidates)
	require.Equal(t, ExpansionSearchExecutionBoundaryGuardedDualArm, decision.ExecutionBoundary)
	require.Equal(t, ExpansionSearchProbeCaps{ReverseSeedRowLimit: 32}, decision.ProbeCaps)
	require.Equal(t, ExpansionSearchAdmission{
		StateLimit:             4096,
		RequiresCompleteProbes: true,
		FallbackStrategy:       ExpansionSearchStepwiseForward,
	}, decision.Admission)
	require.True(t, decision.StructurallyEligible)
	require.True(t, decision.StaticallyEligible)
	require.Equal(t, ExpansionSearchEndpointSeededReverse, decision.SelectedStrategy)
	require.Equal(t, ExpansionSearchStepwiseForward, decision.FallbackStrategy)
	require.Equal(t, "static_guarded", decision.SelectionMode)
	require.Equal(t, "endpoint-seeded-guarded-v1", decision.SelectorVersion)
	require.Equal(t, "property_ends_with", decision.SeedPredicateClass)
	require.Equal(t, int64(32), decision.EndpointLimit)
	require.Equal(t, int64(4096), decision.StateLimit)
	require.Equal(t, 1, decision.PrefixLength)
	require.Equal(t, int64(1), decision.MinimumDepth)
	require.Equal(t, int64(15), decision.MaximumDepth)
	require.True(t, decision.HasFinalLimit)
	require.Empty(t, decision.FallbackReason)
	require.Contains(t, decision.EligibilityFacts, ExpansionSearchEligibilityFact{
		Name:     "single_variable_expansion_in_region",
		Eligible: true,
	})
}

// TestEndpointSeededExpansionKeepsIndependentMultipartRegionQualified verifies
// that an earlier traversal separated by WITH does not invalidate the existing
// guarded fixed-prefix region.
func TestEndpointSeededExpansionKeepsIndependentMultipartRegionQualified(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s)-[:MemberOf*0..]->(excluded:Group)
		WHERE excluded.objectid ENDS WITH '-516'
		WITH collect(s) AS exclude
		MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)
		WHERE g.objectid ENDS WITH $suffix AND NOT c IN exclude
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 2)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[1]
	require.Equal(t, "fixed_prefix_terminal_expansion", decision.Family)
	require.Contains(t, decision.EligibilityFacts, ExpansionSearchEligibilityFact{
		Name:     "single_variable_expansion_in_region",
		Eligible: true,
	})
	require.True(t, decision.StructurallyEligible)
	require.Equal(t, ExpansionSearchEndpointSeededReverse, decision.SelectedStrategy)
	require.Equal(t, ExpansionSearchPolicyEndpointGuardV1, decision.EmittedPolicy)
	require.Equal(t, []ExpansionSearchStrategy{ExpansionSearchStepwiseForward, ExpansionSearchEndpointSeededReverse}, decision.EmittedCandidates)
	require.Equal(t, ExpansionSearchExecutionBoundaryGuardedDualArm, decision.ExecutionBoundary)
	require.Empty(t, decision.FallbackReason)
}

// TestGuardedEndpointSeededExpansionFallbackReasons verifies stable rejection reasons for unsafe endpoint-seeded shapes.
func TestGuardedEndpointSeededExpansionFallbackReasons(t *testing.T) {
	for _, testCase := range []struct {
		// name labels the structural rejection case.
		name string
		// query produces the endpoint-seeding candidate under test.
		query string
		// reason is the expected stable fallback code.
		reason string
	}{
		{
			name:   "terminal not selective",
			query:  `MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group) RETURN p`,
			reason: ExpansionSearchFallbackTerminalNotSelective,
		},
		{
			name:   "zero depth",
			query:  `MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*0..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			reason: ExpansionSearchFallbackZeroDepth,
		},
		{
			name:   "directionless prefix",
			query:  `MATCH p = (c:Computer)-[:HasSession]-(:User)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			reason: ExpansionSearchFallbackDirectionlessPrefix,
		},
		{
			name:   "correlated terminal",
			query:  `MATCH (g:Group) MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			reason: ExpansionSearchFallbackCorrelatedTerminal,
		},
		{
			name:   "correlated terminal predicate",
			query:  `MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' AND g.tenant = c.tenant RETURN p`,
			reason: ExpansionSearchFallbackCorrelatedTerminal,
		},
		{
			name:   "nonterminal expansion",
			query:  `MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)-[:AdminTo]->() WHERE g.objectid ENDS WITH '-512' RETURN p`,
			reason: ExpansionSearchFallbackExpansionNotTerminal,
		},
		{
			name:   "mutation",
			query:  `MATCH (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' CREATE (:Computer) RETURN g`,
			reason: ExpansionSearchFallbackMutation,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.NotEmpty(t, plan.LoweringPlan.ExpansionSearchStrategy)
			decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
			require.Equal(t, "fixed_prefix_terminal_expansion", decision.Family)
			require.False(t, decision.StructurallyEligible)
			require.Equal(t, ExpansionSearchStepwiseForward, decision.SelectedStrategy)
			require.Equal(t, testCase.reason, decision.FallbackReason)
		})
	}
}

// TestGuardedEndpointSeededExpansionAcceptsTerminalIDEquality verifies that a singleton terminal ID is a selective reverse-search seed.
func TestGuardedEndpointSeededExpansionAcceptsTerminalIDEquality(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..8]->(g)
		WHERE id(g) = $terminal_id
		RETURN id(c), id(g)
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.True(t, decision.StructurallyEligible)
	require.Equal(t, "id_equality", decision.SeedPredicateClass)
	require.Equal(t, ExpansionSearchEndpointSeededReverse, decision.SelectedStrategy)
}

// TestFixedSuffixSearchRejectsPredicateFunctionReevaluation verifies that reordered function evaluation disqualifies suffix search.
func TestFixedSuffixSearchRejectsPredicateFunctionReevaluation(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)
		WHERE root.root_key = 'root'
		MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(:SuffixTerminal)
		WHERE root.marker = toString(1)
		RETURN root
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
	decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
	require.False(t, decision.StructurallyEligible)
	require.Equal(t, ExpansionSearchFallbackNonDeterministicPredicate, decision.FallbackReason)
	require.Contains(t, decision.EligibilityFacts, ExpansionSearchEligibilityFact{
		Name:     "deterministic_predicates",
		Eligible: false,
	})
}

// TestExpansionSearchObservationUsesExternalFieldRequirements verifies that downstream field requirements select the search observation mode.
func TestExpansionSearchObservationUsesExternalFieldRequirements(t *testing.T) {
	for _, testCase := range []struct {
		// name labels the downstream observation form.
		name string
		// projection contains the downstream expression being classified.
		projection string
		// observation is the expected search-state representation.
		observation ExpansionSearchObservationMode
	}{
		{
			name:        "endpoint IDs",
			projection:  "id(head), id(terminal)",
			observation: ExpansionSearchObservationEndpointIDs,
		},
		{
			name:        "ordered IDs",
			projection:  "length(path)",
			observation: ExpansionSearchObservationOrderedPathIDs,
		},
		{
			name:        "full path",
			projection:  "path",
			observation: ExpansionSearchObservationFullPath,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
				MATCH path = (root:ExpansionRoot)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
				RETURN `+testCase.projection)
			require.NoError(t, err)
			plan, err := Optimize(regularQuery)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
			require.Equal(t, testCase.observation, plan.LoweringPlan.ExpansionSearchStrategy[0].ObservationMode)
		})
	}
}

// TestExpansionSearchFinalizationRejectsVariableExpansionAcrossWith verifies that multiple statement-wide expansions prevent specialized search.
func TestExpansionSearchFinalizationRejectsVariableExpansionAcrossWith(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (root:ExpansionRoot)-[:Expand*0..16]->()-[:EnterSuffix]->(:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		WITH root, terminal
		MATCH (root)-[:Expand*0..4]->(other)
		RETURN id(terminal), id(other)
	`)
	require.NoError(t, err)
	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 2)
	require.Equal(t, ExpansionSearchFallbackMultipleVariableExpansions, plan.LoweringPlan.ExpansionSearchStrategy[0].FallbackReason)
	require.False(t, plan.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
}

// TestLoweringPlanReportsStableFixedSuffixSearchFallbackCodes verifies diagnostic codes for structurally unsafe suffix searches.
func TestLoweringPlanReportsStableFixedSuffixSearchFallbackCodes(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		// name labels the structural rejection case.
		name string
		// query produces the fixed-suffix candidate under test.
		query string
		// reason is the expected stable fallback code.
		reason string
	}{
		{
			name:   "no fixed suffix",
			query:  `MATCH (root)-[:Expand*0..16]->(head) RETURN id(head)`,
			reason: ExpansionSearchFallbackNoFixedSuffix,
		},
		{
			name:   "unbounded",
			query:  `MATCH (root)-[:Expand*0..]->()-[:EnterSuffix]->(head) RETURN id(head)`,
			reason: ExpansionSearchFallbackUnboundedDepth,
		},
		{
			name:   "short suffix",
			query:  `MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head) RETURN id(head)`,
			reason: ExpansionSearchFallbackSuffixTooShort,
		},
		{
			name:   "directionless",
			query:  `MATCH (root)-[:Expand*0..16]-()-[:EnterSuffix]->(head)-[:ContinueSuffix]->()-[:CompleteSuffix]->(terminal) RETURN id(head)`,
			reason: ExpansionSearchFallbackDirectionlessExpansion,
		},
		{
			name:   "directionless suffix",
			query:  `MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]-(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head)`,
			reason: ExpansionSearchFallbackDirectionlessSuffix,
		},
		{
			name:   "optional",
			query:  `OPTIONAL MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head)`,
			reason: ExpansionSearchFallbackOptionalMatch,
		},
		{
			name:   "shortest path",
			query:  `MATCH path = shortestPath((root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)) RETURN path`,
			reason: ExpansionSearchFallbackShortestPath,
		},
		{
			name:   "all shortest paths",
			query:  `MATCH path = allShortestPaths((root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)) RETURN path`,
			reason: ExpansionSearchFallbackAllShortestPaths,
		},
		{
			name:   "unbound root",
			query:  `MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head), id(terminal)`,
			reason: ExpansionSearchFallbackUnboundRoot,
		},
		{
			name:   "unsupported depth",
			query:  `MATCH (root)-[:Expand*0..65]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head)`,
			reason: ExpansionSearchFallbackUnsupportedDepth,
		},
		{
			name:   "relationship variable",
			query:  `MATCH (root)-[edges:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head)`,
			reason: ExpansionSearchFallbackRelationshipVariable,
		},
		{
			name:   "relationship predicate",
			query:  `MATCH (root)-[edges:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) WHERE edges.enabled = true RETURN id(head)`,
			reason: ExpansionSearchFallbackRelationshipPredicate,
		},
		{
			name:   "correlated suffix",
			query:  `MATCH (head:SuffixHead) MATCH path = (root:ExpansionRoot)-[:Expand*0..16]->()-[:EnterSuffix]->(head)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN path`,
			reason: ExpansionSearchFallbackCorrelatedSuffix,
		},
		{
			name:   "cross-region predicate",
			query:  `MATCH path = (root:ExpansionRoot)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) WHERE root.partition = head.partition RETURN path`,
			reason: ExpansionSearchFallbackCrossRegionPredicate,
		},
		{
			name:   "path predicate",
			query:  `MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) WHERE length(path) > 0 RETURN path`,
			reason: ExpansionSearchFallbackPathDependentPredicate,
		},
		{
			name:   "unsupported observation",
			query:  `MATCH path = (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(path)`,
			reason: ExpansionSearchFallbackUnsupportedObservation,
		},
		{
			name:   "mutation",
			query:  `MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) CREATE (created) RETURN id(head)`,
			reason: ExpansionSearchFallbackMutation,
		},
		{
			name:   "limit pushdown conflict",
			query:  `MATCH (root)-[:Expand*0..16]->()-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN id(head) LIMIT 10`,
			reason: ExpansionSearchFallbackLimitPushdownConflict,
		},
		{
			name:   "tournament unqualified",
			query:  `MATCH (root)-[:Other|Alternate*0..16]->()-[:A]->(head:X)-[:B]->(:Y)-[:C]->(terminal:Z) RETURN id(head)`,
			reason: ExpansionSearchFallbackTournamentUnqualified,
		},
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

// TestLoweringPlanIncludesConstrainedBoundEndpointInExpansionSuffix verifies that a pre-bound terminal remains part of suffix metadata.
func TestLoweringPlanIncludesConstrainedBoundEndpointInExpansionSuffix(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (terminal)
		MATCH path = (root:ExpansionRoot)-[:Expand*0..16]->(boundary:ExpansionNode)-[:EnterSuffix]->(middle:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal)
		RETURN path
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

// TestLoweringPlanReportsCountStoreFastPath verifies lowering plan reports count store fast path behavior.
func TestLoweringPlanReportsCountStoreFastPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// query retains the query while anonymous record is assembled or evaluated.
		query string
		// expected retains the expected while anonymous record is assembled or evaluated.
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

// TestLoweringPlanPlacesBindingPredicates verifies lowering plan places binding predicates behavior.
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
	// InboundTraversalReversal drives this pattern from the constrained ca:EnterpriseCA terminal
	// inward, so the ca predicate anchors at the now-leading step (StepIndex 0) rather than being
	// pushed into an expansion suffix.
	require.Equal(t, TraversalStepTarget{
		QueryPartIndex: 0,
		ClauseIndex:    0,
		PatternIndex:   0,
		StepIndex:      0,
	}, plan.LoweringPlan.PredicatePlacement[0].Target)
	require.Equal(t, []string{"ca"}, plan.LoweringPlan.PredicatePlacement[0].Attachment.BindingSymbols)
	require.Empty(t, plan.LoweringPlan.ExpansionSuffixPushdown)
}

// TestLoweringPlanDoesNotPlaceCrossClauseBindingPredicates verifies lowering plan does not place cross clause binding predicates behavior.
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

// TestLoweringPlanReportsExpandInto verifies lowering plan reports expand into behavior.
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

// TestLoweringPlanReportsExpandIntoForEndpointsCarriedAcrossWithAndUnwind verifies lowering plan reports expand into for endpoints carried across with and unwind behavior.
func TestLoweringPlanReportsExpandIntoForEndpointsCarriedAcrossWithAndUnwind(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a:Group), (b:Group)
		WITH a, b, [1, 2] AS copies
		UNWIND copies AS copy
		MATCH (a)-[:MemberOf]->(b)
		RETURN copy
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.ExpandInto, ExpandIntoDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
	})
}

// TestLoweringPlanReportsExpandIntoForNodeIntroducedByUnwind verifies lowering plan reports expand into for node introduced by unwind behavior.
func TestLoweringPlanReportsExpandIntoForNodeIntroducedByUnwind(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (a:Group), (b:Group)
		WITH b, [a] AS nodes
		UNWIND nodes AS source
		MATCH (source)-[:MemberOf]->(b)
		RETURN source
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.LoweringPlan.ExpandInto, ExpandIntoDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 1,
			ClauseIndex:    1,
			PatternIndex:   0,
			StepIndex:      0,
		},
	})
}

// TestLoweringPlanReportsExpandIntoForAnonymousContinuationEndpoint verifies lowering plan reports expand into for anonymous continuation endpoint behavior.
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

// TestLoweringPlanReportsTraversalDirectionForConstrainedRightEndpoint verifies lowering plan reports traversal direction for constrained right endpoint behavior.
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

// TestLoweringPlanReportsTraversalDirectionForBoundRightEndpoint verifies lowering plan reports traversal direction for bound right endpoint behavior.
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

// TestLoweringPlanSkipsTraversalDirectionWhenLeftEndpointHasBindingPredicate verifies lowering plan skips traversal direction when left endpoint has binding predicate behavior.
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

// TestLoweringPlanSkipsTraversalDirectionWhenLeftEndpointHasRegionPredicate verifies lowering plan skips traversal direction when left endpoint has region predicate behavior.
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

// TestLoweringPlanReportsTraversalDirectionForRightEndpointPredicate verifies lowering plan reports traversal direction for right endpoint predicate behavior.
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

// TestLoweringPlanReportsTraversalDirectionForBoundLeftExpansionToConstrainedRightEndpoint verifies lowering plan reports traversal direction for bound left expansion to constrained right endpoint behavior.
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

// TestLoweringPlanSkipsBoundLeftDirectionForSelectiveSource verifies lowering plan skips bound left direction for selective source behavior.
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

// TestLoweringPlanSkipsBoundLeftDirectionAfterPriorLimit verifies lowering plan skips bound left direction after prior limit behavior.
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

// TestLoweringPlanSkipsBoundLeftDirectionAfterGreedyProjectionLimit verifies lowering plan skips bound left direction after greedy projection limit behavior.
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

// TestLoweringPlanCarriesBindingsAcrossNilWithPart verifies lowering plan carries bindings across nil with part behavior.
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

// TestLoweringPlanAllowsUniqueRightEndpointAfterPriorLimit verifies lowering plan allows unique right endpoint after prior limit behavior.
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

// TestLoweringPlanReportsAggregateTraversalCountForBoundExpansionCount verifies lowering plan reports aggregate traversal count for bound expansion count behavior.
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

// TestLoweringPlanReportsAggregateTraversalCountForRowCount verifies lowering plan reports aggregate traversal count for row count behavior.
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

// TestLoweringPlanReportsAggregateTraversalCountWhenReturningCountAlias verifies lowering plan reports aggregate traversal count when returning count alias behavior.
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

// TestLoweringPlanReportsAggregateTraversalCountWithTerminalFilter verifies lowering plan reports aggregate traversal count with terminal filter behavior.
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

// TestLoweringPlanSkipsAggregateTraversalCountWithCorrelatedTerminalFilter verifies lowering plan skips aggregate traversal count with correlated terminal filter behavior.
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

// TestLoweringPlanSkipsSuffixPushdownAfterRightEndpointPredicateDirectionFlip verifies lowering plan skips suffix pushdown after right endpoint predicate direction flip behavior.
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

// TestLoweringPlanReportsShortestPathStrategyForEndpointPredicates verifies lowering plan reports shortest path strategy for endpoint predicates behavior.
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

// TestLoweringPlanSelectsQualifiedSingletonDistanceExecutor verifies scalar-distance selection for a statically bound endpoint pair.
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
		ShortestPathExecutorS0Direct,
		ShortestPathExecutorS1ArrayBFS,
		ShortestPathExecutorS2TraceRelation,
		ShortestPathExecutorS3Unidirectional,
		ShortestPathExecutorS3EdgeM0,
		ShortestPathExecutorS4CanonicalDistance,
		ShortestPathExecutorS4CanonicalWitness,
		ShortestPathExecutorI1CanonicalDistance,
		ShortestPathExecutorI1CanonicalWitness,
		ShortestPathExecutorI1CanonicalPredecessorWitness,
		ShortestPathExecutorB1AlternatingNodeDistance,
		ShortestPathExecutorB1AlternatingNodeWitness,
		ShortestPathExecutorB2SmallerCurrentLevelDistance,
		ShortestPathExecutorB2SmallerCurrentLevelWitness,
	}, decision.PlannedCandidates)
	require.Equal(t, ShortestPathExecutorS3Unidirectional, decision.SelectedExecutor)
	require.Equal(t, ShortestPathSchedulerSingleEndedLevel, decision.Scheduler)
	require.Equal(t, ShortestPathExecutorIncumbentWorkspace, decision.FallbackExecutor)
	require.Empty(t, decision.FallbackReason)
	require.Equal(t, ShortestPathObservationDistance, decision.ObservationMode)
	require.True(t, decision.StructurallyEligible)
	require.Equal(t, int64(1), decision.MinimumDepth)
	require.Equal(t, int64(16), decision.MaximumDepth)
	require.True(t, decision.ExperimentalWinner)
	require.Contains(t, plan.LoweringPlan.Decisions(), LoweringDecision{Name: LoweringShortestPathExecutor})
}

// TestLoweringPlanSelectsBoundPairAllShortestDAGExecutor verifies predecessor-DAG selection for bound all-shortest-path endpoints.
func TestLoweringPlanSelectsBoundPairAllShortestDAGExecutor(t *testing.T) {
	t.Parallel()
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = allShortestPaths((s)-[*1..]->(e))
		WHERE id(s) = $start_id AND id(e) = $end_id
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ShortestPathExecutor, 1)
	decision := plan.LoweringPlan.ShortestPathExecutor[0]
	require.Equal(t, "ASP", decision.Family)
	require.Equal(t, ShortestPathObservationAllPaths, decision.ObservationMode)
	require.Equal(t, ShortestPathExecutorASPA1DAG, decision.SelectedExecutor)
	require.Equal(t, []ShortestPathExecutor{
		ShortestPathExecutorIncumbentWorkspace,
		ShortestPathExecutorASPA1DAG,
		ShortestPathExecutorASPI1DAG,
		ShortestPathExecutorASPB1AlternatingNodeDAG,
		ShortestPathExecutorASPB2SmallerCurrentLevelDAG,
	}, decision.PlannedCandidates)
	require.Equal(t, ShortestPathSchedulerSingleEndedLevel, decision.Scheduler)
	require.Equal(t, "asp-static-v1", decision.SelectorVersion)
	require.Equal(t, "static", decision.SelectionMode)
	require.True(t, decision.StructurallyEligible)
	require.True(t, decision.StaticallyEligible)
	require.Equal(t, int64(1), decision.MinimumDepth)
	require.Equal(t, defaultShortestPathExpansionDepth, decision.MaximumDepth)
	require.Equal(t, defaultShortestPathStateLimit, decision.StateLimit)
	require.Equal(t, defaultShortestPathFrontierLimit, decision.FrontierLimit)
	require.Equal(t, defaultShortestPathPredecessorLimit, decision.PredecessorLimit)
	require.Empty(t, decision.FallbackReason)
}

// TestShortestPathExecutorSchedulersFreezesTournamentSchedulerMetadata verifies
// production controls and reserved bidirectional arms retain distinct policies.
func TestShortestPathExecutorSchedulersFreezesTournamentSchedulerMetadata(t *testing.T) {
	t.Parallel()
	tests := map[ShortestPathExecutor]ShortestPathScheduler{
		ShortestPathExecutorS3Unidirectional:              ShortestPathSchedulerSingleEndedLevel,
		ShortestPathExecutorS3EdgeM0:                      ShortestPathSchedulerSingleEndedLevel,
		ShortestPathExecutorS4CanonicalDistance:           ShortestPathSchedulerSingleEndedLevel,
		ShortestPathExecutorS4CanonicalWitness:            ShortestPathSchedulerSingleEndedLevel,
		ShortestPathExecutorASPA1DAG:                      ShortestPathSchedulerSingleEndedLevel,
		ShortestPathExecutorB1AlternatingNodeDistance:     ShortestPathSchedulerStrictAlternatingNode,
		ShortestPathExecutorB1AlternatingNodeWitness:      ShortestPathSchedulerStrictAlternatingNode,
		ShortestPathExecutorASPB1AlternatingNodeDAG:       ShortestPathSchedulerStrictAlternatingNode,
		ShortestPathExecutorB2SmallerCurrentLevelDistance: ShortestPathSchedulerSmallerCurrentLevel,
		ShortestPathExecutorB2SmallerCurrentLevelWitness:  ShortestPathSchedulerSmallerCurrentLevel,
		ShortestPathExecutorASPB2SmallerCurrentLevelDAG:   ShortestPathSchedulerSmallerCurrentLevel,
	}
	for executor, scheduler := range tests {
		require.Equal(t, scheduler, executor.Scheduler(), executor)
	}
	require.Empty(t, ShortestPathExecutorIncumbentWorkspace.Scheduler())
}

// TestLoweringPlanShortestExecutorV4SelectionMatrix verifies executor selection across direction, depth, kind, and observation combinations.
func TestLoweringPlanShortestExecutorV4SelectionMatrix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		// name labels the executor-selection case.
		name string
		// pattern is the relationship pattern supplied to shortestPath.
		pattern string
		// observation is the return expression that consumes the path.
		observation string
		// executor is the physical implementation expected from selection.
		executor ShortestPathExecutor
		// reason is the expected fallback code when selection is ineligible.
		reason string
		// direction is the logical traversal direction recorded in diagnostics.
		direction graph.Direction
		// physicalExpansion is the edge endpoint used to advance recursive search.
		physicalExpansion ShortestPathPhysicalExpansion
		// topology is the expected physical topology classification.
		topology ShortestPathTopologyClassification
		// kindCount is the expected number of statically resolved relationship kinds.
		kindCount int
		// untyped reports whether the pattern is expected to omit relationship kinds.
		untyped bool
		// staticEligible is the expected static qualification result.
		staticEligible bool
		// selector identifies the policy version expected to make the decision.
		selector string
	}{
		{
			name:              "outbound distance depth 64 two kinds",
			pattern:           `(s)-[:MemberOf|Contains*1..64]->(e)`,
			observation:       `length(p)`,
			executor:          ShortestPathExecutorS3Unidirectional,
			direction:         graph.DirectionOutbound,
			physicalExpansion: ShortestPathPhysicalExpansionStartID,
			topology:          ShortestPathTopologyPhysicalOutbound,
			kindCount:         2,
			staticEligible:    true,
			selector:          "sp-static-v3",
		},
		{
			name:              "outbound one path one kind",
			pattern:           `(s)-[:MemberOf*1..16]->(e)`,
			observation:       `p`,
			executor:          ShortestPathExecutorS3EdgeM0,
			direction:         graph.DirectionOutbound,
			physicalExpansion: ShortestPathPhysicalExpansionStartID,
			topology:          ShortestPathTopologyPhysicalOutbound,
			kindCount:         1,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
		{
			name:              "outbound one path two kinds",
			pattern:           `(s)-[:MemberOf|Contains*1..16]->(e)`,
			observation:       `p`,
			executor:          ShortestPathExecutorS4CanonicalWitness,
			direction:         graph.DirectionOutbound,
			physicalExpansion: ShortestPathPhysicalExpansionStartID,
			topology:          ShortestPathTopologyPhysicalOutbound,
			kindCount:         2,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
		{
			name:              "outbound one path wildcard",
			pattern:           `(s)-[*1..16]->(e)`,
			observation:       `p`,
			executor:          ShortestPathExecutorS4CanonicalWitness,
			direction:         graph.DirectionOutbound,
			physicalExpansion: ShortestPathPhysicalExpansionStartID,
			topology:          ShortestPathTopologyPhysicalOutbound,
			untyped:           true,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
		{
			name:              "inbound distance depth one",
			pattern:           `(s)<-[:MemberOf*0..1]-(e)`,
			observation:       `length(p)`,
			executor:          ShortestPathExecutorS3Unidirectional,
			direction:         graph.DirectionInbound,
			physicalExpansion: ShortestPathPhysicalExpansionEndID,
			topology:          ShortestPathTopologyPhysicalInboundShallow,
			kindCount:         1,
			staticEligible:    true,
			selector:          "sp-static-v3",
		},
		{
			name:              "inbound path depth one",
			pattern:           `(s)<-[:MemberOf*1..1]-(e)`,
			observation:       `p`,
			executor:          ShortestPathExecutorS3EdgeM0,
			direction:         graph.DirectionInbound,
			physicalExpansion: ShortestPathPhysicalExpansionEndID,
			topology:          ShortestPathTopologyPhysicalInboundShallow,
			kindCount:         1,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
		{
			name:              "inbound distance depth two",
			pattern:           `(s)<-[:MemberOf*1..2]-(e)`,
			observation:       `length(p)`,
			executor:          ShortestPathExecutorS4CanonicalDistance,
			direction:         graph.DirectionInbound,
			physicalExpansion: ShortestPathPhysicalExpansionEndID,
			topology:          ShortestPathTopologyPhysicalInboundDeep,
			kindCount:         1,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
		{
			name:              "inbound path depth 64 two kinds",
			pattern:           `(s)<-[:MemberOf|Contains*1..64]-(e)`,
			observation:       `p`,
			executor:          ShortestPathExecutorS4CanonicalWitness,
			direction:         graph.DirectionInbound,
			physicalExpansion: ShortestPathPhysicalExpansionEndID,
			topology:          ShortestPathTopologyPhysicalInboundDeep,
			kindCount:         2,
			staticEligible:    true,
			selector:          "sp-static-v5-contained",
		},
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
			require.Equal(t, test.selector, decision.SelectorVersion)
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

// TestLoweringPlanShortestExecutorV3PreservesStructuralReasonPrecedence verifies that directionless topology wins over later static failures.
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

// TestLoweringPlanShortestExecutorRejectsUnsupportedMinimumDepth verifies rejection of a minimum depth greater than one.
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

// TestLoweringPlanShortestExecutorRetainsZeroMaximumDepthInDiagnostics verifies that an explicit zero maximum is not omitted from JSON.
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

// TestLoweringPlanShortestExecutorUsesStatementWideCallCount verifies that multiple path calls across query parts disqualify static execution.
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

// TestLoweringPlanShortestExecutorUsesStatementWideReadOnlyFact verifies that a later mutation disqualifies an earlier shortest-path candidate.
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

// TestLoweringPlanShortestExecutorObservationModeRequiresPathForNodes verifies that nodes(path) requires a path witness.
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

// TestLoweringPlanShortestExecutorRequiresKnownObservationMode verifies that an unbound path result prevents static executor selection.
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

// TestLoweringPlanShortestExecutorRejectsAdditionalRowSources verifies fallback classification for correlated or ambiguous endpoint sources.
func TestLoweringPlanShortestExecutorRejectsAdditionalRowSources(t *testing.T) {
	t.Parallel()
	tests := []struct {
		// name labels the additional-row-source case.
		name string
		// query produces the shortest-path candidate under test.
		query string
		// reason is the expected stable fallback code.
		reason string
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

// TestLoweringPlanRecordsStableShortestExecutorFallbackCodes verifies diagnostic codes for unsupported shortest-path shapes.
func TestLoweringPlanRecordsStableShortestExecutorFallbackCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		// name labels the unsupported shortest-path shape.
		name string
		// query produces the shortest-path candidate under test.
		query string
		// reason is the expected stable fallback code.
		reason string
	}{
		{
			name:   "all shortest",
			query:  `MATCH p = allShortestPaths((s)-[:MemberOf*1..4]->(e)) RETURN p`,
			reason: ShortestPathFallbackAllShortestPaths,
		},
		{
			name:   "directionless",
			query:  `MATCH p = shortestPath((s)-[:MemberOf*1..4]-(e)) RETURN p`,
			reason: ShortestPathFallbackDirectionless,
		},
		{
			name:   "relationship variable",
			query:  `MATCH p = shortestPath((s)-[r:MemberOf*1..4]->(e)) RETURN p`,
			reason: ShortestPathFallbackRelationshipVariable,
		},
		{
			name:   "open depth",
			query:  `MATCH p = shortestPath((s)-[:MemberOf*1..]->(e)) RETURN p`,
			reason: ShortestPathFallbackUnsupportedDepth,
		},
		{
			name:   "non singleton",
			query:  `MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e)) RETURN p`,
			reason: ShortestPathFallbackNonSingletonID,
		},
		{
			name:   "multiple id equalities",
			query:  `MATCH p = shortestPath((s)-[:MemberOf*1..4]->(e)) WHERE id(s) = 1 AND id(s) = 2 AND id(e) = 3 RETURN p`,
			reason: ShortestPathFallbackMultipleIDEqualities,
		},
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

// TestLoweringPlanReportsShortestPathStrategyForBoundEndpointPairs verifies lowering plan reports shortest path strategy for bound endpoint pairs behavior.
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

// TestLoweringPlanSkipsShortestPathStrategyForLabelOnlyEndpoints verifies lowering plan skips shortest path strategy for label only endpoints behavior.
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

// TestLoweringPlanReportsShortestPathTerminalFilter verifies lowering plan reports shortest path terminal filter behavior.
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

// TestLoweringPlanReportsShortestPathTerminalFilterForKindOnlyTerminal verifies lowering plan reports shortest path terminal filter for kind only terminal behavior.
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

// TestLoweringPlanReportsTraversalLimitPushdown verifies lowering plan reports traversal limit pushdown behavior.
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

// TestLoweringPlanReportsShortestPathLimitPushdown verifies lowering plan reports shortest path limit pushdown behavior.
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

// TestLoweringPlanSkipsAllShortestPathLimitPushdown verifies lowering plan skips all shortest path limit pushdown behavior.
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

// TestLoweringPlanSkipsOptionalMatchLimitPushdown verifies lowering plan skips optional match limit pushdown behavior.
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

// TestDeclareReadingClauseSelectivitySkipsOptionalMatch verifies declare reading clause selectivity skips optional match behavior.
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

// TestSelectReferencesOnlyLocalIdentifiersValidatesJoinConstraintsIncrementally verifies select references only local identifiers validates join constraints incrementally behavior.
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

// TestFlattenConjunctionHandlesValueBinaryExpressions verifies flatten conjunction handles value binary expressions behavior.
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

// TestQueryReferencesOnlyLocalIdentifiersAllowsEmptyWith verifies query references only local identifiers allows empty with behavior.
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

// TestFromExpressionReferencesOnlyLocalIdentifiersHandlesLateralSubquery verifies from expression references only local identifiers handles lateral subquery behavior.
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

// TestMeasureSelectivityPopReturnsTopFrame verifies measure selectivity pop returns top frame behavior.
func TestMeasureSelectivityPopReturnsTopFrame(t *testing.T) {
	t.Parallel()

	visitor := newMeasureSelectivityVisitor(NewSelectivityModel(nil))
	visitor.addSelectivity(7)
	visitor.pushSelectivity(11)
	visitor.addSelectivity(13)

	require.Equal(t, 24, visitor.popSelectivity())
	require.Equal(t, 7, visitor.Selectivity())
}

// TestMeasureSelectivityScoresIDBonusOnlyForPointPredicates verifies measure selectivity scores id bonus only for point predicates behavior.
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

// TestCollectReferencedSourceIdentifiersIgnoresMatchDeclarations verifies collect referenced source identifiers ignores match declarations behavior.
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

// TestLoweringPlanSkipsDirectionlessExpansionSuffixPushdown verifies lowering plan skips directionless expansion suffix pushdown behavior.
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

// TestPredicateAttachmentRuleAssignsSingleBindingPredicates verifies that single-symbol predicates attach to their binding scopes.
func TestPredicateAttachmentRuleAssignsSingleBindingPredicates(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), fixedSuffixExpansionQuery)
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
		BindingSymbols:  []string{"root"},
		Dependencies:    []string{"root"},
	}, plan.PredicateAttachments[0])

	require.Equal(t, PredicateAttachment{
		QueryPartIndex:  0,
		RegionIndex:     0,
		ClauseIndex:     2,
		ExpressionIndex: 0,
		Scope:           PredicateAttachmentScopeBinding,
		BindingSymbols:  []string{"predicate"},
		Dependencies:    []string{"predicate"},
	}, plan.PredicateAttachments[1])
}

// TestPredicateAttachmentRuleKeepsMultiBindingPredicatesAtRegionScope verifies predicate attachment rule keeps multi binding predicates at region scope behavior.
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

// firstNodeSymbol returns the first node variable encountered during a structural query walk.
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

// TestConservativePatternReorderingMovesIndependentNodeAnchorsEarlier verifies conservative pattern reordering moves independent node anchors earlier behavior.
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
			Name:    "InboundTraversalReversal",
			Applied: false,
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

// TestConservativePatternReorderingKeepsDependentAnchorsInPlace verifies conservative pattern reordering keeps dependent anchors in place behavior.
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
			Name:    "InboundTraversalReversal",
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

// TestConservativePatternReorderingUsesSelectivityWithinDependencySafeRegion verifies conservative pattern reordering uses selectivity within dependency safe region behavior.
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
			Name:    "InboundTraversalReversal",
			Applied: false,
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

// TestConservativePatternReorderingPinsUnresolvedExternalDependencies verifies conservative pattern reordering pins unresolved external dependencies behavior.
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
			Name:    "InboundTraversalReversal",
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

// patternNodeSymbols returns the variable symbols of each node pattern in element order.
func patternNodeSymbols(patternPart *cypher.PatternPart) []string {
	var symbols []string

	for _, element := range patternPart.PatternElements {
		if nodePattern, ok := element.AsNodePattern(); ok {
			symbols = append(symbols, variableSymbol(nodePattern.Variable))
		}
	}

	return symbols
}

// patternRelationshipDirections returns the direction of each relationship pattern in element order.
func patternRelationshipDirections(patternPart *cypher.PatternPart) []graph.Direction {
	var directions []graph.Direction

	for _, element := range patternPart.PatternElements {
		if relationshipPattern, ok := element.AsRelationshipPattern(); ok {
			directions = append(directions, relationshipPattern.Direction)
		}
	}

	return directions
}

func TestInboundTraversalReversalReversesElementsAndDirectionsForSelectiveTerminal(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: true})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.True(t, patternPart.PathDirectionReversed)

	// The pattern is reversed so the traversal is driven from the constrained d:Computer terminal
	// inward toward s:User, with each relationship direction flipped from outbound to inbound.
	require.Equal(t, []string{"d", "g", "s"}, patternNodeSymbols(patternPart))
	require.Equal(t, []graph.Direction{graph.DirectionInbound, graph.DirectionInbound}, patternRelationshipDirections(patternPart))
}

func TestInboundTraversalReversalSkipsWhenSourceBoundByPriorClause(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s:User)
		MATCH p = (s)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[1].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalSkipsWhenTerminalLacksSearchConstraint(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalSkipsWhenLeadingStepNotVariableLength(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (s:User)-[:MemberOf]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalSkipsWhenLeadingExpansionBounded(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (s:User)-[:MemberOf*1..3]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalSkipsShortestPathPattern(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = shortestPath((s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer))
		WHERE d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	patternPart := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalReversesQualifyingTraversalAfterWith(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.enabled = true
		WITH u
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: true})

	require.NotNil(t, plan.Query.SingleQuery.MultiPartQuery)
	patternPart := plan.Query.SingleQuery.MultiPartQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.True(t, patternPart.PathDirectionReversed)

	// The traversal following the WITH is driven from the constrained d:Computer terminal inward
	// toward s:User, with each relationship direction flipped from outbound to inbound.
	require.Equal(t, []string{"d", "g", "s"}, patternNodeSymbols(patternPart))
	require.Equal(t, []graph.Direction{graph.DirectionInbound, graph.DirectionInbound}, patternRelationshipDirections(patternPart))
}

func TestInboundTraversalReversalSkipsWhenSourceCarriedAcrossWith(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (s:User)
		WITH s
		MATCH p = (s)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	require.NotNil(t, plan.Query.SingleQuery.MultiPartQuery)
	patternPart := plan.Query.SingleQuery.MultiPartQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalSkipsWhenSourceBoundByUnwindOfCarriedCollection(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (n:User)
		WHERE n.enabled = true
		WITH collect(n) AS users
		UNWIND users AS s
		MATCH p = (s)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)

	plan, err := Optimize(regularQuery)
	require.NoError(t, err)

	// The traversal source s is bound by the UNWIND of a carried collection, so reversing it would
	// break the established drive order for the externally provided source.
	require.Contains(t, plan.Rules, RuleResult{Name: "InboundTraversalReversal", Applied: false})

	require.NotNil(t, plan.Query.SingleQuery.MultiPartQuery)
	patternPart := plan.Query.SingleQuery.MultiPartQuery.SinglePartQuery.ReadingClauses[1].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}

func TestInboundTraversalReversalRejectsAmbiguousSingleAndMultiPartQuery(t *testing.T) {
	t.Parallel()

	singlePartQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)
	require.NotNil(t, singlePartQuery.SingleQuery.SinglePartQuery)

	multiPartQuery, err := frontend.ParseCypher(frontend.NewContext(), `
		MATCH (u:User)
		WHERE u.enabled = true
		WITH u
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		RETURN p
	`)
	require.NoError(t, err)
	require.NotNil(t, multiPartQuery.SingleQuery.MultiPartQuery)

	// An ambiguous representation carrying both a single-part and a multi-part query is rejected
	// rather than silently optimizing only one of the two.
	plan := &Plan{
		Query: &cypher.RegularQuery{
			SingleQuery: &cypher.SingleQuery{
				SinglePartQuery: singlePartQuery.SingleQuery.SinglePartQuery,
				MultiPartQuery:  multiPartQuery.SingleQuery.MultiPartQuery,
			},
		},
	}

	applied, err := InboundTraversalReversalRule{}.Apply(plan)
	require.NoError(t, err)
	require.False(t, applied)

	// Neither representation is mutated when the ambiguous query is rejected.
	singlePartPattern := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, singlePartPattern.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(singlePartPattern))

	multiPartPattern := plan.Query.SingleQuery.MultiPartQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0]
	require.False(t, multiPartPattern.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(multiPartPattern))
}

func TestInboundTraversalReversalRejectsMultiPartQueryWithoutTerminalSinglePartQuery(t *testing.T) {
	t.Parallel()

	const query = `
		MATCH p = (s:User)-[:MemberOf*0..]->(g:Group)-[:AdminTo]->(d:Computer)
		WHERE s.samaccountname =~ '(?i).*[ge]$' AND d.operatingsystem CONTAINS 'WINDOWS SERVER'
		WITH p
		RETURN p
	`

	// Control: with the terminal single-part query intact, the qualifying pattern in the preceding
	// part is reversed, establishing that this part is reversible.
	control, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	require.NotNil(t, control.SingleQuery.MultiPartQuery)
	require.NotEmpty(t, control.SingleQuery.MultiPartQuery.Parts)
	require.NotNil(t, control.SingleQuery.MultiPartQuery.SinglePartQuery)

	applied, err := InboundTraversalReversalRule{}.Apply(&Plan{Query: control})
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, control.SingleQuery.MultiPartQuery.Parts[0].ReadingClauses[0].Match.Pattern[0].PathDirectionReversed)

	// Removing the terminal single-part query models an unsupported multi-part representation, which
	// is rejected up front so the preceding part is left untouched.
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), query)
	require.NoError(t, err)
	multiPartQuery := regularQuery.SingleQuery.MultiPartQuery
	require.NotNil(t, multiPartQuery)
	require.NotEmpty(t, multiPartQuery.Parts)
	multiPartQuery.SinglePartQuery = nil

	applied, err = InboundTraversalReversalRule{}.Apply(&Plan{Query: regularQuery})
	require.NoError(t, err)
	require.False(t, applied)

	patternPart := multiPartQuery.Parts[0].ReadingClauses[0].Match.Pattern[0]
	require.False(t, patternPart.PathDirectionReversed)
	require.Equal(t, []string{"s", "g", "d"}, patternNodeSymbols(patternPart))
}
