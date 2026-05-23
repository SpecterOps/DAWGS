package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
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
		PredicateAttachments: []PredicateAttachment{ctPredicate},
	})
	require.Contains(t, plan.LoweringPlan.ExpansionSuffixPushdown, ExpansionSuffixPushdownDecision{
		Target: TraversalStepTarget{
			QueryPartIndex: 0,
			ClauseIndex:    2,
			PatternIndex:   0,
			StepIndex:      3,
		},
		SuffixLength:    1,
		SuffixStartStep: 4,
		SuffixEndStep:   4,
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
	require.Equal(t, []LoweringDecision{{Name: LoweringProjectionPruning}}, plan.LoweringPlan.Decisions())
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

func TestSelectivityModelPlansTraversalDirection(t *testing.T) {
	t.Parallel()

	model := NewSelectivityModel(testBindingLookup{})
	rightIDLookup := pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{pgsql.Identifier("n1"), pgsql.ColumnID},
		pgsql.OperatorEquals,
		pgsql.NewLiteral(1, pgsql.Int),
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
		SuffixLength:    1,
		SuffixStartStep: 1,
		SuffixEndStep:   1,
	}}, plan.LoweringPlan.ExpansionSuffixPushdown)
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
		SuffixLength:    2,
		SuffixStartStep: 1,
		SuffixEndStep:   2,
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

func TestSelectReferencesOnlyLocalIdentifiersValidatesJoinConstraintsIncrementally(t *testing.T) {
	t.Parallel()

	tableRef := func(alias pgsql.Identifier) pgsql.TableReference {
		return pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
			Binding: models.OptionalValue(alias),
		}
	}

	selectBody := pgsql.Select{
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

	require.False(t, SelectReferencesOnlyLocalIdentifiers(selectBody, pgsql.NewIdentifierSet()))
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
		{Name: "ConservativePatternReordering", Applied: true},
		{Name: "PredicateAttachment", Applied: false},
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
		{Name: "ConservativePatternReordering", Applied: false},
		{Name: "PredicateAttachment", Applied: true},
	}, plan.Rules)

	readingClauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
	require.Equal(t, "a", firstNodeSymbol(readingClauses[0]))
	require.Equal(t, "b", firstNodeSymbol(readingClauses[1]))
}
