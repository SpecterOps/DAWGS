package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
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
	}}, plan.LoweringPlan.ProjectionPruning)
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
		SuffixLength: 1,
	}}, plan.LoweringPlan.ExpansionSuffixPushdown)
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
