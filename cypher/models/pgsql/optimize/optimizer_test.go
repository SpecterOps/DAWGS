package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

type testRule struct {
	name string
}

func (s testRule) Name() string {
	return s.name
}

func (s testRule) Apply(plan *Plan) error {
	return nil
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
	require.Equal(t, []RuleResult{{Name: "PredicateAttachment", Applied: true}}, plan.Rules)
	require.Len(t, plan.PredicateAttachments, 2)
}

func TestOptimizerRunsRulesAndRefreshesAnalysis(t *testing.T) {
	t.Parallel()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n) RETURN n`)
	require.NoError(t, err)

	plan, err := NewOptimizer(testRule{name: "test"}).Optimize(regularQuery)
	require.NoError(t, err)
	require.Equal(t, []RuleResult{{Name: "test", Applied: true}}, plan.Rules)
	require.Len(t, plan.Analysis.QueryParts, 1)
	require.Len(t, plan.Analysis.QueryParts[0].Regions, 1)
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
