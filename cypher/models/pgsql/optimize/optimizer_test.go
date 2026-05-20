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
