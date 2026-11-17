package optimize

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

// TestBudgetedEndpointPairPredicatesPreserveHistoricalAnalysis verifies that
// V2 proof discovery leaves the caller's AST and legacy correlation gate intact.
func TestBudgetedEndpointPairPredicatesPreserveHistoricalAnalysis(t *testing.T) {
	for _, predicate := range []string{
		"id(s)<>id(t)",
		"id(t)<>id(s)",
		"((id(s)) <> (id(t)))",
		"((id(t)) <> (id(s)))",
		"(id(s)<>id(t) AND (id(t)<>id(s)))",
	} {
		t.Run(predicate, func(t *testing.T) {
			source := fmt.Sprintf(
				"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name IN ['left','overlap'] AND t.name IN ['overlap','right'] AND %s RETURN p",
				predicate,
			)
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)

			original := cypher.Copy(parsed)
			before, err := Optimize(parsed)
			require.NoError(t, err)

			proofs := BudgetedEndpointPairPredicates(before.Query)
			require.Equal(t, map[TraversalStepTarget]EndpointPairPredicate{
				{}: EndpointPairPredicateDistinctIDs,
			}, proofs)
			require.Equal(t, original, parsed)
			require.Equal(t, EndpointResolutionFallbackCorrelatedPair, before.LoweringPlan.EndpointResolution[0].FallbackReason)
			require.Empty(t, before.LoweringPlan.ShortestPathExecutor[0].EndpointPairPredicate)

			after, err := Optimize(parsed)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

// TestBudgetedEndpointPairPredicatesRejectUnsafeCorrelation verifies that no
// proof can hide another endpoint dependency or cross a query-part boundary.
func TestBudgetedEndpointPairPredicatesRejectUnsafeCorrelation(t *testing.T) {
	require.Empty(t, BudgetedEndpointPairPredicates(nil))

	for _, source := range []string{
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND NOT(id(s)=id(t)) RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND (id(s)<>id(t) OR true) RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id(s)<>id(t) AND s.flag=t.flag RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id(s)<>id(t)<>42 RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id(s)+1<>id(t) RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id((s))<>id(t) RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND custom.id(s)<>id(t) RETURN p",
		"MATCH p=shortestPath((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id(s)<>id(t) RETURN p",
		"UNWIND [1] AS x MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' AND id(s)<>id(t) RETURN p",
		"WITH 'left' AS name MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name=name AND t.name='right' AND id(s)<>id(t) RETURN p",
		"MATCH (s {name:'left'}) MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE t.name='right' AND id(s)<>id(t) RETURN p",
		"MATCH p=allShortestPaths((s)-[*1..]->(t)) WHERE s.name='left' AND t.name='right' WITH p,s,t WHERE id(s)<>id(t) RETURN p",
	} {
		t.Run(source, func(t *testing.T) {
			parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
			require.NoError(t, err)

			original := cypher.Copy(parsed)
			require.Empty(t, BudgetedEndpointPairPredicates(parsed))
			require.Equal(t, original, parsed)
		})
	}
}
