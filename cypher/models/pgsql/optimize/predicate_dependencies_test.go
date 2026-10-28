package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

func TestPatternPredicateDependencies(t *testing.T) {
	for _, tc := range []struct {
		query        string
		dependencies []string
	}{
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE (b)-[:R]->(c) RETURN b,c`, []string{"b", "c"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE NOT (b)-[:R]->(c) OR c.enabled = true RETURN b,c`, []string{"b", "c"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE (b)-[r:R]->(c) RETURN b,c`, []string{"b", "c", "r"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE (b)-[:R {name: external.name}]->(c) RETURN b,c`, []string{"b", "c", "external"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE (b)-[:R]->() RETURN b,c`, []string{"b"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE (b)-[:R]->(b) RETURN b,c`, []string{"b"}},
		{`MATCH (b:B) MATCH (c {name: 'c1'}) WHERE ()-[:R]->() RETURN b,c`, []string{}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)
			clauses := query.SingleQuery.SinglePartQuery.ReadingClauses
			require.Equal(t, tc.dependencies, sortedDependencies(clauses[1].Match.Where))
			analysis := Analyze(query)
			require.Equal(t, tc.dependencies, analysis.QueryParts[0].Regions[0].Predicates[0].Dependencies)
		})
	}
	require.Nil(t, sortedDependencies(nil))
	// The wildcard is a projection marker, not a binding dependency.
	require.Empty(t, sortedDependencies(&cypher.Variable{Symbol: cypher.TokenLiteralAsterisk}))
	require.Empty(t, sortedDependencies(&cypher.NodePattern{Variable: &cypher.Variable{}}))
}

func TestPatternPredicateReorderingRequiresEndpoints(t *testing.T) {
	for _, predicate := range []string{`(b)-[:R]->(c)`, `NOT (b)-[:R]->(c)`, `(b)<-[:R]-(c)`, `(b)-[:R]-(c)`, `(b)-[:R]->(c) OR c.enabled = true`} {
		t.Run(predicate, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (b:B) MATCH (c {name: 'c1'}) WHERE `+predicate+` RETURN b,c`)
			require.NoError(t, err)
			plan, err := Optimize(query)
			require.NoError(t, err)
			clauses := plan.Query.SingleQuery.SinglePartQuery.ReadingClauses
			require.Equal(t, "b", firstNodeSymbol(clauses[0]))
			require.Equal(t, "c", firstNodeSymbol(clauses[1]))
			require.False(t, plan.Rules[0].Applied)
		})
	}
}

func TestPatternPredicateReorderingPinsUnknownBindings(t *testing.T) {
	for _, predicate := range []string{`(external)-[:R]->(a)`, `(a)-[external:R]->()`, `(a)-[:R]->(external)`, `(a)-[:R {name: external.name}]->()`} {
		t.Run(predicate, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (a) WHERE `+predicate+` MATCH (b:B {name: 'target'}) RETURN b`)
			require.NoError(t, err)
			clauses := query.SingleQuery.SinglePartQuery.ReadingClauses
			candidates := reorderCandidates(clauses, nil)
			require.False(t, candidates[0].movable)
			plan, err := Optimize(query)
			require.NoError(t, err)
			require.False(t, plan.Rules[0].Applied)
			require.Equal(t, "a", firstNodeSymbol(plan.Query.SingleQuery.SinglePartQuery.ReadingClauses[0]))
		})
	}
}
