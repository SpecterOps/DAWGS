package optimize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestInlineMapSourceReferences(t *testing.T) {
	for _, tc := range []struct {
		name, query, symbol string
	}{
		{"node identity", `MATCH (s)-[:R]->(m)-[:R]->(e {x: id(s)}) RETURN e`, "s"},
		{"node labels", `MATCH (s)-[:R]->(m)-[:R]->(e {x: labels(s)}) RETURN e`, "s"},
		{"relationship map identity", `MATCH (s)-[:R]->(m)-[:R {x: id(s)}]->(e) RETURN e`, "s"},
		{"relationship map labels", `MATCH (s)-[:R]->(m)-[:R {x: labels(s)}]->(e) RETURN e`, "s"},
		{"relationship identity", `MATCH (s)-[r:R]->(m)-[:R {x: id(r)}]->(e) RETURN e`, "r"},
		{"relationship type", `MATCH (s)-[r:R]->(m)-[:R]->(e {x: type(r)}) RETURN e`, "r"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)
			references, err := collectReferencedSourceIdentifiers(query)
			require.NoError(t, err)
			require.Contains(t, references, tc.symbol)
			require.NotContains(t, references, "m", "an unconsumed declaration can still be pruned")
		})
	}
}

func TestLiteralInlineMapsKeepDeclarationPruning(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (s {x: 1})-[r:R {x: 2}]->(e) RETURN e`)
	require.NoError(t, err)
	references, err := collectReferencedSourceIdentifiers(query)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"e": {}}, references)

	plan, err := Optimize(query)
	require.NoError(t, err)
	require.Len(t, plan.LoweringPlan.ProjectionPruning, 1)
	require.True(t, plan.LoweringPlan.ProjectionPruning[0].OmitLeftNode)
	require.True(t, plan.LoweringPlan.ProjectionPruning[0].OmitRelationship)
}
