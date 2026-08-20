package pg

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestTraversalShapeUsesOptimizerFactsWithoutIdentifiersOrValues(t *testing.T) {
	first, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p = allShortestPaths((s)-[:Edge*1..4]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p")
	require.NoError(t, err)
	second, err := frontend.ParseCypher(frontend.NewContext(), "MATCH route = allShortestPaths((x)-[:Edge*1..4]->(y)) WHERE id(x) = $left AND id(y) = $right RETURN route")
	require.NoError(t, err)

	firstShape, err := traversalShapeForQuery(first)
	require.NoError(t, err)
	secondShape, err := traversalShapeForQuery(second)
	require.NoError(t, err)
	require.True(t, firstShape.Available())
	require.Equal(t, TraversalShapeVersion, firstShape.Version)
	require.Equal(t, "ASP", firstShape.Family)
	require.Equal(t, "outbound", firstShape.Direction)
	require.Equal(t, "all_paths", firstShape.ObservationMode)
	require.Equal(t, int64(1), firstShape.MinimumDepth)
	require.Equal(t, int64(4), firstShape.MaximumDepth)
	require.Equal(t, 1, firstShape.RelationshipKindCount)
	require.False(t, firstShape.UntypedRelationship)
	require.Equal(t, firstShape.Fingerprint, secondShape.Fingerprint)
}

func TestTraversalShapeRejectsMultipleTraversalTargets(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p = shortestPath((a)-[:Edge*1..4]->(b)), q = shortestPath((c)-[:Edge*1..4]->(d)) RETURN p, q")
	require.NoError(t, err)

	shape, err := traversalShapeForQuery(query)
	require.NoError(t, err)
	require.False(t, shape.Available())
}
