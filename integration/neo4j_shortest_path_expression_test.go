//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNeo4jNativeShortestPathExpression(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	require.NoError(t, session.WithRollback(t, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties())
		if err != nil {
			return err
		}
		end, err := tx.CreateNode(graph.NewProperties())
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, graph.StringKind("ShortestExpressionEdge"), graph.NewProperties()); err != nil {
			return err
		}
		for _, source := range []string{
			"MATCH (a), (b) WHERE id(a) = $start AND id(b) = $end RETURN length(shortestPath((a)-[:ShortestExpressionEdge*1..3]->(b)))",
			"MATCH (a), (b) WHERE id(a) = $start AND id(b) = $end WITH shortestPath((a)-[:ShortestExpressionEdge*1..3]->(b)) AS p RETURN length(p)",
			"MATCH (a), (b) WHERE id(a) = $start AND id(b) = $end SET a.distance = length(shortestPath((a)-[:ShortestExpressionEdge*1..3]->(b))) RETURN a.distance",
		} {
			result := tx.Query(source, map[string]any{"start": start.ID, "end": end.ID})
			require.True(t, result.Next(), "%v", result.Error())
			require.Equal(t, []any{int64(1)}, result.Values())
			require.False(t, result.Next())
			err := result.Error()
			result.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}))
}
