//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNeo4jRawRewriteFunctionNamespace(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	require.NoError(t, session.WithRollback(t, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"tag": "namespace"}))
		if err != nil {
			return err
		}
		for _, pattern := range []string{"(n {tag: $tag})", "(n $props)"} {
			result := tx.Query("MATCH "+pattern+" RETURN point.distance(point({x: 0.0, y: 0.0}), point({x: 3.0, y: 4.0})), 3.0 / 2.0", map[string]any{"tag": "namespace", "props": map[string]any{"tag": "namespace"}})
			require.True(t, result.Next(), "%v", result.Error())
			require.Equal(t, []any{float64(5), 1.5}, result.Values())
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
