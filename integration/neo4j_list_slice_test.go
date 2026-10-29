//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Neo4j keeps native list slicing when the shared parser declines a rewrite.
// PostgreSQL's explicit unsupported boundary is tested in its own driver suite.
func TestNeo4jNativeListSlicing(t *testing.T) {
	session := Open(t, Options{RequireDriver: neo4j.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true})
	for _, tc := range []struct {
		expression string
		expected   any
	}{
		{"$values[0..1]", []any{int64(3)}},
		{"$values[..1]", []any{int64(3)}},
		{"$values[-1..]", []any{int64(7)}},
		{"$values[99..]", []any{}},
		{"$values[null..]", nil},
		{"$values[$idx..]", []any{int64(7)}},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("RETURN "+tc.expression+" AS value", map[string]any{"values": []int64{3, 7}, "idx": int64(1)})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{tc.expected}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
	// Neo4j requires at least one bound, unlike the shared grammar.
	require.ErrorContains(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		result := tx.Query("RETURN $values[..] AS value", map[string]any{"values": []int64{3, 7}})
		defer result.Close()
		require.False(t, result.Next())
		return result.Error()
	}), "The start or end (or both) is required for a collection slice")

	require.NoError(t, session.WithRollback(t, func(tx graph.Transaction) error {
		result := tx.Query("CREATE (n {value: $values[$idx..1]}) SET n.copy = $values[-1..] RETURN n.value, n.copy", map[string]any{"values": []int64{3, 7}, "idx": int64(0)})
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		require.Equal(t, []any{[]any{int64(3)}, []any{int64(7)}}, result.Values())
		require.False(t, result.Next())
		return result.Error()
	}))
}
