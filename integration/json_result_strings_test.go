//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestJSONResultStrings checks actual string types through both Values and Scan,
// including committed properties and structured values containing JSON-like text.
func TestJSONResultStrings(t *testing.T) {
	session := Open(t, Options{
		SkipIfNoConnection: true,
		CleanupMode:        CleanupGraph,
		ExtraNodeKinds:     graph.Kinds{graph.StringKind("JSONText")},
		ExtraEdgeKinds:     graph.Kinds{graph.StringKind("JSONTextEdge")},
	})
	ClearGraph(t, session.DB, session.Ctx)
	properties := map[string]any{
		"array_text": "[1, 2]", "object_text": `{"k":"v"}`, "quoted_text": `"q"`,
		"empty_array": "[]", "empty_object": "{}", "quoted_empty": `""`,
		"whitespace": " \t[1, 2]\n", "escape_text": `"\u0041"`, "invalid_json": "{broken",
		"number_text": "1234", "boolean_text": "true", "null_text": "null", "empty_text": "",
	}
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(properties), graph.StringKind("JSONText"))
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("JSONText"))
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, graph.StringKind("JSONTextEdge"), graph.AsProperties(properties))
		return err
	}))

	for key, want := range properties {
		t.Run(key, func(t *testing.T) {
			for name, query := range map[string]string{
				"node property":         "MATCH (n:JSONText) WHERE n." + key + " IS NOT NULL RETURN n." + key,
				"carried property":      "MATCH (n:JSONText) WHERE n." + key + " IS NOT NULL WITH n." + key + " AS value RETURN value",
				"relationship property": "MATCH ()-[r:JSONTextEdge]->() RETURN r." + key,
				"parameter":             "RETURN $value AS value",
			} {
				t.Run(name, func(t *testing.T) {
					require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
						result := tx.Query(query, map[string]any{"value": want})
						defer result.Close()
						require.True(t, result.Next(), "%v", result.Error())
						require.Equal(t, []any{want}, result.Values())
						var value string
						require.NoError(t, graph.ScanNextResult(result, &value))
						require.Equal(t, want, value)
						require.False(t, result.Next())
						return result.Error()
					}))
				})
			}
		})
	}

	t.Run("entities and path", func(t *testing.T) {
		require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
			result := tx.Query("MATCH p = (a:JSONText)-[r:JSONTextEdge]->(b) RETURN a, r, p", nil)
			defer result.Close()
			require.True(t, result.Next(), "%v", result.Error())
			var node graph.Node
			var edge graph.Relationship
			var path graph.Path
			require.NoError(t, graph.ScanNextResult(result, &node, &edge, &path))
			require.Equal(t, properties, node.Properties.Map)
			require.Equal(t, properties, edge.Properties.Map)
			require.Len(t, path.Nodes, 2)
			require.Len(t, path.Edges, 1)
			require.Equal(t, properties, path.Nodes[0].Properties.Map)
			require.Equal(t, properties, path.Edges[0].Properties.Map)
			require.False(t, result.Next())
			return result.Error()
		}))
	})

	t.Run("structured parameter", func(t *testing.T) {
		want := map[string]any{"text": `{"k":"v"}`, "nested": []any{"[1, 2]", `"q"`, nil, int64(42), true}}
		require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
			result := tx.Query("RETURN $value AS value", map[string]any{"value": want})
			defer result.Close()
			require.True(t, result.Next(), "%v", result.Error())
			require.Equal(t, []any{want}, result.Values())
			require.False(t, result.Next())
			return result.Error()
		}))
	})
}
