//go:build manual_integration

package integration

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestExactResultNumbers checks the actual Go types and values without the
// corpus assertion helpers' numeric coercion or a JSON round trip.
func TestExactResultNumbers(t *testing.T) {
	session := Open(t, Options{
		SkipIfNoConnection: true,
		CleanupMode:        CleanupGraph,
		ExtraNodeKinds:     graph.Kinds{graph.StringKind("ExactNumber")},
		ExtraEdgeKinds:     graph.Kinds{graph.StringKind("ExactNumberEdge")},
	})
	ClearGraph(t, session.DB, session.Ctx)
	properties := map[string]any{
		"large":      int64(4611686018427387905),
		"above_safe": int64(9007199254740993),
		"negative":   int64(-9007199254740993),
		"max":        int64(math.MaxInt64),
		"min":        int64(math.MinInt64),
		"small":      int64(42),
		"fraction":   1.25,
		"text":       "4611686018427387905",
		"flag":       true,
		"numbers":    []int64{9007199254740993, math.MaxInt64, math.MinInt64},
	}
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(properties), graph.StringKind("ExactNumber"))
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(properties), graph.StringKind("ExactNumber"))
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, graph.StringKind("ExactNumberEdge"), graph.AsProperties(properties))
		return err
	}))

	for _, test := range []struct {
		name, query string
		params      map[string]any
		check       func(*testing.T, graph.Result)
	}{
		{"scalars", `MATCH (n:ExactNumber) RETURN n.large, n.above_safe, n.negative, n.max, n.min, n.small, n.fraction, n.text, n.flag`, nil, func(t *testing.T, result graph.Result) {
			want := []any{int64(4611686018427387905), int64(9007199254740993), int64(-9007199254740993), int64(math.MaxInt64), int64(math.MinInt64), int64(42), 1.25, "4611686018427387905", true}
			require.Equal(t, want, result.Values())
		}},
		{"carried scalar", `MATCH (n:ExactNumber) WITH n.large AS value RETURN value`, nil, func(t *testing.T, result graph.Result) {
			var value int64
			require.NoError(t, graph.ScanNextResult(result, &value))
			require.Equal(t, int64(4611686018427387905), value)
		}},
		{"map", `RETURN $value AS value`, map[string]any{"value": map[string]any{"large": int64(4611686018427387905), "nested": map[string]any{"max": int64(math.MaxInt64)}, "values": []int64{math.MinInt64, 9007199254740993}, "missing": nil}}, func(t *testing.T, result graph.Result) {
			require.Equal(t, map[string]any{"large": int64(4611686018427387905), "nested": map[string]any{"max": int64(math.MaxInt64)}, "values": []any{int64(math.MinInt64), int64(9007199254740993)}, "missing": nil}, result.Values()[0])
		}},
		{"entities and path", `MATCH p = (a:ExactNumber)-[r:ExactNumberEdge]->(b:ExactNumber) RETURN a, r, p`, nil, func(t *testing.T, result graph.Result) {
			var node graph.Node
			var edge graph.Relationship
			var path graph.Path
			require.NoError(t, graph.ScanNextResult(result, &node, &edge, &path))
			assertExactNumberProperties(t, node.Properties.Map)
			assertExactNumberProperties(t, edge.Properties.Map)
			require.Len(t, path.Nodes, 2)
			require.Len(t, path.Edges, 1)
			for _, node := range path.Nodes {
				assertExactNumberProperties(t, node.Properties.Map)
			}
			assertExactNumberProperties(t, path.Edges[0].Properties.Map)
		}},
		{"entity collections", `MATCH (a:ExactNumber)-[r:ExactNumberEdge]->(b) RETURN collect(a), collect(r)`, nil, func(t *testing.T, result graph.Result) {
			var nodes []*graph.Node
			var edges []*graph.Relationship
			require.NoError(t, graph.ScanNextResult(result, &nodes, &edges))
			require.Len(t, nodes, 1)
			require.Len(t, edges, 1)
			assertExactNumberProperties(t, nodes[0].Properties.Map)
			assertExactNumberProperties(t, edges[0].Properties.Map)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(test.query, test.params)
				defer result.Close()
				count := 0
				for result.Next() {
					test.check(t, result)
					count++
				}
				require.Positive(t, count, "%v", result.Error())
				return result.Error()
			}))
		})
	}
}

func assertExactNumberProperties(t *testing.T, properties map[string]any) {
	t.Helper()
	for key, want := range map[string]any{
		"large": int64(4611686018427387905), "above_safe": int64(9007199254740993),
		"negative": int64(-9007199254740993), "max": int64(math.MaxInt64),
		"min": int64(math.MinInt64), "small": int64(42), "fraction": 1.25,
		"text": "4611686018427387905", "flag": true,
	} {
		require.Equal(t, want, properties[key], key)
	}
	// Both drivers expose integer property arrays through the graph mapper.
	var numbers []int64
	require.True(t, graph.NewValueMapper().Map(properties["numbers"], &numbers))
	require.Equal(t, []int64{9007199254740993, math.MaxInt64, math.MinInt64}, numbers)
}
