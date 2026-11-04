//go:build manual_integration

package integration

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Check concrete element types without the corpus assertions' numeric coercion.
func TestTypedCollect(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("TypedCollect")},
		ExtraEdgeKinds: graph.Kinds{graph.StringKind("TypedCollectEdge")}})
	for _, tc := range []struct {
		name, tail string
		values     []any
		want       []any
		rows       bool
	}{
		{"integers", "RETURN collect(n.x)", []any{int64(1), int64(2)}, []any{int64(1), int64(2)}, false},
		{"large integers", "RETURN collect(n.x)", []any{int64(9007199254740993), int64(math.MaxInt64), int64(math.MinInt64)}, []any{int64(9007199254740993), int64(math.MaxInt64), int64(math.MinInt64)}, false},
		{"booleans", "RETURN collect(n.x)", []any{true, false}, []any{true, false}, false},
		{"floats", "RETURN collect(n.x)", []any{1.25, -2.5}, []any{1.25, -2.5}, false},
		{"strings", "RETURN collect(n.x)", []any{"1", "true", "[1, 2]", "\"q\""}, []any{"1", "true", "[1, 2]", "\"q\""}, false},
		{"mixed", "RETURN collect(n.x)", []any{int64(1), "1", true, 1.25, nil}, []any{int64(1), "1", true, 1.25}, false},
		{"distinct types", "RETURN collect(DISTINCT n.x)", []any{int64(1), int64(1), "1", true, true}, []any{int64(1), "1", true}, false},
		{"empty", "RETURN collect(n.x)", nil, []any{}, false},
		{"nulls", "RETURN collect(n.x)", []any{nil, nil}, []any{}, false},
		{"carried nulls", "WITH n.x AS x RETURN collect(x)", []any{nil, int64(2)}, []any{int64(2)}, false},
		{"carried property", "WITH n.x AS x RETURN collect(x)", []any{int64(9007199254740993), true, "1"}, []any{int64(9007199254740993), true, "1"}, false},
		{"carried list", "WITH collect(n.x) AS xs WITH xs AS ys RETURN ys", []any{int64(9007199254740993)}, []any{int64(9007199254740993)}, false},
		{"nested lists", "RETURN collect(n.x)", []any{[]int64{1, 2}, []int64{}}, []any{[]any{int64(1), int64(2)}, []any{}}, false},
		{"unwind values", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN x", []any{int64(9007199254740993), true, "1"}, []any{int64(9007199254740993), true, "1"}, true},
		{"unwind arithmetic", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN x + 1", []any{int64(9007199254740993), int64(2)}, []any{int64(9007199254740994), int64(3)}, true},
		{"unwind integer division", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN x / 2", []any{int64(3)}, []any{int64(1)}, true},
		{"unwind boolean", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN x AND true", []any{true, false}, []any{true, false}, true},
		{"unwind text", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN toUpper(x)", []any{"hi", "[one]"}, []any{"HI", "[ONE]"}, true},
		{"recollect", "WITH collect(n.x) AS xs UNWIND xs AS x RETURN collect(x)", []any{int64(9007199254740993), true}, []any{int64(9007199254740993), true}, false},
		{"nested collect alias", "WITH collect(n.x) AS xs RETURN collect(xs)", []any{int64(9007199254740993)}, []any{[]any{int64(9007199254740993)}}, false},
		{"literal boolean", "RETURN collect(true)", []any{int64(1)}, []any{true}, false},
		{"literal null", "RETURN collect(null)", []any{int64(1)}, []any{}, false},
		{"literal list", "RETURN collect([1, 2])", []any{int64(1)}, []any{[]any{int64(1), int64(2)}}, false},
		{"concatenated literal", "WITH collect(n.x) + [3] AS xs UNWIND xs AS x RETURN x", []any{int64(2)}, []any{int64(2), int64(3)}, true},
		{"prepended literal", "WITH [3] + collect(n.x) AS xs UNWIND xs AS x RETURN x", []any{int64(2)}, []any{int64(2), int64(3)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				for _, value := range tc.values {
					properties := graph.AsProperties(map[string]any{"x": value})
					node, err := tx.CreateNode(properties, graph.StringKind("TypedCollect"))
					if err != nil {
						return err
					}
					if _, err := tx.CreateRelationshipByIDs(node.ID, node.ID, graph.StringKind("TypedCollectEdge"), properties); err != nil {
						return err
					}
				}
				return nil
			}))
			for _, match := range []string{"MATCH (n:TypedCollect) ", "MATCH ()-[n:TypedCollectEdge]->() "} {
				t.Run(match, func(t *testing.T) {
					require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
						result := tx.Query(match+tc.tail, nil)
						defer result.Close()
						var got []any
						if tc.rows {
							for result.Next() {
								got = append(got, result.Values()[0])
							}
						} else {
							require.True(t, result.Next(), "%v", result.Error())
							require.IsType(t, []any{}, result.Values()[0])
							got = result.Values()[0].([]any)
							require.False(t, result.Next())
						}
						require.NoError(t, result.Error())
						require.ElementsMatch(t, tc.want, got)
						return result.Error()
					}))
				})
			}
		})
	}
}
