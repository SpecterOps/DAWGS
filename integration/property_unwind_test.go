//go:build manual_integration

package integration

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Corpus assertions normalize numbers. Check concrete Go element types as well,
// using committed properties so fixture JSON cannot round integer inputs.
func TestPropertyUnwind(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("PropertyUnwind")},
		ExtraEdgeKinds: graph.Kinds{graph.StringKind("PropertyUnwindEdge")}})
	for _, tc := range []struct {
		name  string
		input any
		want  []any
	}{
		{"integers", []int64{9007199254740993, math.MaxInt64, math.MinInt64}, []any{int64(9007199254740993), int64(math.MaxInt64), int64(math.MinInt64)}},
		{"strings", []string{"1", "true", "[1, 2]", "\"q\"", ""}, []any{"1", "true", "[1, 2]", "\"q\"", ""}},
		{"booleans", []bool{true, false}, []any{true, false}},
		{"floats", []float64{1.25, -2.5}, []any{1.25, -2.5}},
		{"empty", []int64{}, nil},
		{"missing", nil, nil},
		{"scalar integer", int64(9007199254740993), []any{int64(9007199254740993)}},
		{"scalar string", "[1, 2]", []any{"[1, 2]"}},
		{"scalar boolean", true, []any{true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				props := graph.AsProperties(map[string]any{"arr": tc.input})
				node, err := tx.CreateNode(props, graph.StringKind("PropertyUnwind"))
				if err != nil {
					return err
				}
				_, err = tx.CreateRelationshipByIDs(node.ID, node.ID, graph.StringKind("PropertyUnwindEdge"), props)
				return err
			}))
			for _, match := range []string{"MATCH (n:PropertyUnwind) ", "MATCH ()-[n:PropertyUnwindEdge]->() "} {
				for _, tail := range []string{
					"UNWIND n.arr AS x RETURN x",
					"WITH n.arr AS xs UNWIND xs AS x RETURN x",
					"WITH collect(n.arr) AS lists UNWIND lists AS xs UNWIND xs AS x RETURN x",
				} {
					t.Run(match+tail, func(t *testing.T) {
						require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
							result := tx.Query(match+tail, nil)
							defer result.Close()
							var got []any
							for result.Next() {
								got = append(got, result.Values()[0])
							}
							require.NoError(t, result.Error())
							require.ElementsMatch(t, tc.want, got)
							return result.Error()
						}))
					})
				}
			}
		})
	}
}
