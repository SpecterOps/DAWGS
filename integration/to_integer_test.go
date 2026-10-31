//go:build manual_integration

package integration

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestToInteger checks native parameters and committed properties without the
// numeric conversions performed by the shared JSON fixture assertions.
func TestToInteger(t *testing.T) {
	kind := graph.StringKind("ToIntegerInput")
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{kind}, ExtraEdgeKinds: graph.Kinds{kind}})
	for _, tc := range []struct {
		name  string
		value any
		want  any
	}{
		{"positive", 1.9, int64(1)},
		{"negative", -1.5, int64(-1)},
		{"positive half", 1.5, int64(1)},
		{"negative near integer", -1.9, int64(-1)},
		{"positive near zero", 0.9, int64(0)},
		{"negative near zero", -0.9, int64(0)},
		{"positive zero", 0.0, int64(0)},
		{"negative zero", math.Copysign(0, -1), int64(0)},
		{"integral float", 2.0, int64(2)},
		{"large integer", int64(9007199254740993), int64(9007199254740993)},
		{"maximum integer", int64(math.MaxInt64), int64(math.MaxInt64)},
		{"minimum integer", int64(math.MinInt64), int64(math.MinInt64)},
		{"large fractional float", 4503599627370495.5, int64(4503599627370495)},
		{"maximum string", "9223372036854775807", int64(math.MaxInt64)},
		{"minimum string", "-9223372036854775808", int64(math.MinInt64)},
		{"null", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				node, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": tc.value}), kind)
				if err != nil {
					return err
				}
				_, err = tx.CreateRelationshipByIDs(node.ID, node.ID, kind, graph.AsProperties(map[string]any{"x": tc.value}))
				return err
			}))
			for _, query := range []string{
				"RETURN toInteger($x)",
				"MATCH (n:ToIntegerInput) RETURN toInteger(n.x)",
				"MATCH (n:ToIntegerInput) WITH n.x AS x RETURN toInteger(x)",
				"MATCH ()-[r:ToIntegerInput]->() RETURN toInteger(r.x)",
				"MATCH ()-[r:ToIntegerInput]->() WITH r.x AS x RETURN toInteger(x)",
			} {
				t.Run(query, func(t *testing.T) {
					require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
						result := tx.Query(query, map[string]any{"x": tc.value})
						defer result.Close()
						require.True(t, result.Next(), "%v", result.Error())
						require.Equal(t, []any{tc.want}, result.Values())
						require.False(t, result.Next())
						return result.Error()
					}))
				})
			}
		})
	}
	// Keep float parameters in their native representation at the int64 limits;
	// a float-to-numeric intermediate can round away significant digits.
	for _, value := range []float64{math.Nextafter(0x1p63, 0), -0x1p63} {
		require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
			result := tx.Query("RETURN toInteger($x)", map[string]any{"x": value})
			defer result.Close()
			require.True(t, result.Next(), "%v", result.Error())
			require.Equal(t, []any{int64(value)}, result.Values())
			require.False(t, result.Next())
			return result.Error()
		}))
	}
}
