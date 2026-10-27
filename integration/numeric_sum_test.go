//go:build manual_integration

package integration

import (
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestNumericSum checks committed inputs and concrete result types independently
// of the JSON fixture loader and its numeric assertion conversions.
func TestNumericSum(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("NumericSum")}})
	for _, tc := range []struct {
		name, query string
		values      []any
		want        any
	}{
		{"large integer", "RETURN sum(n.x)", []any{int64(9007199254740993), int64(2)}, int64(9007199254740995)},
		{"maximum", "RETURN sum(n.x)", []any{int64(math.MaxInt64)}, int64(math.MaxInt64)},
		{"minimum", "RETURN sum(n.x)", []any{int64(math.MinInt64)}, int64(math.MinInt64)},
		{"negative", "RETURN sum(n.x)", []any{int64(-9007199254740993), int64(-2)}, int64(-9007199254740995)},
		{"cancellation", "RETURN sum(n.x)", []any{int64(9007199254740993), int64(-9007199254740992)}, int64(1)},
		{"zero", "RETURN sum(n.x)", []any{int64(0), int64(0)}, int64(0)},
		{"empty", "RETURN sum(n.x)", nil, int64(0)},
		{"nulls", "RETURN sum(n.x)", []any{nil, nil}, int64(0)},
		{"nullable", "RETURN sum(n.x)", []any{nil, int64(9007199254740993)}, int64(9007199254740993)},
		{"float", "RETURN sum(n.x)", []any{1.25, 2.5}, 3.75},
		{"integral float result", "RETURN sum(n.x)", []any{1.25, 0.75}, 2.0},
		{"mixed", "RETURN sum(n.x)", []any{int64(2), 1.25}, 3.25},
		{"distinct", "RETURN sum(DISTINCT n.x)", []any{int64(9007199254740993), int64(9007199254740993), int64(2)}, int64(9007199254740995)},
		{"carried property", "WITH n.x AS x RETURN sum(x)", []any{int64(9007199254740993), int64(2)}, int64(9007199254740995)},
		{"carried result", "WITH sum(n.x) AS total RETURN total", []any{int64(9007199254740993), int64(2)}, int64(9007199254740995)},
		{"arithmetic result", "WITH sum(n.x) AS total RETURN total / 2", []any{int64(9007199254740993), int64(1)}, int64(4503599627370497)},
		{"repeated aggregate", "WITH sum(n.x) AS total RETURN sum(total)", []any{int64(9007199254740993), int64(2)}, int64(9007199254740995)},
		{"literal", "RETURN sum(9007199254740993)", []any{int64(1)}, int64(9007199254740993)},
		{"float literal", "RETURN sum(1.0)", []any{int64(1)}, 1.0},
		{"carried float literal", "WITH 1.0 AS x RETURN sum(x)", []any{int64(1)}, 1.0},
		{"null literal", "RETURN sum(null)", []any{int64(1)}, int64(0)},
		// AVG keeps its existing representation; check its ordinary value and
		// null identity without promising exact integer output or new semantics.
		{"average", "RETURN avg(n.x) = 1.5", []any{int64(1), int64(2)}, true},
		{"empty average", "RETURN avg(n.x)", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				for _, value := range tc.values {
					if _, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": value}), graph.StringKind("NumericSum")); err != nil {
						return err
					}
				}
				return nil
			}))
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("MATCH (n:NumericSum) "+tc.query, nil)
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{tc.want}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
	for _, literal := range []string{"'123'", "true", "[1, 2]"} {
		t.Run("invalid "+literal, func(t *testing.T) {
			err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("RETURN sum("+literal+")", nil)
				defer result.Close()
				for result.Next() {
				}
				return result.Error()
			})
			require.Error(t, err)
		})
	}
}
