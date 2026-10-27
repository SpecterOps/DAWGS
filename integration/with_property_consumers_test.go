//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestWithPropertyConsumers checks committed values and their actual Go types,
// independently of the template fixture's numeric conversions.
func TestWithPropertyConsumers(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("WithConsumer")}})
	for _, tc := range []struct {
		name, expression string
		value, want      any
	}{
		{"string", "toString(x)", "Alice", "Alice"},
		{"JSON-looking string", "toString(x)", `{"k":"v"}`, `{"k":"v"}`},
		{"escaped string", "toString(x)", "a\"b\\c\n雪", "a\"b\\c\n雪"},
		{"empty string", "toString(x)", "", ""},
		{"integer", "toString(x)", int64(9007199254740993), "9007199254740993"},
		{"float", "toString(x)", 1.25, "1.25"},
		{"Boolean", "toString(x)", true, "true"},
		{"null", "toString(x)", nil, nil},
		{"lower", "toLower(x)", "ALIce", "alice"},
		{"upper", "toUpper(x)", "ALIce", "ALICE"},
		{"split", "split(x, ',')", "a,b", []any{"a", "b"}},
		{"split delimiter", "split('a,b', x)", ",", []any{"a", "b"}},
		{"string size", "size(x)", "a雪😀", int64(3)},
		{"list head", "head(x)", []int64{1, 2}, int64(1)},
		{"list tail", "tail(x)", []int64{1, 2}, []any{int64(2)}},
		{"list head arithmetic", "head(x) + 1", []int64{1, 2}, int64(2)},
		{"list coalesce head", "head(coalesce(x, [1, 2]))", nil, int64(1)},
		{"list coalesce tail", "tail(coalesce(x, [1, 2]))", nil, []any{int64(2)}},
		{"empty head", "head(x)", []int64{}, nil},
		{"empty tail", "tail(x)", []int64{}, []any{}},
		{"null head", "head(x)", nil, nil},
		{"null tail", "tail(x)", nil, nil},
		{"list size", "size(x)", []int64{1, 2}, int64(2)},
		{"empty list size", "size(x)", []int64{}, int64(0)},
		{"null size", "size(x)", nil, nil},
		{"Boolean NOT", "NOT x", true, false},
		{"Boolean AND", "x AND true", true, true},
		{"Boolean OR", "x OR true", nil, true},
		{"coalesced Boolean", "NOT coalesce(x, false)", nil, true},
		{"date conversion", "date(x) = date('2026-01-02')", "2026-01-02", true},
		{"null integer conversion", "toInteger(x)", nil, nil},
		{"integer conversion", "toInteger(x)", "123", int64(123)},
		{"coalesce value", "coalesce(x, 'fallback')", "Alice", "Alice"},
		{"coalesce null", "coalesce(x, 'fallback')", nil, "fallback"},
		{"coalesce mixed", "coalesce(x, 0)", "Alice", "Alice"},
		{"coalesce empty list size", "size(coalesce(x, []))", nil, int64(0)},
		{"coalesce size", "size(coalesce(x, 'fallback'))", nil, int64(8)},
		{"string equality", "x = 'Alice'", "Alice", true},
		{"string predicate", "x STARTS WITH 'Al'", "Alice", true},
		{"arithmetic", "x + 1", int64(9007199254740993), int64(9007199254740994)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				_, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": tc.value}))
				return err
			}))
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("MATCH (n) WITH n.x AS x RETURN "+tc.expression+" AS value", nil)
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{tc.want}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
	for _, expression := range []string{"toString(x)", "toLower(x)", "toUpper(x)", "split(x, ',')", "size(x)", "head(x)"} {
		t.Run("invalid "+expression, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				_, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": int64(123)}))
				return err
			}))
			// toString accepts numbers; use a list for its unsupported-value control.
			query := "MATCH (n) WITH n.x AS x RETURN " + expression
			if expression == "toString(x)" {
				query = "MATCH (n) WITH [1, 2] AS x RETURN " + expression
			}
			err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(query, nil)
				defer result.Close()
				for result.Next() {
				}
				return result.Error()
			})
			require.Error(t, err)
		})
	}
}
