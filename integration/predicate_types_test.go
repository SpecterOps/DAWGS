//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestPropertyPredicateTypes checks committed typed properties and actual Boolean
// or nil results, independently of the JSON fixture and assertion conversions.
func TestPropertyPredicateTypes(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("TypedPredicate")}})
	ClearGraph(t, session.DB, session.Ctx)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"num": int64(123), "str": "123", "bool": true, "list": []int64{2},
			"large": int64(9007199254740993), "small": int64(-9223372036854775808),
			"max": int64(9223372036854775807), "float": 123.5, "null": nil,
		}))
		return err
	}))
	for _, tc := range []struct {
		expression string
		want       any
	}{
		{"n.num STARTS WITH '1'", nil}, {"NOT n.num STARTS WITH '1'", nil},
		{"n.bool CONTAINS 'r'", nil}, {"n.list CONTAINS '2'", nil},
		{"n.missing STARTS WITH 'x'", nil}, {"NOT n.missing STARTS WITH 'x'", nil},
		{"n.null STARTS WITH 'x'", nil}, {"NOT n.null STARTS WITH 'x'", nil},
		{"n.null < n.num", nil},
		{"n.str STARTS WITH n.num", nil}, {"n.str STARTS WITH n.str", true},
		{"n.num =~ '1.*'", nil}, {"NOT n.num =~ 'x'", nil},
		{"n.str STARTS WITH null", nil},
		{"n.str < 200", nil}, {"n.num < '200'", nil}, {"n.num < n.str", nil},
		{"NOT n.str < 200", nil}, {"n.bool < n.num", nil}, {"n.list < n.num", nil},
		{"n.bool < false", false}, {"n.bool > false", true},
		{"n.list < [1, 3]", false}, {"n.list > [1, 3]", true},
		{"n.large > 9007199254740992", true}, {"n.large <= 9007199254740992", false},
		{"n.max > n.large", true}, {"n.small < -9007199254740993", true},
		{"n.num < n.float", true}, {"n.num >= 123.5", false},
		{"n.num < null", nil}, {"n.str < '2'", true}, {"n.str >= '123'", true},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("MATCH (n) RETURN "+tc.expression+" AS value", nil)
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{tc.want}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
	for _, tc := range []struct {
		query string
		want  any
	}{
		{"MATCH (n) WITH n, 200 AS boundary RETURN n.num < boundary AS value", true},
		{"UNWIND [200] AS boundary MATCH (n) RETURN n.num < boundary AS value", true},
		{"MATCH (n) WITH n, '2' AS boundary RETURN n.str < boundary AS value", true},
		{"UNWIND [123] AS value RETURN value STARTS WITH '1' AS matched", nil},
		{"UNWIND ['123'] AS value RETURN value STARTS WITH '1' AS matched", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query(tc.query, nil)
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				require.Equal(t, []any{tc.want}, result.Values())
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}
