//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// TestPropertyArithmetic pins actual Go result types and exact integer values
// using typed stored properties rather than JSON fixture number conversion.
func TestPropertyArithmetic(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph,
		ExtraNodeKinds: graph.Kinds{graph.StringKind("Arithmetic")}})
	for _, tc := range []struct {
		name, expression string
		x, y, want       any
	}{
		{"integer addition", "n.x + n.y", int64(7), int64(2), int64(9)},
		{"integer division", "n.x / n.y", int64(7), int64(2), int64(3)},
		{"negative remainder", "n.x % n.y", int64(-7), int64(2), int64(-1)},
		{"exact addition", "n.x + n.y", int64(9007199254740992), int64(1), int64(9007199254740993)},
		{"exact multiplication", "n.x * n.y", int64(3000000001), int64(3), int64(9000000003)},
		{"float division", "n.x / n.y", 7.5, 2.5, 3.0},
		{"float addition", "n.x + n.y", 7.5, 2.5, 10.0},
		{"float chain", "(n.x + n.y) / 4", 7.5, 2.5, 2.5},
		{"floating remainder", "n.x % n.y", 1.0, 0.1, 0.09999999999999995},
		{"power", "n.x ^ n.y", int64(7), int64(2), 49.0},
		{"string addition", "n.x + n.y", "7", "2", "72"},
		{"mixed addition", "n.x + n.y", "value=", int64(2), "value=2"},
		{"missing operand", "n.x + n.missing", int64(7), int64(2), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				_, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": tc.x, "y": tc.y}))
				return err
			}))
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
		name, expression string
		x, y             any
	}{
		{"Boolean addition", "n.x + n.y", true, false},
		{"string modulo", "n.x % n.y", "7", "2"},
		{"numeric strings are not numbers", "n.x - n.y", "7", "2"},
		{"integer division by zero", "n.x / n.y", int64(7), int64(0)},
		{"integer modulo by zero", "n.x % n.y", int64(7), int64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ClearGraph(t, session.DB, session.Ctx)
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				_, err := tx.CreateNode(graph.AsProperties(map[string]any{"x": tc.x, "y": tc.y}))
				return err
			}))
			err := session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				result := tx.Query("MATCH (n) RETURN "+tc.expression, nil)
				defer result.Close()
				for result.Next() {
				}
				return result.Error()
			})
			require.Error(t, err)
		})
	}
}
