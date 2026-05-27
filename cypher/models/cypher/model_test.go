package cypher_test

import (
	"errors"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

func TestMapLiteralItemsReturnsSortedItems(t *testing.T) {
	aValue := cypher.NewVariableWithSymbol("a_value")
	bValue := cypher.NewVariableWithSymbol("b_value")

	items := cypher.MapLiteral{
		"b": bValue,
		"a": aValue,
	}.Items()

	require.Len(t, items, 2)
	require.Equal(t, "a", items[0].Key)
	require.Same(t, aValue, items[0].Value)
	require.Equal(t, "b", items[1].Key)
	require.Same(t, bValue, items[1].Value)
}

func TestMapLiteralForEachItemReturnsDelegateError(t *testing.T) {
	expectedErr := errors.New("stop iteration")
	var visitedKeys []string

	err := cypher.MapLiteral{
		"c": cypher.NewVariableWithSymbol("c_value"),
		"b": cypher.NewVariableWithSymbol("b_value"),
		"a": cypher.NewVariableWithSymbol("a_value"),
	}.ForEachItem(func(key string, _ cypher.Expression) error {
		visitedKeys = append(visitedKeys, key)
		if key == "b" {
			return expectedErr
		}

		return nil
	})

	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, []string{"a", "b"}, visitedKeys)
}
