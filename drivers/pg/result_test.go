package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeJSONValue(t *testing.T) {
	t.Run("number", func(t *testing.T) {
		value, ok := decodeJSONValue([]byte("42"))
		require.True(t, ok)
		require.Equal(t, float64(42), value)
	})

	t.Run("string", func(t *testing.T) {
		value, ok := decodeJSONValue(`"alpha"`)
		require.True(t, ok)
		require.Equal(t, "alpha", value)
	})

	t.Run("non json string", func(t *testing.T) {
		_, ok := decodeJSONValue("alpha")
		require.False(t, ok)
	})
}
