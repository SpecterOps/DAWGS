package pg

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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

	t.Run("object string", func(t *testing.T) {
		value, ok := decodeJSONValue(`{"name":"alpha"}`)
		require.True(t, ok)
		require.Equal(t, map[string]any{"name": "alpha"}, value)
	})

	t.Run("array string", func(t *testing.T) {
		value, ok := decodeJSONValue(`["alpha"]`)
		require.True(t, ok)
		require.Equal(t, []any{"alpha"}, value)
	})

	t.Run("non json string", func(t *testing.T) {
		_, ok := decodeJSONValue("alpha")
		require.False(t, ok)
	})

	t.Run("decoded string scalar tokens", func(t *testing.T) {
		for _, scalarToken := range []string{"1234", "true", "false", "null"} {
			_, ok := decodeJSONValue(scalarToken)
			require.False(t, ok, scalarToken)
		}
	})
}

func TestDecodeJSONValuesPreservesDecodedStringScalars(t *testing.T) {
	var (
		values = []any{
			"1234",
			"true",
			"false",
			"null",
		}
		fields = []pgconn.FieldDescription{
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.JSONBOID},
		}
	)

	require.Equal(t, values, decodeJSONValues(values, fields))
}
