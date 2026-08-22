package pg

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

var (
	// benchmarkDecodedJSONValues retains decoded rows so benchmark work cannot be optimized away.
	benchmarkDecodedJSONValues []any

	// benchmarkResultKeys retains cached column names so benchmark work cannot be optimized away.
	benchmarkResultKeys []string
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

// TestDecodeJSONValuesPreservesDecodedStringScalars verifies JSON-typed strings already decoded by pgx are not reinterpreted as JSON tokens.
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
		expected = append([]any(nil), values...)
	)

	decoded := decodeJSONValues(values, fields)
	require.Equal(t, expected, decoded)
	require.Same(t, &values[0], &decoded[0])
}

// TestDecodeJSONValuesReusesInputSlice verifies JSON replacement occurs in the pgx-owned row slice without an extra copy.
func TestDecodeJSONValuesReusesInputSlice(t *testing.T) {
	var (
		values = []any{
			[]byte(`{"name":"alpha"}`),
			int64(42),
		}
		fields = []pgconn.FieldDescription{
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.Int8OID},
		}
	)

	decoded := decodeJSONValues(values, fields)

	require.Same(t, &values[0], &decoded[0])
	require.Equal(t, map[string]any{"name": "alpha"}, decoded[0])
	require.Equal(t, int64(42), decoded[1])
}

// TestDecodeJSONValuesDoesNotAllocateForDecodedFields verifies already-decoded fields follow the zero-allocation path.
func TestDecodeJSONValuesDoesNotAllocateForDecodedFields(t *testing.T) {
	var (
		values = []any{
			map[string]any{"name": "alpha"},
			int64(42),
		}
		fields = []pgconn.FieldDescription{
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.Int8OID},
		}
	)

	require.Zero(t, testing.AllocsPerRun(100, func() {
		decodeJSONValues(values, fields)
	}))
}

// TestQueryResultCachesKeysAcrossRows verifies column-name storage is reused while row values remain independently owned.
func TestQueryResultCachesKeysAcrossRows(t *testing.T) {
	mock, err := pgxmock.NewConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mock.Close(context.Background()))
		require.NoError(t, mock.ExpectationsWereMet())
	})

	mock.ExpectQuery("select values").WillReturnRows(
		pgxmock.NewRows([]string{"name", "count"}).
			AddRow("alpha", int64(1)).
			AddRow("beta", int64(2)),
	)
	mock.ExpectClose()

	rows, err := mock.Query(context.Background(), "select values")
	require.NoError(t, err)

	result := &queryResult{
		rows: rows,
	}
	require.True(t, result.Next())
	require.Equal(t, []string{"name", "count"}, result.Keys())
	firstKey := &result.Keys()[0]
	firstValues := result.Values()
	require.Equal(t, []any{"alpha", int64(1)}, firstValues)

	require.True(t, result.Next())
	require.Same(t, firstKey, &result.Keys()[0])
	require.Equal(t, []any{"beta", int64(2)}, result.Values())
	// Rows.Values owns each returned row slice. Advancing the cursor must not
	// mutate values retained by a caller or mapper from the previous row.
	require.Equal(t, []any{"alpha", int64(1)}, firstValues)
	require.False(t, result.Next())
	require.NoError(t, result.Error())
}

// TestQueryResultCacheKeysDoesNotAllocateAfterInitialization verifies repeated key access performs no allocation.
func TestQueryResultCacheKeysDoesNotAllocateAfterInitialization(t *testing.T) {
	var (
		result = &queryResult{}
		fields = []pgconn.FieldDescription{
			{Name: "name"},
			{Name: "count"},
		}
	)
	result.cacheKeys(fields)

	require.Zero(t, testing.AllocsPerRun(100, func() {
		result.cacheKeys(fields)
	}))
}

// BenchmarkDecodeJSONValuesDecodedFields compares in-place decoding with the previous shallow-copy approach.
func BenchmarkDecodeJSONValuesDecodedFields(b *testing.B) {
	var (
		values = []any{
			map[string]any{"name": "alpha"},
			int64(42),
		}
		fields = []pgconn.FieldDescription{
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.Int8OID},
		}
	)

	b.Run("in_place", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchmarkDecodedJSONValues = decodeJSONValues(values, fields)
		}
	})

	b.Run("shallow_copy_reference", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			copiedValues := make([]any, len(values))
			copy(copiedValues, values)
			benchmarkDecodedJSONValues = decodeJSONValues(copiedValues, fields)
		}
	})
}

// BenchmarkQueryResultCacheKeys compares cached column names with rebuilding them for every row.
func BenchmarkQueryResultCacheKeys(b *testing.B) {
	fields := []pgconn.FieldDescription{
		{Name: "name"},
		{Name: "count"},
	}

	b.Run("cached", func(b *testing.B) {
		result := &queryResult{}
		result.cacheKeys(fields)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			result.cacheKeys(fields)
			benchmarkResultKeys = result.keys
		}
	})

	b.Run("rebuild_reference", func(b *testing.B) {
		result := &queryResult{}
		b.ReportAllocs()
		for b.Loop() {
			result.keys = make([]string, len(fields))
			for idx, field := range fields {
				result.keys[idx] = field.Name
			}
			benchmarkResultKeys = result.keys
		}
	})
}
