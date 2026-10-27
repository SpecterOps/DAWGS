package pg

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

var (
	// benchmarkDecodedJSONValues retains decoded rows so benchmark work cannot be optimized away.
	benchmarkDecodedJSONValues []any

	// benchmarkResultKeys retains cached column names so benchmark work cannot be optimized away.
	benchmarkResultKeys []string
)

func TestDecodeJSONValue(t *testing.T) {
	for _, test := range []struct {
		source string
		want   any
	}{
		{"42", int64(42)},
		{`"alpha"`, "alpha"},
		{`{"name":"alpha","value":9007199254740993}`, map[string]any{"name": "alpha", "value": int64(9007199254740993)}},
		{`["alpha",true,null]`, []any{"alpha", true, nil}},
		{`"[1, 2]"`, "[1, 2]"},
		{"true", true},
		{"null", nil},
	} {
		t.Run(test.source, func(t *testing.T) {
			value, ok := decodeJSONValue([]byte(test.source))
			require.True(t, ok)
			require.Equal(t, test.want, value)
		})
	}
	for _, source := range []string{"", "{", "[1,", "42 true", "9223372036854775808"} {
		_, ok := decodeJSONValue([]byte(source))
		require.False(t, ok, source)
	}
}

// TestDecodeJSONValuesPreservesDecodedStringScalars verifies JSON-typed strings already decoded by pgx are not reinterpreted as JSON tokens.
func TestDecodeJSONValuesPreservesDecodedStringScalars(t *testing.T) {
	for _, source := range []string{
		"", "alpha", "1234", "true", "false", "null",
		"[1, 2]", `{"k":"v"}`, `"q"`, "[]", "{}", `""`,
		" \t[1, 2]\n", `["nested",{"flag":true}]`,
		`"\u0041"`, "{broken", "[1,", `"unterminated`,
	} {
		t.Run(source, func(t *testing.T) {
			_, ok := decodeJSONValue(source)
			require.False(t, ok)
			for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID, pgtype.TextOID} {
				values := []any{source}
				decoded := decodeJSONValues(values, []pgconn.FieldDescription{{DataTypeOID: oid}})
				require.Equal(t, []any{source}, decoded)
				require.Same(t, &values[0], &decoded[0])
			}
		})
	}
}

func TestDecodeJSONValuesPreservesNonJSONBytes(t *testing.T) {
	values := []any{[]byte(`{"k":"v"}`), []byte(`[1, 2]`)}
	expected := append([]any(nil), values...)
	decoded := decodeJSONValues(values, []pgconn.FieldDescription{
		{DataTypeOID: pgtype.ByteaOID}, {DataTypeOID: pgtype.TextOID},
	})
	require.Equal(t, expected, decoded)
}

// Actual JSON collections must still map to graph values while preserving the
// JSON-like strings inside their property maps.
func TestDecodeJSONValuesMapsGraphCollections(t *testing.T) {
	values := decodeJSONValues([]any{[]byte(`[
 {"id":1,"kind_ids":[],"properties":{"text":"[1, 2]"}},
 {"id":2,"kind_ids":[],"properties":{"text":"\"q\""}}
]`)}, []pgconn.FieldDescription{{DataTypeOID: pgtype.JSONBOID}})
	mapper := NewValueMapper(context.Background(), pgutil.NewInMemoryKindMapper())
	var nodes []graph.Node
	require.True(t, mapper.Map(values[0], &nodes))
	require.Len(t, nodes, 2)
	require.Equal(t, graph.ID(1), nodes[0].ID)
	require.Equal(t, graph.ID(2), nodes[1].ID)
	require.Equal(t, "[1, 2]", nodes[0].Properties.Map["text"])
	require.Equal(t, `"q"`, nodes[1].Properties.Map["text"])
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
			`{"k":"v"}`,
			`[1, 2]`,
			`"q"`,
		}
		fields = []pgconn.FieldDescription{
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.Int8OID},
			{DataTypeOID: pgtype.JSONOID},
			{DataTypeOID: pgtype.JSONBOID},
			{DataTypeOID: pgtype.JSONBOID},
		}
	)

	original := append([]any(nil), values...)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		copy(values, original)
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
