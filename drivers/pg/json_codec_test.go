package pg

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestJSONCodecsPreserveNumbers(t *testing.T) {
	typeMap := pgtype.NewMap()
	installJSONCodecs(typeMap)
	for _, oid := range []uint32{pgtype.JSONOID, pgtype.JSONBOID} {
		for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
			for _, test := range []struct {
				source string
				want   any
			}{
				{"42", int64(42)},
				{"4611686018427387905", int64(4611686018427387905)},
				{"9007199254740993", int64(9007199254740993)},
				{"-9007199254740993", int64(-9007199254740993)},
				{"9223372036854775807", int64(math.MaxInt64)},
				{"-9223372036854775808", int64(math.MinInt64)},
				{"1.0", float64(1)}, {"1e3", float64(1000)}, {"1.25", 1.25},
				{`{"nested":[4611686018427387905,{"min":-9223372036854775808}],"text":"9007199254740993"}`, map[string]any{"nested": []any{int64(4611686018427387905), map[string]any{"min": int64(math.MinInt64)}}, "text": "9007199254740993"}},
				{"null", nil}, {"true", true}, {`"alpha"`, "alpha"},
				{"[]", []any{}}, {"{}", map[string]any{}},
			} {
				encoded, err := typeMap.Encode(oid, format, json.RawMessage(test.source), nil)
				require.NoError(t, err)
				dataType := requirePGType(t, typeMap, oid)
				decoded, err := dataType.Codec.DecodeValue(typeMap, oid, format, encoded)
				require.NoError(t, err, test.source)
				require.Equal(t, test.want, decoded, "oid=%d format=%d %s", oid, format, test.source)
				var scanned any
				require.NoError(t, typeMap.Scan(oid, format, encoded, &scanned))
				require.Equal(t, test.want, scanned)
			}
			for _, source := range []string{"9223372036854775808", "-9223372036854775809", "1e309", `{"nested":[9223372036854775808]}`} {
				encoded, err := typeMap.Encode(oid, format, json.RawMessage(source), nil)
				require.NoError(t, err)
				_, err = requirePGType(t, typeMap, oid).Codec.DecodeValue(typeMap, oid, format, encoded)
				require.ErrorContains(t, err, "range")
			}
		}
	}
}

func TestJSONCodecsArrayElements(t *testing.T) {
	typeMap := pgtype.NewMap()
	installJSONCodecs(typeMap)
	for _, oid := range []uint32{pgtype.JSONArrayOID, pgtype.JSONBArrayOID} {
		for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
			encoded, err := typeMap.Encode(oid, format, []json.RawMessage{json.RawMessage("4611686018427387905"), nil, json.RawMessage(`{"n":9223372036854775807}`)}, nil)
			require.NoError(t, err)
			decoded, err := requirePGType(t, typeMap, oid).Codec.DecodeValue(typeMap, oid, format, encoded)
			require.NoError(t, err)
			require.Equal(t, []any{int64(4611686018427387905), nil, map[string]any{"n": int64(math.MaxInt64)}}, decoded)
		}
	}
}

func TestUnmarshalJSONNumbersTargetsAndErrors(t *testing.T) {
	var properties map[string]any
	require.NoError(t, unmarshalJSONNumbers([]byte(`{"n":4611686018427387905}`), &properties))
	require.Equal(t, int64(4611686018427387905), properties["n"])
	var list []any
	require.NoError(t, unmarshalJSONNumbers([]byte(`[4611686018427387905]`), &list))
	require.Equal(t, []any{int64(4611686018427387905)}, list)
	var typed struct {
		N int64 `json:"n"`
	}
	require.NoError(t, unmarshalJSONNumbers([]byte(`{"n":4611686018427387905}`), &typed))
	require.Equal(t, int64(4611686018427387905), typed.N)
	for _, source := range []string{"", "{", "42 true", "42 trailing"} {
		var value any
		require.Error(t, unmarshalJSONNumbers([]byte(source), &value))
	}
	require.ErrorContains(t, unmarshalJSONNumbers([]byte(`{"n":9223372036854775808}`), &properties), `property "n"`)
	require.ErrorContains(t, unmarshalJSONNumbers([]byte(`[1e309]`), &list), "index 0")
}

func TestDecodeJSONValueExactInteger(t *testing.T) {
	value, ok := decodeJSONValue([]byte("4611686018427387905"))
	require.True(t, ok)
	require.Equal(t, int64(4611686018427387905), value)
}

func BenchmarkJSONNumberDecoding(b *testing.B) {
	for _, source := range []struct{ name, json string }{
		{"scalar", "4611686018427387905"},
		{"properties", `{"large":4611686018427387905,"name":"node","values":[1,2,3],"fraction":1.25}`},
	} {
		for _, decoder := range []struct {
			name      string
			unmarshal func([]byte, any) error
		}{
			{"float64_reference", json.Unmarshal}, {"exact_numbers", unmarshalJSONNumbers},
		} {
			b.Run(source.name+"/"+decoder.name, func(b *testing.B) {
				data := []byte(source.json)
				b.ReportAllocs()
				for b.Loop() {
					var value any
					if err := decoder.unmarshal(data, &value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
