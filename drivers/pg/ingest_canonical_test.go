package pg

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestCanonicalIngestValueGoldenBytes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "null", value: nil, want: "00"},
		{name: "false", value: false, want: "01"},
		{name: "true", value: true, want: "02"},
		{name: "UTF-8 string", value: "é", want: "030000000000000002c3a9"},
		{name: "string bytes are not JSON escaped", value: "\n\"\\", want: "0300000000000000030a225c"},
		{name: "positive number", value: json.Number("1"), want: "040000000000000000013100"},
		{name: "negative normalized number", value: json.Number("-12.300e2"), want: "0401000000000000000331323302"},
		{name: "signed zero", value: json.Number("-0.00e+99"), want: "040000000000000000013000"},
		{name: "large positive exponent", value: json.Number("1e131071"), want: "0400000000000000000131feff0f"},
		{name: "large negative exponent", value: json.Number("1e-16383"), want: "0400000000000000000131fdff01"},
		{
			name: "array preserves order",
			value: []any{
				nil,
				true,
			},
			want: "0500000000000000020002",
		},
		{
			name: "nested object sorts keys by raw UTF-8 bytes",
			value: map[string]any{
				"é": []any{json.Number("2"), false},
				"z": "v",
			},
			want: "060000000000000002" +
				"00000000000000017a03000000000000000176" +
				"0000000000000002c3a9050000000000000002" +
				"04000000000000000001320001",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			require.NoError(t, writeCanonicalIngestValue(&encoded, test.value))
			require.Equal(t, test.want, hex.EncodeToString(encoded.Bytes()))
		})
	}
}

func TestCanonicalIngestNumbersNormalizeEquivalentSpellings(t *testing.T) {
	spellings := []string{"1", "1.0", "1e0", "10e-1", "0.100e1"}
	var want []byte

	for index, spelling := range spellings {
		var encoded bytes.Buffer
		require.NoError(t, writeCanonicalIngestValue(&encoded, json.Number(spelling)))
		if index == 0 {
			want = bytes.Clone(encoded.Bytes())
		} else {
			require.Equal(t, want, encoded.Bytes(), spelling)
		}
	}
}

func TestCanonicalIngestValueRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "invalid UTF-8 string", value: string([]byte{0xff})},
		{name: "invalid UTF-8 key", value: map[string]any{string([]byte{0xff}): nil}},
		{name: "NUL string", value: "before\x00after"},
		{name: "NUL object key", value: map[string]any{"before\x00after": nil}},
		{name: "float", value: 1.25},
		{name: "invalid JSON number", value: json.Number("01")},
		{name: "function", value: func() {}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, writeCanonicalIngestValue(&bytes.Buffer{}, test.value))
		})
	}
}

func TestCanonicalIngestNodeContentRejectsNULKind(t *testing.T) {
	_, err := hashIngestNodeContent(graph.Kinds{graph.StringKind("before\x00after")}, map[string]any{})
	require.Error(t, err)
}

func TestEmptyEdgeContentHashGolden(t *testing.T) {
	hash, err := hashIngestEdgeContent(map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "c2379af07cdafae0000d3a5974d738ab", hex.EncodeToString(hash[:]))
}

func TestNodeContentHashSortsKindsAndExcludesOnlyObjectID(t *testing.T) {
	properties := map[string]any{
		"name":     "alice",
		"objectid": "S-1-5-21",
	}
	first, err := hashIngestNodeContent(graph.Kinds{graph.StringKind("User"), graph.StringKind("Équipe")}, properties)
	require.NoError(t, err)
	second, err := hashIngestNodeContent(graph.Kinds{graph.StringKind("Équipe"), graph.StringKind("User")}, map[string]any{
		"name":     "alice",
		"objectid": "different",
	})
	require.NoError(t, err)
	require.Equal(t, first, second)

	changedProperty, err := hashIngestNodeContent(graph.Kinds{graph.StringKind("User"), graph.StringKind("Équipe")}, map[string]any{
		"name":     "bob",
		"objectid": "S-1-5-21",
	})
	require.NoError(t, err)
	require.NotEqual(t, first, changedProperty)

	changedKind, err := hashIngestNodeContent(graph.Kinds{graph.StringKind("Group"), graph.StringKind("Équipe")}, properties)
	require.NoError(t, err)
	require.NotEqual(t, first, changedKind)
	require.Equal(t, "S-1-5-21", properties["objectid"], "hashing must not mutate the caller's map")
}

func TestCanonicalIngestContentHashDomainsAndEdgeObjectID(t *testing.T) {
	properties := map[string]any{"objectid": "S-1-5-21"}
	nodeHash, err := hashIngestNodeContent(nil, properties)
	require.NoError(t, err)
	edgeHash, err := hashIngestEdgeContent(properties)
	require.NoError(t, err)
	emptyEdgeHash, err := hashIngestEdgeContent(map[string]any{})
	require.NoError(t, err)

	require.NotEqual(t, nodeHash, edgeHash, "node and edge content domains must differ")
	require.NotEqual(t, edgeHash, emptyEdgeHash, "edge objectid participates in its content hash")
}

func TestNormalizeIngestPropertiesUsesJSONNumbersWithoutMutation(t *testing.T) {
	nested := map[string]any{
		"integer": int64(42),
		"array":   []any{float64(1.25), "value"},
	}
	properties := graph.AsProperties(map[string]any{
		"nested": nested,
		"bool":   true,
	})

	normalized, err := normalizeIngestProperties(properties)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"nested": map[string]any{
			"integer": json.Number("42"),
			"array":   []any{json.Number("1.25"), "value"},
		},
		"bool": true,
	}, normalized)
	require.IsType(t, int64(0), nested["integer"])
	require.IsType(t, float64(0), nested["array"].([]any)[0])
}

func TestNormalizeIngestPropertiesTreatsNilAsEmpty(t *testing.T) {
	normalized, err := normalizeIngestProperties(nil)
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, normalized)
}

func TestNormalizeIngestPropertiesRejectsUnsupportedJSON(t *testing.T) {
	overlappingStrings := []string{"valid", string([]byte{0xff})}
	tests := []struct {
		name  string
		value any
	}{
		{name: "invalid UTF-8 value", value: string([]byte{0xff})},
		{name: "invalid UTF-8 key", value: map[string]any{string([]byte{0xff}): "value"}},
		{name: "NUL value", value: "before\x00after"},
		{name: "NUL key", value: map[string]any{"before\x00after": "value"}},
		{name: "invalid UTF-8 in overlapping slices", value: []any{overlappingStrings[:1], overlappingStrings}},
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
		{name: "function", value: func() {}},
		{name: "channel", value: make(chan int)},
		{name: "complex", value: complex(1, 2)},
		{name: "trailing JSON", value: trailingJSONValue{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizeIngestProperties(graph.AsProperties(map[string]any{"value": test.value}))
			require.Error(t, err)
		})
	}
}

func TestNormalizeIngestPropertiesRejectsUnpairedEncodedJSONSurrogates(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
	}{
		{name: "lone high surrogate", encoded: `"\ud800"`},
		{name: "lone low surrogate", encoded: `"\udc00"`},
		{name: "high surrogate followed by ordinary code point", encoded: `"\ud800\u0041"`},
		{name: "high surrogate followed by high surrogate", encoded: `"\ud800\udbff"`},
		{name: "low surrogate followed by low surrogate", encoded: `"\udc00\udfff"`},
		{name: "high surrogate separated from low surrogate", encoded: `"\ud800x\udc00"`},
	}
	sources := []struct {
		name  string
		value func(string) any
	}{
		{
			name: "json.RawMessage",
			value: func(encoded string) any {
				return json.RawMessage(encoded)
			},
		},
		{
			name: "custom json.Marshaler",
			value: func(encoded string) any {
				return encodedIngestJSON(encoded)
			},
		},
	}

	for _, source := range sources {
		for _, test := range tests {
			t.Run(source.name+"/"+test.name, func(t *testing.T) {
				_, err := normalizeIngestProperties(graph.AsProperties(map[string]any{
					"value": source.value(test.encoded),
				}))

				require.ErrorContains(t, err, "encoded ingest properties")
				require.ErrorContains(t, err, "surrogate")
			})
		}
	}
}

func TestNormalizeIngestPropertiesAcceptsValidEncodedJSONSurrogatePairs(t *testing.T) {
	direct, err := normalizeIngestProperties(graph.AsProperties(map[string]any{
		"value": "😀",
	}))
	require.NoError(t, err)

	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "json.RawMessage lowercase", value: json.RawMessage(`"\ud83d\ude00"`)},
		{name: "custom json.Marshaler uppercase", value: encodedIngestJSON(`"\uD83D\uDE00"`)},
		{name: "ordinary custom escape", value: encodedIngestJSON(`"caf\u00e9"`)},
		{name: "escaped literal backslash", value: json.RawMessage(`"\\ud800"`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			normalized, err := normalizeIngestProperties(graph.AsProperties(map[string]any{
				"value": test.value,
			}))
			require.NoError(t, err)

			switch test.name {
			case "json.RawMessage lowercase", "custom json.Marshaler uppercase":
				require.Equal(t, direct, normalized)
			case "ordinary custom escape":
				require.Equal(t, "café", normalized["value"])
			case "escaped literal backslash":
				require.Equal(t, `\ud800`, normalized["value"])
			}
		})
	}
}

func TestNormalizeIngestPropertiesEnforcesPostgreSQLNumericLimits(t *testing.T) {
	for _, value := range []json.Number{"9e131071", "1e-16383", "0e1073741823"} {
		_, err := normalizeIngestProperties(graph.AsProperties(map[string]any{"number": value}))
		require.NoError(t, err, value.String())
	}

	for _, value := range []json.Number{
		"1e131072",
		"1e-16384",
		"0e-16384",
		"0e1073741824",
		json.Number("1." + strings.Repeat("0", 16384)),
	} {
		_, err := normalizeIngestProperties(graph.AsProperties(map[string]any{"number": value}))
		require.Error(t, err, value.String())
	}
}

type trailingJSONValue struct{}

func (trailingJSONValue) MarshalJSON() ([]byte, error) {
	return []byte(`{} {}`), nil
}

type encodedIngestJSON string

func (s encodedIngestJSON) MarshalJSON() ([]byte, error) {
	return []byte(s), nil
}
