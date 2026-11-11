package jsonutil

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNormalizeNumbers checks integer precision, floating-point ranges, and nested JSON conversion.
func TestNormalizeNumbers(t *testing.T) {
	for _, test := range []struct {
		// input is the JSON-compatible value before numeric normalization.
		input any
		// want is the normalized value, including the expected concrete numeric types.
		want any
	}{
		{
			input: json.Number("0"),
			want:  int64(0),
		},
		{
			input: json.Number("-0"),
			want:  int64(0),
		},
		{
			input: json.Number("9007199254740993"),
			want:  int64(9007199254740993),
		},
		{
			input: json.Number("-9007199254740993"),
			want:  int64(-9007199254740993),
		},
		{
			input: json.Number("9223372036854775807"),
			want:  int64(math.MaxInt64),
		},
		{
			input: json.Number("-9223372036854775808"),
			want:  int64(math.MinInt64),
		},
		{
			input: json.Number("0.1"),
			want:  0.1,
		},
		{
			input: json.Number("1e3"),
			want:  float64(1000),
		},
		{
			input: json.Number("1E-3"),
			want:  0.001,
		},
		{
			input: json.Number("1.7976931348623157e308"),
			want:  math.MaxFloat64,
		},
		{
			input: json.Number("5e-324"),
			want:  math.SmallestNonzeroFloat64,
		},
		{
			input: "9007199254740993",
			want:  "9007199254740993",
		},
		{
			input: nil,
			want:  nil,
		}, {
			input: true,
			want:  true,
		}, {
			input: int64(8),
			want:  int64(8),
		}, {
			input: 1.25,
			want:  1.25,
		},
		{
			input: map[string]any(nil),
			want:  map[string]any(nil),
		}, {
			input: []any(nil),
			want:  []any(nil),
		},
		{
			input: map[string]any{},
			want:  map[string]any{},
		}, {
			input: []any{},
			want:  []any{},
		},
		{
			input: map[string]any{"list": []any{json.Number("9007199254740993"), map[string]any{"n": json.Number("1.25")}}},
			want:  map[string]any{"list": []any{int64(9007199254740993), map[string]any{"n": 1.25}}},
		},
	} {
		got, err := NormalizeNumbers(test.input)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}

// TestNormalizeNumbersRejectsUnsupportedRanges checks range errors retain their nested property and index context.
func TestNormalizeNumbersRejectsUnsupportedRanges(t *testing.T) {
	for _, value := range []string{"9223372036854775808", "-9223372036854775809", "1e309", "-1e309"} {
		_, err := NormalizeNumbers(map[string]any{"outer": []any{map[string]any{"inner": json.Number(value)}}})
		require.ErrorContains(t, err, `property "outer": index 0: property "inner": JSON`)
	}
}
