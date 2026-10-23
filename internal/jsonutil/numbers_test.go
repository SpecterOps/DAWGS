package jsonutil

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeNumbers(t *testing.T) {
	for _, test := range []struct {
		input any
		want  any
	}{
		{json.Number("0"), int64(0)},
		{json.Number("-0"), int64(0)},
		{json.Number("9007199254740993"), int64(9007199254740993)},
		{json.Number("-9007199254740993"), int64(-9007199254740993)},
		{json.Number("9223372036854775807"), int64(math.MaxInt64)},
		{json.Number("-9223372036854775808"), int64(math.MinInt64)},
		{json.Number("0.1"), 0.1},
		{json.Number("1e3"), float64(1000)},
		{json.Number("1E-3"), 0.001},
		{json.Number("1.7976931348623157e308"), math.MaxFloat64},
		{json.Number("5e-324"), math.SmallestNonzeroFloat64},
		{"9007199254740993", "9007199254740993"},
		{nil, nil}, {true, true}, {int64(8), int64(8)}, {1.25, 1.25},
		{map[string]any(nil), map[string]any(nil)}, {[]any(nil), []any(nil)},
		{map[string]any{}, map[string]any{}}, {[]any{}, []any{}},
		{map[string]any{"list": []any{json.Number("9007199254740993"), map[string]any{"n": json.Number("1.25")}}},
			map[string]any{"list": []any{int64(9007199254740993), map[string]any{"n": 1.25}}}},
	} {
		got, err := NormalizeNumbers(test.input)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
}

func TestNormalizeNumbersRejectsUnsupportedRanges(t *testing.T) {
	for _, value := range []string{"9223372036854775808", "-9223372036854775809", "1e309", "-1e309"} {
		_, err := NormalizeNumbers(map[string]any{"outer": []any{map[string]any{"inner": json.Number(value)}}})
		require.ErrorContains(t, err, `property "outer": index 0: property "inner": JSON`)
	}
}
