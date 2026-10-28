package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestToIntegerTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				query     string
				params    map[string]any
				truncates bool
			}{
				{query: "RETURN toInteger(1.9)", truncates: true},
				{query: "RETURN toInteger(-1.5)", truncates: true},
				{query: "RETURN toInteger($x)", params: map[string]any{"x": float32(1.9)}, truncates: true},
				{query: "RETURN toInteger($x)", params: map[string]any{"x": 1.9}, truncates: true},
				{query: "WITH -1.9 AS x RETURN toInteger(x)", truncates: true},
				{query: "UNWIND [1.9,-1.5,0.9,-0.9] AS x RETURN toInteger(x)", truncates: true},
				{query: "UNWIND $xs AS x WITH x AS value RETURN toInteger(value)", params: map[string]any{"xs": []float64{1.9, -1.5}}, truncates: true},
				{query: "UNWIND $xs AS x RETURN toInteger(x)", params: map[string]any{"xs": []float32{1.9, -1.5}}, truncates: true},
				{query: "UNWIND $xs AS x RETURN toInteger(x)", params: map[string]any{"xs": []int16{1, -1}}},
				{query: "UNWIND $xs AS x RETURN toInteger(x)", params: map[string]any{"xs": []int32{1, -1}}},
				{query: "UNWIND $xs AS x RETURN toInteger(x)", params: map[string]any{"xs": []int64{9007199254740993, -9007199254740993}}},
				{query: "UNWIND [1,2] AS x RETURN toInteger(x)"},
				{query: "UNWIND ['1','2'] AS x RETURN toInteger(x)"},
				{query: "MATCH (n) RETURN toInteger(n.x)", truncates: true},
				{query: "MATCH (n) WITH n.x AS x RETURN toInteger(x)", truncates: true},
				{query: "MATCH ()-[r]->() RETURN toInteger(r.x)", truncates: true},
				{query: "RETURN toInteger(9007199254740993)"},
				{query: "RETURN toInteger('9223372036854775807')"},
				{query: "RETURN toInteger($x)", params: map[string]any{"x": int64(9007199254740993)}},
				{query: "RETURN toInteger(null)"},
			} {
				t.Run(tc.query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), tc.params, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					if tc.truncates {
						require.Contains(t, formatted.Statement, "trunc(")
					} else {
						require.NotContains(t, formatted.Statement, "trunc(")
					}
					if tc.params != nil {
						require.NotContains(t, formatted.Statement, "::numeric", "native parameters must retain their precision")
					}
				})
			}
		})
	}
}
