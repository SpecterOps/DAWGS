package opengraph

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDocumentPreservesNumbers(t *testing.T) {
	properties := `{"large":9007199254740993,"wide":4611686018427387905,
		"min":-9223372036854775808,"max":9223372036854775807,"small":7,
		"fraction":1.25,"exponent":1.25e2,"string":"9007199254740993",
		"nested":{"array":[9007199254740993,1.25,null,true,{"negative":-9007199254740993}]}}`
	doc, err := ParseDocument(strings.NewReader(fmt.Sprintf(`{"graph":{
		"nodes":[{"id":"a","properties":%s}],
		"edges":[{"start_id":"a","end_id":"a","kind":"LOOP","properties":%s}]}}`, properties, properties)))
	require.NoError(t, err)
	want := map[string]any{
		"large": int64(9007199254740993), "wide": int64(4611686018427387905),
		"min": int64(math.MinInt64), "max": int64(math.MaxInt64), "small": int64(7),
		"fraction": 1.25, "exponent": float64(125), "string": "9007199254740993",
		"nested": map[string]any{"array": []any{int64(9007199254740993), 1.25, nil, true,
			map[string]any{"negative": int64(-9007199254740993)}}},
	}
	require.Equal(t, want, doc.Graph.Nodes[0].Properties)
	require.Equal(t, want, doc.Graph.Edges[0].Properties)
}

func TestParseDocumentRejectsUnsupportedNumbers(t *testing.T) {
	for _, value := range []string{"9223372036854775808", "-9223372036854775809", "18446744073709551615", "1e309"} {
		for _, location := range []string{"node", "edge"} {
			t.Run(location+"/"+value, func(t *testing.T) {
				nodeProperties, edgeProperties := `{}`, `{}`
				properties := fmt.Sprintf(`{"nested":[{"number":%s}]}`, value)
				if location == "node" {
					nodeProperties = properties
				} else {
					edgeProperties = properties
				}
				input := fmt.Sprintf(`{"graph":{"nodes":[{"id":"a","properties":%s}],
					"edges":[{"start_id":"a","end_id":"a","kind":"LOOP","properties":%s}]}}`, nodeProperties, edgeProperties)
				_, err := ParseDocument(strings.NewReader(input))
				require.ErrorContains(t, err, location)
				require.ErrorContains(t, err, `property "nested"`)
				require.ErrorContains(t, err, "index 0")
				require.ErrorContains(t, err, `property "number"`)
			})
		}
	}
}
