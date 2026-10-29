package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestPropertyUnwindTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, query := range []string{
				"MATCH (n) UNWIND n.arr AS x RETURN x",
				"MATCH (n) UNWIND (n.arr) AS x RETURN x + 1",
				"MATCH ()-[r]->() UNWIND r.arr AS x RETURN toUpper(x)",
				"MATCH (n) WITH n.arr AS xs UNWIND xs AS x RETURN x",
				"MATCH (n) UNWIND n.arr AS xs UNWIND xs AS x RETURN x",
				"MATCH (n) UNWIND n.arr AS x SET n.value = x",
				"MATCH (n) UNWIND n.arr AS x CREATE (:NodeKind1 {value:x})",
			} {
				t.Run(query, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, "jsonb_array_elements(")
					require.NotContains(t, formatted.Statement, "->> E'arr'")
				})
			}
		})
	}
}
