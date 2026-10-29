package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestDeleteConnectedNodeTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query string
			guard bool
		}{
			{"MATCH (n) DELETE n", true},
			{"MATCH (n) DETACH DELETE n", false},
			{"MATCH ()-[r]->() DELETE r", false},
			{"MATCH (n)-[r]->(m) DELETE n, r", true},
			{"MATCH (n)-[r]->(m) DELETE n DETACH DELETE m", true},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(result)
				require.NoError(t, err)
				if tc.guard {
					require.Contains(t, formatted.Statement, "cypher_assert_node_deletable(42,")
				} else {
					require.NotContains(t, formatted.Statement, "cypher_assert_node_deletable")
				}
			})
		}
	}
}
