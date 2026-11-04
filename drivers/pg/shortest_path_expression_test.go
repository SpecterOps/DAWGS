package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTextRejectsShortestPathExpressions(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, source := range []string{
				"MATCH (a), (b) RETURN shortestPath((a)-[:R*]->(b)) AS p",
				"MATCH (a), (b) RETURN allShortestPaths((a)-[:R*1..3]->(b)) AS p",
				"MATCH (a), (b) WITH shortestPath((a)-[:R*]->(b)) AS p RETURN p",
				"MATCH (a), (b) SET a.value = length(shortestPath((a)-[:R*]->(b)))",
				"MATCH (a), (b) CREATE (:Created {value: length(shortestPath((a)-[:R*]->(b)))})",
				"MATCH (a), (b) WHERE shortestPath((a)-[:R*]->(b)) IS NOT NULL DELETE a",
			} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), source, map[string]any{"value": int64(1)}, 7)
						require.ErrorContains(t, err, "shortest-path expressions are not supported")
						require.NotContains(t, err.Error(), "panic")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}
