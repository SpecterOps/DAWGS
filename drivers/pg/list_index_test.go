package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTextRejectsListIndexing(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, source := range []string{
				"WITH [1,2] AS xs RETURN xs[0] AS x",
				"RETURN $values[$idx] AS x",
				"MATCH (n) WHERE [0,1][0] = 0 SET n.changed = true",
				"MATCH (n) SET n.value = [1,2][-1]",
				"CREATE (n {value: [1,2][99]})",
				"MATCH (n) WHERE [0,1][0] = 0 DELETE n",
			} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), source, map[string]any{"values": []int64{1, 2}, "idx": int64(0)}, 7)
						require.ErrorContains(t, err, "list indexing is not supported")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}
