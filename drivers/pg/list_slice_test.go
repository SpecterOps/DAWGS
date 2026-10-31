package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTextRejectsListSlicing(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, source := range []string{
				"WITH [1,2] AS xs RETURN xs[0..2] AS x",
				"RETURN $values[$lo..$hi] AS x",
				"MATCH (n) WHERE size([0,1][..1]) = 1 SET n.changed = true",
				"MATCH (n) SET n.value = [1,2][-1..]",
				"CREATE (n {value: [1,2][..99]})",
				"MATCH (n) WHERE size([0,1][..1]) = 1 DELETE n",
			} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), source, map[string]any{"values": []int64{1, 2}, "lo": int64(0), "hi": int64(1)}, 7)
						require.ErrorContains(t, err, "list slicing is not supported")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}
