package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTextRejectsStandaloneCall(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, source := range []string{"CALL db.labels()", "CALL db.labels", "CALL db.labels() YIELD *", "CALL example.write($value)", "/* before */\nCALL example.write({value: 1});"} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), source, map[string]any{"value": int64(1)}, 7)
						require.ErrorContains(t, err, "standalone CALL is not supported")
						require.NotContains(t, err.Error(), "panic")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}
