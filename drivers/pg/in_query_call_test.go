package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func inQueryCallCases() []struct{ source, message string } {
	var cases []struct{ source, message string }
	for _, invocation := range []string{"test.doNothing()", "test.write($value)", "db.labels() YIELD label AS name WHERE name = 'x'"} {
		for _, template := range []string{
			"CALL %s RETURN 1 AS x",
			"MATCH (n) CALL %s RETURN n",
			"WITH 1 AS x CALL %s RETURN x",
			"MATCH (n) CALL %s SET n.name = 'changed' RETURN n",
			"MATCH (n) CALL %s DETACH DELETE n",
			"MATCH (n) CALL %s CREATE (:PGV2IntegrationNode {name: 'rejected'})",
			"CREATE (:PGV2IntegrationNode {name: 'before'}) WITH 1 AS x CALL %s RETURN x",
			"MATCH (n) SET n.name = 'changed' WITH n CALL %s RETURN n",
			"MATCH (n) CALL %s WITH n RETURN n",
		} {
			cases = append(cases, struct{ source, message string }{fmt.Sprintf(template, invocation), "in-query CALL is not supported"})
		}
	}
	return cases
}

func TestCompileTextRejectsInQueryCall(t *testing.T) {
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 4})
			for _, tc := range inQueryCallCases() {
				t.Run(tc.source, func(t *testing.T) {
					for range 2 {
						sql, _, err := manager.compileText(context.Background(), tc.source, map[string]any{"value": int64(1)}, 7)
						require.ErrorContains(t, err, tc.message)
						require.NotContains(t, err.Error(), "panic")
						require.Empty(t, sql)
					}
				})
			}
		})
	}
}
