//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLStandaloneCallFailsBeforeExecution(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			for _, source := range []string{
				"CALL db.labels()",
				"CALL db.labels",
				"CALL db.labels() YIELD *",
				"CALL example.write($value)",
				"/* before */\nCALL example.write({value: 1});",
			} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
							result := tx.Query(source, map[string]any{"value": int64(1)})
							defer result.Close()
							require.False(t, result.Next())
							return result.Error()
						})
						require.ErrorContains(t, err, "standalone CALL is not supported")
					}
					// Independent transactions verify the graph is unchanged after rejection.
					require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
						result := tx.Query("MATCH (n:PGV2IntegrationNode) RETURN n.name ORDER BY n.name", nil)
						defer result.Close()
						var names []string
						for result.Next() {
							var name string
							if err := result.Scan(&name); err != nil {
								return err
							}
							names = append(names, name)
						}
						require.Equal(t, []string{"end", "start"}, names)
						return result.Error()
					}))
				})
			}
		})
	}
}
