//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLUnsupportedImportAndCreateFailBeforeExecution(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			for _, tc := range unsupportedImportCreateCases() {
				t.Run(tc.source, func(t *testing.T) {
					for range 2 {
						require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
							result := tx.Query(tc.source, map[string]any{"url": "file:///missing.csv"})
							defer result.Close()
							require.False(t, result.Next())
							require.ErrorContains(t, result.Error(), tc.message)
							// Commit even when the caller consumes the compilation error:
							// preceding writes must never have been dispatched.
							return nil
						}))
					}
					require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
						result := tx.Query("MATCH (n) RETURN n.name ORDER BY n.name", nil)
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
					require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
						result := tx.Query("MATCH ()-[r]->() RETURN r.name", nil)
						defer result.Close()
						require.True(t, result.Next(), "%v", result.Error())
						require.Equal(t, []any{"edge"}, result.Values())
						require.False(t, result.Next())
						return result.Error()
					}))
				})
			}
		})
	}
}
