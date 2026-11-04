//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLListSlicingFailsBeforeExecution(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)
	ctx := context.Background()
	for _, optimized := range []bool{true, false} {
		t.Run(fmt.Sprint(optimized), func(t *testing.T) {
			setOptimizedTranslationForTest(t, optimized)
			for _, source := range []string{
				"WITH [1,2] AS xs RETURN xs[0..2] AS x",
				"RETURN $values[$lo..$hi] AS x",
				"MATCH (n:PGV2IntegrationNode) SET n.name = ['changed'][..1]",
				"CREATE (n:PGV2IntegrationNode {name: ['extra'][0..]})",
				"MATCH (n:PGV2IntegrationNode) WHERE size([0,1][..1]) = 1 DETACH DELETE n",
			} {
				t.Run(source, func(t *testing.T) {
					for range 2 {
						err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
							result := tx.Query(source, map[string]any{"values": []int64{1, 2}, "lo": int64(0), "hi": int64(1)})
							defer result.Close()
							require.False(t, result.Next())
							return result.Error()
						})
						require.ErrorContains(t, err, "list slicing is not supported")
					}
					// Independent transactions verify that rejected mutations committed no
					// changes, including a SET that would previously rename both nodes.
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
