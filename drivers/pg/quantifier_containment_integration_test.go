//go:build manual_integration

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Pattern-less scalar-list quantifier projections crashed the compiler through the
// translation cache's re-panic before containment; the driver must now surface the
// translation error through the query result.
func TestPostgreSQLQuantifierWithoutPatternReturnsTranslationError(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	for _, quantifier := range []string{"all", "any", "none", "single"} {
		t.Run(quantifier, func(t *testing.T) {
			require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
				result := tx.Query(`RETURN `+quantifier+`(x IN [1] WHERE x = 1) AS value`, nil)
				defer result.Close()
				require.False(t, result.Next())
				require.Error(t, result.Error())
				return nil
			}))
		})
	}
}

func TestPostgreSQLScalarListQuantifierInWhereExecutes(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (n:PGV2IntegrationNode) WHERE all(x IN [1, 2] WHERE x < 3) RETURN count(n)`, nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		var count int64
		require.NoError(t, result.Scan(&count))
		require.Equal(t, int64(2), count)
		require.False(t, result.Next())
		return result.Error()
	}))
}
