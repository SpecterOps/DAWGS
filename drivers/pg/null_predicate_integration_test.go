//go:build manual_integration

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Literal null predicates panicked through the translation cache's re-panic
// before repair; the driver must now execute them and return the correct
// boolean results.
func TestPostgreSQLLiteralNullPredicateProjections(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	for _, tc := range []struct {
		query  string
		expect bool
	}{
		{`RETURN null IS NULL AS value`, true},
		{`RETURN null IS NOT NULL AS value`, false},
		{`RETURN 1 IS NULL AS value`, false},
		{`RETURN 'a' IS NOT NULL AS value`, true},
		{`WITH null AS n RETURN n IS NULL AS value`, true},
		{`UNWIND [1] AS x RETURN null IS NOT NULL AS value`, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
				result := tx.Query(tc.query, nil)
				defer result.Close()

				require.True(t, result.Next(), "%v", result.Error())

				var value bool
				require.NoError(t, result.Scan(&value))
				require.Equal(t, tc.expect, value)
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}

// A literal null predicate in WHERE must filter rows: `1 IS NULL` previously
// had its constraint silently dropped, returning every matching node instead
// of none.
func TestPostgreSQLWhereLiteralNullPredicatesFilterRows(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (n:PGV2IntegrationNode) WHERE 1 IS NULL RETURN n`, nil)
		defer result.Close()

		require.False(t, result.Next())
		return result.Error()
	}))

	for _, tc := range []struct {
		query  string
		expect int64
	}{
		{`MATCH (n:PGV2IntegrationNode) WHERE null IS NULL RETURN count(n)`, 2},
		{`MATCH (n:PGV2IntegrationNode) WHERE null IS NOT NULL RETURN count(n)`, 0},
		{`MATCH (n:PGV2IntegrationNode) WHERE n IS NOT NULL RETURN count(n)`, 2},
		{`MATCH (n:PGV2IntegrationNode) WHERE n IS NULL RETURN count(n)`, 0},
		{`MATCH (n:PGV2IntegrationNode) WHERE n.name IS NOT NULL AND null IS NULL RETURN count(n)`, 2},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
				result := tx.Query(tc.query, nil)
				defer result.Close()

				require.True(t, result.Next(), "%v", result.Error())

				var count int64
				require.NoError(t, result.Scan(&count))
				require.Equal(t, tc.expect, count)
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}
