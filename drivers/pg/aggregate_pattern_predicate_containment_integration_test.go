//go:build manual_integration

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// The aggregate traversal count lowering panicked on MATCH WHERE pattern
// predicates when optimized translation was enabled; the driver must now
// surface a normal translation error through the query result, including on
// repeated requests through the translation cache.
func TestPostgreSQLAggregateTraversalCountPatternPredicateReturnsTranslationError(t *testing.T) {
	setOptimizedTranslationForTest(t, true)

	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	for _, trigger := range []string{
		`MATCH (s:PGV2IntegrationNode) WHERE (s)-[:PGV2IntegrationEdge]->() MATCH (s)-[:PGV2IntegrationEdge*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		`MATCH (s:PGV2IntegrationNode) MATCH (s)-[:PGV2IntegrationEdge*1..3]->(t) WHERE (t)-[:PGV2IntegrationEdge]->() WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
	} {
		for attempt := 0; attempt < 2; attempt++ {
			require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
				result := tx.Query(trigger, nil)
				defer result.Close()

				require.False(t, result.Next())
				require.Error(t, result.Error())
				require.Contains(t, result.Error().Error(), "unsupported aggregate traversal count predicate: pattern predicates in MATCH WHERE are not supported")
				return nil
			}), "trigger attempt %d", attempt)
		}
	}
}

// The aggregate traversal count lowering must keep executing the shapes it
// translates correctly; the fixture's single start -> end edge yields one
// one-hop terminal for the start node.
func TestPostgreSQLAggregateTraversalCountExecutes(t *testing.T) {
	setOptimizedTranslationForTest(t, true)

	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (s:PGV2IntegrationNode) MATCH (s)-[:PGV2IntegrationEdge*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 10`, nil)
		defer result.Close()

		require.True(t, result.Next(), "%v", result.Error())

		var (
			node  graph.Node
			count int64
		)
		require.NoError(t, result.Scan(&node, &count))
		require.Equal(t, "start", node.Properties.Map["name"])
		require.Equal(t, int64(1), count)
		require.False(t, result.Next())
		return result.Error()
	}))
}
