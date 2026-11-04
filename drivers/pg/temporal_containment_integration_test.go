//go:build manual_integration

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Temporal constructions with an argument panicked through the compiler before
// containment; the driver must now surface an explicit translation error for
// their unsupported component lookups, both on the first compilation and on a
// repeated request.
func TestPostgreSQLTemporalComponentsReturnTranslationError(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	for _, tc := range []struct{ name, query, message string }{
		{"datetime literal epochmillis", `RETURN datetime('2020-01-01').epochmillis AS v`, "unsupported temporal type component epochmillis on cast type timestamp with time zone"},
		{"date property year", `MATCH (n:PGV2IntegrationNode) RETURN date(n.name).year AS v`, "unsupported temporal type component year on cast type date"},
		{"where datetime comparison", `MATCH (n:PGV2IntegrationNode) WHERE datetime('2020-01-01').epochmillis > 0 RETURN n`, "unsupported temporal type component epochmillis on cast type timestamp with time zone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for attempt := range 2 {
				require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
					result := tx.Query(tc.query, nil)
					defer result.Close()

					require.False(t, result.Next(), "attempt %d must not return rows", attempt)
					require.ErrorContains(t, result.Error(), tc.message)
					return nil
				}), "attempt %d", attempt)
			}
		})
	}
}

// Supported temporal lowering keeps executing: current-instant components
// produce values and temporal comparisons filter rows.
func TestPostgreSQLTemporalControlsExecute(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 4, nil)
	setUpV2IntegrationGraph(t, driver)

	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`RETURN datetime().epochseconds > 0 AS v`, nil)
		defer result.Close()

		require.True(t, result.Next(), "%v", result.Error())

		var positive bool
		require.NoError(t, result.Scan(&positive))
		require.True(t, positive)
		require.False(t, result.Next())
		return result.Error()
	}))

	// The current-instant component participates in a WHERE predicate over the
	// fixture's start node.
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		result := tx.Query(`MATCH (n:PGV2IntegrationNode) WHERE n.name = 'start' AND datetime().epochseconds > 0 RETURN count(n)`, nil)
		defer result.Close()

		require.True(t, result.Next(), "%v", result.Error())

		var count int64
		require.NoError(t, result.Scan(&count))
		require.Equal(t, int64(1), count)
		require.False(t, result.Next())
		return result.Error()
	}))
}
