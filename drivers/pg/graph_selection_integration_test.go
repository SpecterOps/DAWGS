//go:build manual_integration

package pg

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestGraphSelectionPreservesSchema(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	definition := graph.Graph{Name: "dawgs063_selection", NodeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}}, NodeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}}, EdgeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}}, EdgeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}}}
	require.NoError(t, driver.AssertSchema(ctx, graph.Schema{DefaultGraph: definition}))
	fresh := NewSchemaManager(driver.pool, 0)
	t.Cleanup(func() { fresh.parseCache.Close(); fresh.compilationCache.Close() })
	readDefinition := func() int {
		count := 0
		require.NoError(t, fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
			model, err := query.On(tx).SelectGraphByName(definition.Name)
			if err != nil {
				return err
			}
			count = len(model.Partitions.Node.Indexes) + len(model.Partitions.Node.Constraints) + len(model.Partitions.Edge.Indexes) + len(model.Partitions.Edge.Constraints)
			return nil
		}))
		return count
	}
	before := readDefinition()
	require.GreaterOrEqual(t, before, 4)
	require.NoError(t, fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.WithGraph(graph.Graph{Name: definition.Name}).Nodes().Count()
		require.Zero(t, count)
		return err
	}))
	require.Equal(t, before, readDefinition())
	require.ErrorIs(t, fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.WithGraph(graph.Graph{Name: "dawgs063_missing"}).Nodes().Count()
		return err
	}), pgx.ErrNoRows)
	// Explicit assertion still reconciles, even after ordinary graph selection.
	require.NoError(t, fresh.AssertDefaultGraph(ctx, graph.Graph{Name: definition.Name}))
	require.Equal(t, before-4, readDefinition())
	// Explicit assertion reconciles a cached graph too.
	require.NoError(t, fresh.AssertDefaultGraph(ctx, definition))
	require.Equal(t, before, readDefinition())
}
