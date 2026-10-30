//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestAssertSchemaCreatesAndReconcilesDeclaredGraphs(t *testing.T) {
	for _, withDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("default=%t", withDefault), func(t *testing.T) {
			driver := newDAWGS065Driver(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			prefix := fmt.Sprintf("dawgs_declared_graph_%d", time.Now().UnixNano())
			first := graph.Graph{Name: prefix + "_first"}
			second := graph.Graph{Name: prefix + "_second", NodeIndexes: []graph.Index{{Field: "name", Type: graph.BTreeIndex}}}
			schema := graph.Schema{Graphs: []graph.Graph{first, second}}
			if withDefault {
				schema.DefaultGraph = first
			}
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				for _, definition := range schema.Graphs {
					var id int32
					err := driver.pool.QueryRow(cleanupCtx, "select id from graph where name=$1", definition.Name).Scan(&id)
					if err == pgx.ErrNoRows {
						continue
					}
					require.NoError(t, err)
					_, err = driver.pool.Exec(cleanupCtx, fmt.Sprintf("drop table if exists node_%d, edge_%d", id, id))
					require.NoError(t, err)
					_, err = driver.pool.Exec(cleanupCtx, "delete from graph where id=$1", id)
					require.NoError(t, err)
				}
			})
			require.NoError(t, driver.AssertSchema(ctx, schema))
			for _, definition := range schema.Graphs {
				require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					_, err := tx.WithGraph(definition).CreateNode(graph.NewProperties())
					return err
				}))
			}
			assertSecondGraphIndexes := func(want int) {
				t.Helper()
				require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					definition, err := query.On(tx).SelectGraphByName(second.Name)
					if err != nil {
						return err
					}
					require.Len(t, definition.Partitions.Node.Indexes, want)
					return nil
				}))
			}
			assertSecondGraphIndexes(1)
			schema.Graphs[1].NodeIndexes = nil
			require.NoError(t, driver.AssertSchema(ctx, schema))
			assertSecondGraphIndexes(0)
		})
	}
}

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
