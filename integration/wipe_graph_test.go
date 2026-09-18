package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// TestWipeGraph verifies the PostgreSQL-only bulk-delete primitive and skips unless CONNECTION_STRING selects that backend.
func TestWipeGraph(t *testing.T) {
	var (
		wipeNode = graph.StringKind("WipeNode")
		survivor = graph.StringKind("WipeSurvivor")
		wipeEdge = graph.StringKind("WIPE_EDGE")

		defaultGraph = graph.Graph{
			Name:  "wipe_default",
			Nodes: graph.Kinds{wipeNode, survivor},
			Edges: graph.Kinds{wipeEdge},
		}
		secondaryGraph = graph.Graph{
			Name:  "wipe_secondary",
			Nodes: graph.Kinds{wipeNode, survivor},
			Edges: graph.Kinds{wipeEdge},
		}

		schema = graph.Schema{
			Graphs:       []graph.Graph{defaultGraph, secondaryGraph},
			DefaultGraph: defaultGraph,
		}
	)

	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		Schema:               &schema,
	})

	var (
		ctx         = session.Ctx
		db          = session.DB
		wiper, isPG = graph.AsDriver[*pg.Driver](db)
	)

	require.True(t, isPG, "expected a Postgres driver")

	// seed populates the default and secondary partitions with two connected nodes and an additional node in the
	// secondary graph so the test exercises a multi-partition truncate.
	seed := func(t *testing.T) {
		require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
			start, err := tx.CreateNode(graph.NewProperties(), wipeNode)
			if err != nil {
				return err
			}

			end, err := tx.CreateNode(graph.NewProperties(), wipeNode)
			if err != nil {
				return err
			}

			if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, wipeEdge, graph.NewProperties()); err != nil {
				return err
			}

			if _, err := tx.WithGraph(secondaryGraph).CreateNode(graph.NewProperties(), wipeNode); err != nil {
				return err
			}

			return nil
		}))
	}

	t.Run("truncates all partitions and atomically retains the recreated node", func(t *testing.T) {
		session.ClearGraph(t)
		seed(t)

		require.Equal(t, int64(3), countNodes(t, ctx, db, defaultGraph, secondaryGraph))
		require.Equal(t, int64(1), countEdges(t, ctx, db))

		require.NoError(t, wiper.WipeGraph(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "kept"}), survivor)
			return err
		}))

		require.Equal(t, int64(1), countNodes(t, ctx, db, defaultGraph, secondaryGraph))
		require.Equal(t, int64(0), countEdges(t, ctx, db))

		require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			node, err := tx.Nodes().Filter(query.Kind(query.Node(), survivor)).First()
			if err != nil {
				return err
			}

			require.True(t, node.Kinds.ContainsOneOf(survivor))

			name, err := node.Properties.Get("name").String()
			require.NoError(t, err)
			require.Equal(t, "kept", name)

			return nil
		}))
	})

	t.Run("rolls back the truncate when the retain delegate fails", func(t *testing.T) {
		session.ClearGraph(t)
		seed(t)

		errRetain := errors.New("retain failed")

		err := wiper.WipeGraph(ctx, func(tx graph.Transaction) error {
			return errRetain
		})

		require.ErrorIs(t, err, errRetain)

		// The transaction rolled back, so the seeded graph is left untouched.
		require.Equal(t, int64(3), countNodes(t, ctx, db, defaultGraph, secondaryGraph))
		require.Equal(t, int64(1), countEdges(t, ctx, db))
	})

	t.Run("truncates without a retain delegate", func(t *testing.T) {
		session.ClearGraph(t)
		seed(t)

		require.NoError(t, wiper.WipeGraph(ctx, nil))

		require.Equal(t, int64(0), countNodes(t, ctx, db, defaultGraph, secondaryGraph))
		require.Equal(t, int64(0), countEdges(t, ctx, db))
	})
}

// countNodes returns the total node count across graphs.
func countNodes(t *testing.T, ctx context.Context, db graph.Database, graphs ...graph.Graph) int64 {
	t.Helper()

	var count int64

	require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		for _, targetGraph := range graphs {
			result, err := tx.WithGraph(targetGraph).Nodes().Count()
			if err != nil {
				return err
			}
			count += result
		}
		return nil
	}))

	return count
}

// countEdges returns the relationship count in the database's current graph.
func countEdges(t *testing.T, ctx context.Context, db graph.Database) int64 {
	t.Helper()

	var count int64

	require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result, err := tx.Relationships().Count()
		count = result
		return err
	}))

	return count
}
