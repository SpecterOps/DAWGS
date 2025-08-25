package changelog_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/changelog"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const pg_connection_string = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"

func setupIntegrationTest(t *testing.T) (*changelog.Changelog, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pool, err := pgxpool.New(ctx, pg_connection_string)
	require.NoError(t, err)

	dawgsDB, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: pg_connection_string,
		Pool:             pool,
	})
	require.NoError(t, err)

	// initialize a graph with bare minimum kinds... parent bloodhound app has
	// a defaultGraphSchema defined in the graphschema package. we don't have that nicety available to us here.
	graphSchema := graph.Schema{
		Graphs: []graph.Graph{{
			Name: "test",
			Nodes: graph.Kinds{
				graph.StringKind("NodeKind1"),
				graph.StringKind("NodeKind2"),
			},
			Edges: graph.Kinds{
				graph.StringKind("EdgeKind1"),
				graph.StringKind("EdgeKind2"),
			},
		}},
		DefaultGraph: graph.Graph{
			Name: "test",
		},
	}
	err = dawgsDB.AssertSchema(ctx, graphSchema)
	require.NoError(t, err)

	kindMapper, err := pg.KindMapperFromGraphDatabase(dawgsDB)
	require.NoError(t, err)

	// set batch_size to 1 so that we can test flushing logic
	daemon := changelog.NewChangelog(pool, kindMapper, 1)
	daemon.Start(ctx)
	require.NoError(t, err)

	return daemon, ctx, func() {
		_, err := pool.Exec(ctx, "TRUNCATE node_change_stream")
		if err != nil {
			t.Logf("warning: node cleanup failed: %v", err)
		}
		_, err = pool.Exec(ctx, "TRUNCATE edge_change_stream")
		if err != nil {
			t.Logf("warning: edge cleanup failed: %v", err)
		}
		pool.Close()
		cancel()
	}
}

func TestResolveNodeChangeStatus(t *testing.T) {
	t.Run("resolve change for unvisited node", func(t *testing.T) {
		log, ctx, teardown := setupIntegrationTest(t)
		defer teardown()

		proposedChange := changelog.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("a", 1),
		)

		shouldSubmit, err := log.ResolveChange(ctx, proposedChange)
		require.NoError(t, err)
		require.True(t, shouldSubmit)
	})
}
