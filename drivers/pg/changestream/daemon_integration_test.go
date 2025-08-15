package changestream_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/changestream"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const pg_connection_string = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"

type flagEnabled struct{}

func (s *flagEnabled) GetFlagByKey(ctx context.Context, flag string) (bool, error) {
	return true, nil
}

type flagDisabled struct{}

func (s *flagDisabled) GetFlagByKey(ctx context.Context, flag string) (bool, error) {
	return false, nil
}

func setupIntegrationTest(t *testing.T, enableChangelog bool) (*changestream.Changelog, context.Context, func()) {
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

	_, err = pool.Exec(ctx, changestream.ASSERT_NODE_CS_TABLE_SQL)
	require.NoError(t, err)

	// err = changestream.AssertChangelogPartition(ctx, pgxPool)
	// require.NoError(t, err)

	var flag changestream.GetFlagByKeyer
	if enableChangelog {
		flag = &flagEnabled{}
	} else {
		flag = &flagDisabled{}
	}

	// set batch_size to 1 so that we can test flushing logic
	daemon := changestream.NewChangelogDaemon(ctx, flag, pool, kindMapper, 1)

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

func insertChangelogRecord(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	oldChange := changestream.NewNodeChange(
		"123",
		graph.StringsToKinds([]string{"NodeKind1"}),
		graph.NewProperties().Set("a", 1),
	)

	hashbytes, _ := oldChange.Hash()

	// simulate an existing record in the changelog
	_, err := pool.Exec(ctx, `INSERT INTO node_change_stream (node_id,kind_ids,hash,change_type,modified_properties,deleted_properties,created_at)
				VALUES (
					$1,                             
					$2,               
					$3,
					0,                                 
					'{"a": 1}'::jsonb,                 
					ARRAY[]::text[],                 
					now()                             
				);`, oldChange.NodeID, []int{1}, hashbytes)

	require.NoError(t, err)
}

func TestResolveNodeChangeStatus(t *testing.T) {
	t.Run("resolve change for unvisited node", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		proposedChange := changestream.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("a", 1),
		)

		err := changelog.ResolveNodeChange(ctx, proposedChange)
		require.NoError(t, err)
		require.Equal(t, changestream.ChangeTypeAdded, proposedChange.Type())
		require.Contains(t, proposedChange.ModifiedProperties, "a")
		require.Equal(t, 1, proposedChange.ModifiedProperties["a"])
		require.Empty(t, proposedChange.Deleted)
	})

	t.Run("resolve change for visited node with no changes.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		proposedChange := changestream.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("a", 1),
		)

		hashbytes, _ := proposedChange.Hash()

		// simulate an existing record in the changelog
		_, err := changelog.DB.PGX.Exec(ctx, `INSERT INTO node_change_stream (node_id,kind_ids,hash,change_type,modified_properties,deleted_properties,created_at)
				VALUES (
					$1,                             
					$2,               
					$3,
					0,                                 
					'{"a": 1}'::jsonb,                 
					ARRAY[]::text[],                 
					now()                             
				);`, proposedChange.NodeID, []int{1}, hashbytes)
		require.NoError(t, err)

		err = changelog.ResolveNodeChange(ctx, proposedChange)
		require.NoError(t, err)
		require.Equal(t, changestream.ChangeTypeNoChange, proposedChange.Type())
	})

	t.Run("resolve change for visited node with 1 modified property.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		insertChangelogRecord(t, ctx, changelog.DB.PGX)

		newChange := changestream.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().SetAll(map[string]any{"a": float64(1), "b": 2}), // todo: the actual impl should not need to be explicit with numbers here
		)

		err := changelog.ResolveNodeChange(ctx, newChange)
		require.NoError(t, err)
		require.Equal(t, changestream.ChangeTypeModified, newChange.Type())
		require.Len(t, newChange.ModifiedProperties, 1)
		require.Contains(t, newChange.ModifiedProperties, "b")
		require.Equal(t, 2, newChange.ModifiedProperties["b"])
	})

	t.Run("resolve change for visited node with 1 deleted, 1 modified property.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		insertChangelogRecord(t, ctx, changelog.DB.PGX)

		newChange := changestream.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().SetAll(map[string]any{"b": 2}), // todo: the actual impl should not need to be explicit with numbers here
		)

		err := changelog.ResolveNodeChange(ctx, newChange)
		require.NoError(t, err)
		require.Equal(t, changestream.ChangeTypeModified, newChange.Type())
		require.Len(t, newChange.ModifiedProperties, 1)
		require.Contains(t, newChange.ModifiedProperties, "b")
		require.Equal(t, 2, newChange.ModifiedProperties["b"])
		require.Len(t, newChange.Deleted, 1)
		require.Contains(t, newChange.Deleted, "a")
	})

	t.Run("resolve change for visited node with 1 different kind.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		insertChangelogRecord(t, ctx, changelog.DB.PGX)

		newChange := changestream.NewNodeChange(
			"123",
			graph.StringsToKinds([]string{"NodeKind2"}),
			graph.NewProperties().SetAll(map[string]any{"a": float64(1)}),
		)

		err := changelog.ResolveNodeChange(ctx, newChange)
		require.NoError(t, err)
		require.Equal(t, changestream.ChangeTypeModified, newChange.Type())
		require.Empty(t, newChange.ModifiedProperties)
		require.Empty(t, newChange.Deleted)
		require.Equal(t, newChange.Kinds, newChange.Kinds)
	})
}
