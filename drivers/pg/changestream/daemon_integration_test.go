package changestream_test

import (
	"context"
	"testing"
	"time"

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

func setupIntegrationTest(t *testing.T, enableChangelog bool) (*changestream.Changelog, context.Context, graph.Database, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pgxPool, err := pgxpool.New(ctx, pg_connection_string)
	require.NoError(t, err)

	g, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: pg_connection_string,
		Pool:             pgxPool,
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
	schemaManager := pg.NewSchemaManager(pgxPool)
	err = schemaManager.AssertSchema(ctx, graphSchema)
	require.NoError(t, err)

	_, err = pgxPool.Exec(ctx, changestream.ASSERT_NODE_CS_TABLE_SQL)
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
	daemon := changestream.NewChangelogDaemon(ctx, flag, pgxPool, schemaManager, 1)

	return daemon, ctx, g, func() {
		_, err := pgxPool.Exec(ctx, "TRUNCATE node_change_stream")
		if err != nil {
			t.Logf("warning: node cleanup failed: %v", err)
		}
		_, err = pgxPool.Exec(ctx, "TRUNCATE edge_change_stream")
		if err != nil {
			t.Logf("warning: edge cleanup failed: %v", err)
		}
		pgxPool.Close()
		cancel()
	}
}

func TestChangelog(t *testing.T) {
	t.Run("feature flag is off. always submit change. ", func(t *testing.T) {
		changelog, ctx, g, teardown := setupIntegrationTest(t, false)
		defer teardown()

		change := graph.NewNodeChange(
			graph.ChangeTypeModified,
			"abc",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("foo", "bar"),
		)

		changeStatus, err := changelog.ResolveChangeStatus(ctx, change)
		require.NoError(t, err)
		require.True(t, changeStatus.Changed)
		require.False(t, changeStatus.Exists)

		// is this weird?
		if changeStatus.ShouldSubmit() {
			g.BatchOperation(ctx, func(batch graph.Batch) error {
				err := changelog.Submit(ctx, change, batch)
				require.NoError(t, err)

				return nil
			})
		} else {
			require.Fail(t, "ShouldSubmit() should always be true when feature flag is false.")
		}

		time.Sleep(time.Second)

		var (
			nodeID     string
			changeType int
			kindIDs    []int16
			hash       []byte
		)

		row := changelog.Writer.PGX.QueryRow(ctx, `
			SELECT node_id, change_type, kind_ids, hash
			FROM node_change_stream
			WHERE node_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, change.NodeID)

		err = row.Scan(&nodeID, &changeType, &kindIDs, &hash)
		require.NoError(t, err)
		require.Equal(t, change.NodeID, nodeID)
		require.Equal(t, change.ChangeType, graph.ChangeType(changeType))
		require.Equal(t, changeStatus.PropertiesHash, hash)

		kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind1"))
		require.NoError(t, err)
		require.Contains(t, kindIDs, kindID)
	})
	// t.Run("node unvisited. submit the change.", func(t *testing.T) {
	// 	changelog, ctx, teardown := setupIntegrationTest(t, true)
	// 	defer teardown()

	// 	change := graph.NewNodeChange(
	// 		graph.ChangeTypeModified,
	// 		"abc",
	// 		graph.StringsToKinds([]string{"NodeKind1"}),
	// 		graph.NewProperties().Set("foo", "bar"),
	// 	)

	// 	changeStatus, err := changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.False(t, changeStatus.Exists)

	// 	// is this weird?
	// 	if changeStatus.ShouldSubmit() {
	// 		ok := changelog.Submit(ctx, change)
	// 		require.True(t, ok)
	// 	}

	// 	// darn. but how else?
	// 	time.Sleep(time.Second)

	// 	var (
	// 		nodeID     string
	// 		changeType int
	// 		kindIDs    []int16
	// 		hash       []byte
	// 	)

	// 	row := changelog.Writer.PGX.QueryRow(ctx, `
	// 		SELECT node_id, change_type, kind_ids, hash
	// 		FROM node_change_stream
	// 		WHERE node_id = $1
	// 		ORDER BY created_at DESC
	// 		LIMIT 1
	// 	`, change.NodeID)

	// 	err = row.Scan(&nodeID, &changeType, &kindIDs, &hash)
	// 	require.NoError(t, err)
	// 	require.Equal(t, change.NodeID, nodeID)
	// 	require.Equal(t, change.ChangeType, changestream.ChangeType(changeType))
	// 	require.Equal(t, changeStatus.PropertiesHash, hash)

	// 	kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind1"))
	// 	require.NoError(t, err)
	// 	require.Contains(t, kindIDs, kindID)
	// })

	// t.Run("node visited. unchanged. skip submission.", func(t *testing.T) {
	// 	changelog, ctx, teardown := setupIntegrationTest(t, true)
	// 	defer teardown()

	// 	change := changestream.NewNodeChange(
	// 		changestream.ChangeTypeModified,
	// 		"abc",
	// 		graph.StringsToKinds([]string{"NodeKind1"}),
	// 		graph.NewProperties().Set("foo", "bar"),
	// 	)

	// 	// simulate the first write
	// 	changeStatus, err := changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.False(t, changeStatus.Exists)

	// 	// now queue up the actual scenario
	// 	changeStatus, err = changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.False(t, changeStatus.Changed)
	// 	require.True(t, changeStatus.Exists)

	// 	// is this weird?
	// 	if changeStatus.ShouldSubmit() {
	// 		// ok := changelog.Submit(ctx, change)
	// 		require.Fail(t, "the same change was submitted. ShouldSubmit() should be false")
	// 	}
	// })

	// t.Run("node visited. properties changed. submit the change.", func(t *testing.T) {
	// 	changelog, ctx, teardown := setupIntegrationTest(t, true)
	// 	defer teardown()

	// 	change := changestream.NewNodeChange(
	// 		changestream.ChangeTypeModified,
	// 		"abc",
	// 		graph.StringsToKinds([]string{"NodeKind1"}),
	// 		graph.NewProperties().Set("foo", "bar"),
	// 	)

	// 	changeStatus, err := changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.False(t, changeStatus.Exists)

	// 	// simulate a property change
	// 	change.Properties = graph.NewProperties().SetAll(map[string]any{"foo": "a", "bar": "b"})
	// 	changeStatus, err = changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.True(t, changeStatus.Exists)

	// 	// is this weird?
	// 	if changeStatus.ShouldSubmit() {
	// 		ok := changelog.Submit(ctx, change)
	// 		require.True(t, ok)
	// 	}

	// 	// darn. but how else?
	// 	time.Sleep(time.Second)

	// 	var (
	// 		nodeID     string
	// 		changeType int
	// 		kindIDs    []int16
	// 		hash       []byte
	// 	)

	// 	row := changelog.Writer.PGX.QueryRow(ctx, `
	// 		SELECT node_id, change_type, kind_ids, hash
	// 		FROM node_change_stream
	// 		WHERE node_id = $1
	// 		ORDER BY created_at DESC
	// 		LIMIT 1
	// 	`, change.NodeID)

	// 	err = row.Scan(&nodeID, &changeType, &kindIDs, &hash)
	// 	require.NoError(t, err)
	// 	require.Equal(t, change.NodeID, nodeID)
	// 	require.Equal(t, change.ChangeType, changestream.ChangeType(changeType))
	// 	require.Equal(t, changeStatus.PropertiesHash, hash)

	// 	kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind1"))
	// 	require.NoError(t, err)
	// 	require.Contains(t, kindIDs, kindID)
	// })

	// // TODO: wire up kind changes
	// t.Run("node visited. kinds changed. submit the change.", func(t *testing.T) {
	// 	changelog, ctx, teardown := setupIntegrationTest(t, true)
	// 	defer teardown()

	// 	change := changestream.NewNodeChange(
	// 		changestream.ChangeTypeModified,
	// 		"abc",
	// 		graph.StringsToKinds([]string{"NodeKind1"}),
	// 		graph.NewProperties().Set("foo", "bar"),
	// 	)

	// 	changeStatus, err := changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.False(t, changeStatus.Exists)

	// 	// simulate a kind change
	// 	change.Kinds = graph.StringsToKinds([]string{"NodeKind2"})
	// 	changeStatus, err = changelog.ResolveChangeStatus(ctx, change)
	// 	require.NoError(t, err)
	// 	require.True(t, changeStatus.Changed)
	// 	require.True(t, changeStatus.Exists)

	// 	// is this weird?
	// 	if changeStatus.ShouldSubmit() {
	// 		ok := changelog.Submit(ctx, change)
	// 		require.True(t, ok)
	// 	}

	// 	// darn. but how else?
	// 	time.Sleep(time.Second)

	// 	var (
	// 		nodeID     string
	// 		changeType int
	// 		kindIDs    []int16
	// 		hash       []byte
	// 	)

	// 	row := changelog.Writer.PGX.QueryRow(ctx, `
	// 		SELECT node_id, change_type, kind_ids, hash
	// 		FROM node_change_stream
	// 		WHERE node_id = $1
	// 		ORDER BY created_at DESC
	// 		LIMIT 1
	// 	`, change.NodeID)

	// 	err = row.Scan(&nodeID, &changeType, &kindIDs, &hash)
	// 	require.NoError(t, err)
	// 	require.Equal(t, change.NodeID, nodeID)
	// 	require.Equal(t, change.ChangeType, changestream.ChangeType(changeType))
	// 	require.Equal(t, changeStatus.PropertiesHash, hash)

	// 	kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind2"))
	// 	require.NoError(t, err)
	// 	require.Contains(t, kindIDs, kindID)
	// })
}
