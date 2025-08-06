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

// TODO: use pgtestdb
func setupIntegrationTest(t *testing.T, enableChangelog bool) (*changestream.Daemon, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pgxPool, err := pgxpool.New(ctx, pg_connection_string)
	require.NoError(t, err)

	_, err = dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: pg_connection_string,
		Pool:             pgxPool,
	})
	require.NoError(t, err)

	// kindMapper, err := pg.KindMapperFromGraphDatabase(graphDB)
	// require.NoError(t, err)

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

	_, err = pgxPool.Exec(ctx, changestream.ASSERT_TABLE_SQL)
	require.NoError(t, err)

	// err = changestream.AssertChangelogPartition(ctx, pgxPool)
	// require.NoError(t, err)

	var flag changestream.GetFlagByKeyer
	if enableChangelog {
		flag = &flagEnabled{}
	} else {
		flag = &flagDisabled{}
	}

	daemon := changestream.NewDaemon(ctx, flag, pgxPool, schemaManager)
	daemon.State.CheckFeatureFlag(ctx) // todo: this sets the enabled flag on the state...

	return daemon, ctx, func() {
		_, err := pgxPool.Exec(ctx, "TRUNCATE node_change_stream")
		if err != nil {
			t.Logf("warning: cleanup failed: %v", err)
		}
		pgxPool.Close()
		cancel()
	}
}

func setupIntegrationTestRefactor(t *testing.T, enableChangelog bool) (*changestream.Changelog, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pgxPool, err := pgxpool.New(ctx, pg_connection_string)
	require.NoError(t, err)

	_, err = dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		ConnectionString: pg_connection_string,
		Pool:             pgxPool,
	})
	require.NoError(t, err)

	// kindMapper, err := pg.KindMapperFromGraphDatabase(graphDB)
	// require.NoError(t, err)

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

	_, err = pgxPool.Exec(ctx, changestream.ASSERT_TABLE_SQL)
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

	return daemon, ctx, func() {
		_, err := pgxPool.Exec(ctx, "TRUNCATE node_change_stream")
		if err != nil {
			t.Logf("warning: cleanup failed: %v", err)
		}
		pgxPool.Close()
		cancel()
	}
}
func TestResolveNodeChangeStatus(t *testing.T) {
	t.Run("cache hit, return early", func(t *testing.T) {
		daemon, ctx, teardown := setupIntegrationTest(t, true)
		defer teardown()

		props := graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2})
		hashedProps, _ := props.Hash(nil)
		proposedChange := changestream.NewNodeChange(changestream.ChangeTypeModified, "abc", []graph.Kind{graph.StringKind("kind A")}, props)

		// cache control. simulate a cache hit
		daemon.PutCachedChange(proposedChange.IdentityKey(), changestream.ChangeStatus{
			PropertiesHash: hashedProps,
		})

		status, err := daemon.ResolveNodeChangeStatus(ctx, proposedChange)
		require.NoError(t, err)
		require.False(t, status.Changed)
		require.True(t, status.Exists)
	})

	t.Run("cache miss, changelog disabled", func(t *testing.T) {
		daemon, ctx, teardown := setupIntegrationTest(t, false)
		defer teardown()

		// simulate turning off change log
		// daemon.State.DisableDaemon()

		props := graph.NewProperties().SetAll(map[string]any{"a": 1, "b": 2})
		proposedChange := changestream.NewNodeChange(changestream.ChangeTypeModified, "abc", []graph.Kind{graph.StringKind("kind A")}, props)

		status, err := daemon.ResolveNodeChangeStatus(ctx, proposedChange)
		require.NoError(t, err)
		require.True(t, status.Changed)
		require.False(t, status.Exists)
	})

	t.Run("cache miss, changelog enabled, DB match with same hash", func(t *testing.T) {
		daemon, ctx, teardown := setupIntegrationTest(t, true)
		defer teardown()

		props := graph.NewProperties().SetAll(map[string]any{"foo": "bar", "a": 1})
		hash, _ := props.Hash(nil)
		nodeID := "node-match-same"

		// simulate an existing db record with same property hash
		_, err := daemon.PGX().Exec(ctx, `
			INSERT INTO node_change_stream (node_id, kind_ids, properties_hash, property_fields, change_type, created_at)
			VALUES ($1, '{}', $2, '{}', $3, now())
		`, nodeID, hash, changestream.ChangeTypeModified)
		require.NoError(t, err)

		proposedChange := changestream.NewNodeChange(changestream.ChangeTypeModified, nodeID, graph.Kinds{}, props)

		status, err := daemon.ResolveNodeChangeStatus(ctx, proposedChange)
		require.NoError(t, err)
		require.False(t, status.Changed)
		require.True(t, status.Exists)
	})

	t.Run("cache miss, changelog enabled, DB match with different hash", func(t *testing.T) {
		daemon, ctx, teardown := setupIntegrationTest(t, true)
		defer teardown()

		props := graph.NewProperties().SetAll(map[string]any{"foo": "bar", "a": 1})
		nodeID := "node-match-same"

		existingProps := graph.NewProperties().SetAll(map[string]any{"foo": "bar"})
		existingPropsHash, _ := existingProps.Hash(nil)

		// simulate an existing db record with differenty property hash
		_, err := daemon.PGX().Exec(ctx, `
			INSERT INTO node_change_stream (node_id, kind_ids, properties_hash, property_fields, change_type, created_at)
			VALUES ($1, '{}', $2, '{}', $3, now())
		`, nodeID, existingPropsHash, changestream.ChangeTypeModified)
		require.NoError(t, err)

		proposedChange := changestream.NewNodeChange(changestream.ChangeTypeModified, nodeID, graph.Kinds{}, props)

		status, err := daemon.ResolveNodeChangeStatus(ctx, proposedChange)
		require.NoError(t, err)
		require.True(t, status.Changed)
		require.True(t, status.Exists)
	})

	t.Run("cache miss, changelog enabled, no DB match", func(t *testing.T) {
		daemon, ctx, teardown := setupIntegrationTest(t, true)
		defer teardown()

		props := graph.NewProperties().SetAll(map[string]any{"foo": "bar", "a": 1})
		nodeID := "nothing-matches-me :("

		proposedChange := changestream.NewNodeChange(changestream.ChangeTypeModified, nodeID, graph.Kinds{}, props)

		status, err := daemon.ResolveNodeChangeStatus(ctx, proposedChange)
		require.NoError(t, err)
		require.False(t, status.Changed)
		require.False(t, status.Exists)
	})
}

func TestFlushNodeChanges(t *testing.T) {
	daemon, ctx, teardown := setupIntegrationTest(t, true)
	defer teardown()

	// Prepare one buffered NodeChange
	props := graph.NewProperties().Set("foo", "bar")
	node := &changestream.NodeChange{
		NodeID:     "test-node-123",
		Properties: props,
		Kinds:      graph.Kinds{graph.StringKind("NodeKind1")},
	}
	hash, _ := node.Properties.Hash(nil)
	now := time.Now()

	// queue a single change
	daemon.QueueChange(ctx, now, node)

	// Call flush
	err := daemon.FlushNodeChanges(ctx)
	require.NoError(t, err)

	// Query the DB to verify the row exists
	var (
		nodeID         string
		changeType     int
		kindIDs        []int32
		properties     []string
		propertiesHash []byte
	)

	row := daemon.PGX().QueryRow(ctx, `
		SELECT node_id, change_type, kind_ids, property_fields, properties_hash
		FROM node_change_stream
		WHERE node_id = $1
		ORDER BY created_at DESC
		LIMIT 1
	`, node.NodeID)

	err = row.Scan(&nodeID, &changeType, &kindIDs, &properties, &propertiesHash)
	require.NoError(t, err)

	require.Equal(t, node.NodeID, nodeID)
	require.Equal(t, changestream.ChangeTypeModified, changestream.ChangeType(changeType))
	require.Equal(t, node.Properties.Keys(nil), properties)
	require.Equal(t, hash, propertiesHash)
}

func TestChangelog(t *testing.T) {
	t.Run("node unvisited. submit the change.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTestRefactor(t, true)
		defer teardown()

		change := changestream.NewNodeChange(
			changestream.ChangeTypeModified,
			"abc",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("foo", "bar"),
		)
		hash, _ := change.Properties.Hash(nil) // get the hash for a later assertion

		changeStatus, err := changelog.ResolveNodeChangeStatus(ctx, change)
		require.NoError(t, err)
		require.True(t, changeStatus.Changed)
		require.False(t, changeStatus.Exists)

		// is this weird?
		if changeStatus.ShouldSubmit() {
			ok := changelog.Submit(ctx, change)
			require.True(t, ok)
		}

		// damnit. but how else?
		time.Sleep(time.Second)

		var (
			nodeID         string
			changeType     int
			kindIDs        []int16
			properties     []string
			propertiesHash []byte
		)

		row := changelog.Writer.PGX.QueryRow(ctx, `
			SELECT node_id, change_type, kind_ids, property_fields, properties_hash
			FROM node_change_stream
			WHERE node_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, change.NodeID)

		err = row.Scan(&nodeID, &changeType, &kindIDs, &properties, &propertiesHash)
		require.NoError(t, err)
		require.Equal(t, change.NodeID, nodeID)
		require.Equal(t, change.ChangeType, changestream.ChangeType(changeType))
		require.Equal(t, change.Properties.Keys(nil), properties)
		require.Equal(t, hash, propertiesHash)

		kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind1"))
		require.NoError(t, err)
		require.Contains(t, kindIDs, kindID)
	})

	t.Run("node visited. unchanged. skip submission.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTestRefactor(t, true)
		defer teardown()

		change := changestream.NewNodeChange(
			changestream.ChangeTypeModified,
			"abc",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("foo", "bar"),
		)

		// simulate the first write
		changeStatus, err := changelog.ResolveNodeChangeStatus(ctx, change)
		require.NoError(t, err)
		require.True(t, changeStatus.Changed)
		require.False(t, changeStatus.Exists)

		// now queue up the actual scenario
		changeStatus, err = changelog.ResolveNodeChangeStatus(ctx, change)
		require.NoError(t, err)
		require.False(t, changeStatus.Changed)
		require.True(t, changeStatus.Exists)

		// is this weird?
		if changeStatus.ShouldSubmit() {
			// ok := changelog.Submit(ctx, change)
			require.Fail(t, "the same change was submitted. ShouldSubmit() should be false")
		}
	})

	t.Run("node visited. properties changed. submit the change.", func(t *testing.T) {
		changelog, ctx, teardown := setupIntegrationTestRefactor(t, true)
		defer teardown()

		change := changestream.NewNodeChange(
			changestream.ChangeTypeModified,
			"abc",
			graph.StringsToKinds([]string{"NodeKind1"}),
			graph.NewProperties().Set("foo", "bar"),
		)

		changeStatus, err := changelog.ResolveNodeChangeStatus(ctx, change)
		require.NoError(t, err)
		require.True(t, changeStatus.Changed)
		require.False(t, changeStatus.Exists)

		// simulate a property change
		change.Properties = graph.NewProperties().SetAll(map[string]any{"foo": "a", "bar": "b"})
		hash, _ := change.Properties.Hash(nil) // get the hash for a later assertion
		changeStatus, err = changelog.ResolveNodeChangeStatus(ctx, change)
		require.NoError(t, err)
		require.True(t, changeStatus.Changed)
		require.True(t, changeStatus.Exists)

		// is this weird?
		if changeStatus.ShouldSubmit() {
			ok := changelog.Submit(ctx, change)
			require.True(t, ok)
		}

		// damnit. but how else?
		time.Sleep(time.Second)

		var (
			nodeID         string
			changeType     int
			kindIDs        []int16
			properties     []string
			propertiesHash []byte
		)

		row := changelog.Writer.PGX.QueryRow(ctx, `
			SELECT node_id, change_type, kind_ids, property_fields, properties_hash
			FROM node_change_stream
			WHERE node_id = $1
			ORDER BY created_at DESC
			LIMIT 1
		`, change.NodeID)

		err = row.Scan(&nodeID, &changeType, &kindIDs, &properties, &propertiesHash)
		require.NoError(t, err)
		require.Equal(t, change.NodeID, nodeID)
		require.Equal(t, change.ChangeType, changestream.ChangeType(changeType))
		require.Equal(t, change.Properties.Keys(nil), properties)
		require.Equal(t, hash, propertiesHash)

		kindID, err := changelog.Writer.KindMapper.MapKind(ctx, graph.StringKind("NodeKind1"))
		require.NoError(t, err)
		require.Contains(t, kindIDs, kindID)
	})

	// TODO: wire up kind changes
	t.Run("node visited. kinds changed. submit the change.", func(t *testing.T) {

	})
}
