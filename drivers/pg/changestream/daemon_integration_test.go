package changestream_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg/changestream"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type flagEnabled struct{}

func (s *flagEnabled) GetFlagByKey(ctx context.Context, flag string) (bool, error) {
	return true, nil
}

type flagDisabled struct{}

func (s *flagDisabled) GetFlagByKey(ctx context.Context, flag string) (bool, error) {
	return false, nil
}

func setupIntegrationTest(t *testing.T, enableChangelog bool) (*changestream.Daemon, context.Context, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	pgxPool, err := pgxpool.New(ctx, "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432")
	require.NoError(t, err)

	_, err = pgxPool.Exec(ctx, changestream.ASSERT_TABLE_SQL)
	require.NoError(t, err)

	err = changestream.AssertChangelogPartition(ctx, pgxPool)
	require.NoError(t, err)

	var flag changestream.GetFlagByKeyer
	if enableChangelog {
		flag = &flagEnabled{}
	} else {
		flag = &flagDisabled{}
	}

	daemon := changestream.NewDaemon(ctx, flag, pgxPool)
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
