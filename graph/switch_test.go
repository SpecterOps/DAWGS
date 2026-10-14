package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// optimizeStorageStubDatabase records OptimizeStorage calls. Unimplemented graph.Database methods panic via the nil
// embedded interface.
type optimizeStorageStubDatabase struct {
	graph.Database

	calls       int
	err         error
	receivedCtx context.Context
}

func (s *optimizeStorageStubDatabase) OptimizeStorage(ctx context.Context) error {
	s.calls++
	s.receivedCtx = ctx
	return s.err
}

// blockingOptimizeStorageStubDatabase blocks in OptimizeStorage until its context is cancelled.
type blockingOptimizeStorageStubDatabase struct {
	graph.Database

	started chan struct{}
}

func (s *blockingOptimizeStorageStubDatabase) OptimizeStorage(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestDatabaseSwitchOptimizeStorage(t *testing.T) {
	t.Run("delegates to the active database and propagates its error", func(t *testing.T) {
		ctx := context.Background()
		db := &optimizeStorageStubDatabase{}
		dbSwitch := graph.NewDatabaseSwitch(ctx, db)

		require.NoError(t, dbSwitch.OptimizeStorage(ctx))
		require.Equal(t, 1, db.calls)

		db.err = errors.New("optimize storage failed")
		require.ErrorIs(t, dbSwitch.OptimizeStorage(ctx), db.err)
		require.Equal(t, 2, db.calls)
	})

	t.Run("passes a switch-managed context that is retired on return", func(t *testing.T) {
		ctx := context.Background()
		db := &optimizeStorageStubDatabase{}
		dbSwitch := graph.NewDatabaseSwitch(ctx, db)

		require.NoError(t, dbSwitch.OptimizeStorage(ctx))
		require.NotNil(t, db.receivedCtx)
		require.NotEqual(t, ctx, db.receivedCtx)
		require.ErrorIs(t, db.receivedCtx.Err(), context.Canceled)
		require.NoError(t, ctx.Err())
	})

	t.Run("routes to the new active database after a switch", func(t *testing.T) {
		ctx := context.Background()
		first := &optimizeStorageStubDatabase{}
		second := &optimizeStorageStubDatabase{}
		dbSwitch := graph.NewDatabaseSwitch(ctx, first)

		require.NoError(t, dbSwitch.OptimizeStorage(ctx))
		dbSwitch.Switch(second)
		require.NoError(t, dbSwitch.OptimizeStorage(ctx))

		require.Equal(t, 1, first.calls)
		require.Equal(t, 1, second.calls)
	})

	t.Run("switch cancels an in-flight call", func(t *testing.T) {
		ctx := context.Background()
		db := &blockingOptimizeStorageStubDatabase{started: make(chan struct{})}
		next := &optimizeStorageStubDatabase{}
		dbSwitch := graph.NewDatabaseSwitch(ctx, db)

		optimizeErr := make(chan error, 1)
		go func() {
			optimizeErr <- dbSwitch.OptimizeStorage(ctx)
		}()

		select {
		case <-db.started:
		case <-time.After(time.Second):
			t.Fatal("OptimizeStorage did not start within timeout")
		}

		switched := make(chan struct{})
		go func() {
			dbSwitch.Switch(next)
			close(switched)
		}()

		select {
		case err := <-optimizeErr:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("OptimizeStorage did not return after Switch cancelled it")
		}

		select {
		case <-switched:
		case <-time.After(time.Second):
			t.Fatal("Switch did not complete after the in-flight call returned")
		}

		require.NoError(t, dbSwitch.OptimizeStorage(ctx))
		require.Equal(t, 1, next.calls)
	})
}
