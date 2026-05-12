package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubDatabase is a no-op Database implementation suitable for exercising
// behavior on *graph.DatabaseSwitch that does not depend on real driver
// semantics. Methods return zero values; tests that need richer behavior
// should embed this and override only what they need.
type stubDatabase struct{}

func (stubDatabase) SetWriteFlushSize(int) {}
func (stubDatabase) SetBatchWriteSize(int) {}
func (stubDatabase) ReadTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}
func (stubDatabase) WriteTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}
func (stubDatabase) BatchOperation(context.Context, graph.BatchDelegate, ...graph.BatchOption) error {
	return nil
}
func (stubDatabase) AssertSchema(context.Context, graph.Schema) error   { return nil }
func (stubDatabase) SetDefaultGraph(context.Context, graph.Graph) error { return nil }
func (stubDatabase) Run(context.Context, string, map[string]any) error  { return nil }
func (stubDatabase) Close(context.Context) error                        { return nil }
func (stubDatabase) FetchKinds(context.Context) (graph.Kinds, error)    { return nil, nil }
func (stubDatabase) RefreshKinds(context.Context) error                 { return nil }

// optimizingStubDatabase is a stubDatabase that additionally satisfies
// graph.Optimizer. Each call to Optimize increments calls and returns err.
type optimizingStubDatabase struct {
	stubDatabase
	calls int
	err   error
}

func (s *optimizingStubDatabase) Optimize(context.Context) error {
	s.calls++
	return s.err
}

// TestDatabaseSwitch_Optimize_DelegatesWhenUnderlyingImplementsOptimizer
// verifies that *graph.DatabaseSwitch forwards Optimize to the active driver
// when that driver implements graph.Optimizer, and propagates its return.
func TestDatabaseSwitch_Optimize_DelegatesWhenUnderlyingImplementsOptimizer(t *testing.T) {
	ctx := context.Background()

	driver := &optimizingStubDatabase{}
	dbSwitch := graph.NewDatabaseSwitch(ctx, driver)

	require.NoError(t, dbSwitch.Optimize(ctx))
	assert.Equal(t, 1, driver.calls, "Optimize should be invoked exactly once on the underlying driver")

	driver.err = errors.New("optimizer reported failure")
	err := dbSwitch.Optimize(ctx)
	assert.ErrorIs(t, err, driver.err, "DatabaseSwitch must propagate the underlying optimizer error")
	assert.Equal(t, 2, driver.calls)
}

// TestDatabaseSwitch_Optimize_NoOpWhenUnderlyingDoesNotImplementOptimizer
// verifies that the wrapper returns nil without panicking when the active
// driver lacks an Optimize method.
func TestDatabaseSwitch_Optimize_NoOpWhenUnderlyingDoesNotImplementOptimizer(t *testing.T) {
	ctx := context.Background()
	dbSwitch := graph.NewDatabaseSwitch(ctx, stubDatabase{})

	require.NoError(t, dbSwitch.Optimize(ctx))
}

// TestDatabaseSwitch_Optimize_FollowsActiveDriverAfterSwitch verifies that
// after Switch reassigns the active driver, Optimize routes to the new one.
func TestDatabaseSwitch_Optimize_FollowsActiveDriverAfterSwitch(t *testing.T) {
	ctx := context.Background()

	first := &optimizingStubDatabase{}
	second := &optimizingStubDatabase{}

	dbSwitch := graph.NewDatabaseSwitch(ctx, first)
	require.NoError(t, dbSwitch.Optimize(ctx))
	assert.Equal(t, 1, first.calls)
	assert.Equal(t, 0, second.calls)

	dbSwitch.Switch(second)
	require.NoError(t, dbSwitch.Optimize(ctx))
	assert.Equal(t, 1, first.calls, "first driver should not be invoked after Switch")
	assert.Equal(t, 1, second.calls, "Optimize should be routed to the new active driver")
}

// Compile-time assertion that *graph.DatabaseSwitch satisfies graph.Optimizer.
// This keeps the wrapper's optional-capability contract enforced by the type
// system rather than relying on test discovery alone.
var _ graph.Optimizer = (*graph.DatabaseSwitch)(nil)
