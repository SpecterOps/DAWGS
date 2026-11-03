//go:build manual_integration

package pg

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestKindCacheExistingGraphAttachmentInitializesDecoding(t *testing.T) {
	driver := newV2IntegrationDriver(t, 1, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	fresh := NewSchemaManager(driver.pool, 0)
	t.Cleanup(func() { fresh.parseCache.Close(); fresh.compilationCache.Close() })
	ctx := context.Background()
	require.NoError(t, fresh.SetDefaultGraph(ctx, v2IntegrationSchema.DefaultGraph))
	require.True(t, fresh.kindCache.current.Load().loaded)
	require.NoError(t, fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.Nodes().First()
		if err != nil {
			return err
		}
		require.Contains(t, node.Kinds, v2IntegrationNodeKind)
		result := tx.Query("MATCH (n) RETURN n LIMIT 1", nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		var decoded graph.Node
		if err := graph.ScanNextResult(result, &decoded); err != nil {
			return err
		}
		require.Contains(t, decoded.Kinds, v2IntegrationNodeKind)
		return result.Error()
	}))
	require.Equal(t, 1, trace.count(), "attachment loads once and entity reads remain warm")
}

func TestKindCacheConcurrentColdWrappersWaitForCatalog(t *testing.T) {
	driver := newV2IntegrationDriver(t, 4, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	trace.entered, trace.release = make(chan struct{}), make(chan struct{})
	fresh := NewSchemaManager(driver.pool, 0)
	t.Cleanup(func() { fresh.parseCache.Close(); fresh.compilationCache.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allowQuery := make(chan struct{})
	var release sync.Once
	releaseAll := func() { release.Do(func() { close(trace.release); close(allowQuery) }) }
	defer releaseAll()
	delegates := make(chan struct{}, 4)
	done := make(chan error, 4)
	read := func() {
		done <- fresh.ReadTransaction(ctx, func(tx graph.Transaction) error {
			delegates <- struct{}{}
			<-allowQuery
			_, err := tx.WithGraph(v2IntegrationSchema.DefaultGraph).Nodes().First()
			return err
		})
	}
	go read()
	select {
	case <-trace.entered:
	case <-ctx.Done():
		t.Fatal("initial catalog load did not start")
	}
	for i := 0; i < 3; i++ {
		go read()
	}
	requireEventually(t, func() bool { return driver.pool.Stat().AcquiredConns() == 4 }, "all cold wrappers to acquire connections")
	require.Empty(t, delegates, "cold wrappers cannot expose incomplete decoder metadata")
	releaseAll()
	for i := 0; i < 4; i++ {
		require.NoError(t, <-done)
	}
	require.Equal(t, 1, trace.count())
	require.False(t, fresh.kindCache.pending())
}

func TestKindCacheColdWrapperCancellationDoesNotWaitForInitializer(t *testing.T) {
	driver := newV2IntegrationDriver(t, 2, 8, nil)
	setUpV2IntegrationGraph(t, driver)
	trace := traceKindCatalog(t, driver)
	trace.entered, trace.release = make(chan struct{}), make(chan struct{})
	fresh := NewSchemaManager(driver.pool, 0)
	t.Cleanup(func() { fresh.parseCache.Close(); fresh.compilationCache.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var release sync.Once
	releaseInitializer := func() { release.Do(func() { close(trace.release) }) }
	defer releaseInitializer()
	initialDone := make(chan error, 1)
	go func() {
		initialDone <- fresh.ReadTransaction(ctx, func(graph.Transaction) error { return nil })
	}()
	select {
	case <-trace.entered:
	case <-ctx.Done():
		t.Fatal("initial catalog load did not start")
	}
	waiterCtx, cancelWaiter := context.WithCancel(ctx)
	defer cancelWaiter()
	waiterDone := make(chan error, 1)
	delegateEntered := make(chan struct{}, 1)
	go func() {
		waiterDone <- fresh.ReadTransaction(waiterCtx, func(graph.Transaction) error {
			delegateEntered <- struct{}{}
			return nil
		})
	}()
	requireEventually(t, func() bool { return driver.pool.Stat().AcquiredConns() == 2 }, "cold waiter to acquire its connection")
	cancelWaiter()
	select {
	case err := <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled cold wrapper waited for another caller's catalog query")
	}
	require.Empty(t, delegateEntered)
	require.False(t, fresh.kindCache.current.Load().loaded)
	require.True(t, fresh.kindCache.pending())
	releaseInitializer()
	require.NoError(t, <-initialDone)
	require.True(t, fresh.kindCache.current.Load().loaded)
	require.False(t, fresh.kindCache.pending())
	require.NoError(t, fresh.ReadTransaction(ctx, func(graph.Transaction) error { return nil }))
	require.Equal(t, 1, trace.count(), "canceled waiters cannot start or repeat a catalog query")
}

type kindBootstrapTrace struct {
	sync.Mutex
	schemaStarted bool
	prematureRead bool
}

func (s *kindBootstrapTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	s.Lock()
	defer s.Unlock()
	lower := strings.ToLower(data.SQL)
	if strings.Contains(lower, "create table if not exists kind") {
		s.schemaStarted = true
	} else if strings.Contains(lower, "select id, name from kind") && !s.schemaStarted {
		s.prematureRead = true
	}
	return ctx
}

func (*kindBootstrapTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestKindCacheSchemaBootstrapPrecedesFailedRefreshRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	trace := &kindBootstrapTrace{}
	config, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	config.ConnConfig.Tracer = trace
	config.MinConns, config.MaxConns = 0, 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	driver := NewDriver(0, pool)
	t.Cleanup(func() { require.NoError(t, driver.Close(context.Background())) })
	failedCtx, fail := context.WithCancel(ctx)
	fail()
	require.ErrorIs(t, driver.RefreshKinds(failedCtx), context.Canceled)
	require.True(t, driver.kindCache.pending())

	require.NoError(t, driver.AssertSchema(ctx, graph.Schema{}))
	trace.Lock()
	defer trace.Unlock()
	require.True(t, trace.schemaStarted)
	require.False(t, trace.prematureRead, "schema storage must exist before retrying a pending catalog load")
	require.True(t, driver.kindCache.current.Load().loaded)
	require.False(t, driver.kindCache.pending())
}
