//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// DAWGS-065 reproduction: kind registration inside a write transaction
// acquires a second pool connection while holding the schema write lock
// (SchemaManager.assertKinds -> SchemaManager.WriteTransaction). These
// probes pin the current wedged behavior with bounded contexts; the
// connection-ownership repair must invert them so that registration at the
// pool maximum completes. They are PostgreSQL driver-scoped and skip unless
// CONNECTION_STRING selects a PostgreSQL target.

const (
	dawgs065GraphName = "dawgs065_pool_wedge"

	dawgs065KindPrefix = "DAWGS065"
)

var dawgs065BaseNodeKind = graph.StringKind("DAWGS065WedgeNode")

// newDAWGS065Driver builds a driver whose effective pool maximum is verified
// instead of assumed, which is the trap the original finding calls out: pools
// built without an explicit RuntimeConfig silently use the v1 default of 50
// connections.
func newDAWGS065Driver(t *testing.T, maxConns int32) *Driver {
	t.Helper()
	driver := newV2IntegrationDriver(t, maxConns, 8, nil)
	require.Equal(t, maxConns, driver.pool.Stat().MaxConns(), "effective pool maximum must match the requested limit")
	return driver
}

// warmDAWGS065Pool opens every physical connection up front so later timing
// windows measure lock waits rather than connection establishment.
func warmDAWGS065Pool(t *testing.T, pool *pgxpool.Pool, conns int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	acquired := make([]*pgxpool.Conn, 0, conns)
	for i := 0; i < conns; i++ {
		conn, err := pool.Acquire(ctx)
		require.NoError(t, err)
		acquired = append(acquired, conn)
	}
	for _, conn := range acquired {
		conn.Release()
	}
}

func setupDAWGS065Schema(t *testing.T, driver *Driver) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schema := graph.Schema{
		Graphs: []graph.Graph{{
			Name:  dawgs065GraphName,
			Nodes: graph.Kinds{dawgs065BaseNodeKind},
		}},
		DefaultGraph: graph.Graph{Name: dawgs065GraphName},
	}
	require.NoError(t, driver.AssertSchema(ctx, schema))

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()

		require.NoError(t, driver.Run(cleanupCtx, fmt.Sprintf(
			`delete from edge where graph_id = (select id from graph where name = '%s')`, dawgs065GraphName), nil))
		require.NoError(t, driver.Run(cleanupCtx, fmt.Sprintf(
			`delete from node where graph_id = (select id from graph where name = '%s')`, dawgs065GraphName), nil))
		require.NoError(t, driver.Run(cleanupCtx, fmt.Sprintf(
			`delete from kind where name like '%s%%'`, dawgs065KindPrefix), nil))
	})
}

// dawgs065Scalar runs one scalar SQL read through the driver under test.
func dawgs065Scalar(t *testing.T, driver *Driver, sql string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var count int64
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Raw(sql, nil)
		defer result.Close()
		if result.Next() {
			return result.Scan(&count)
		}
		return result.Error()
	}))
	return count
}

func dawgs065NodeCountSQL() string {
	return fmt.Sprintf(`select count(*) from node where graph_id = (select id from graph where name = '%s')`, dawgs065GraphName)
}

func dawgs065KindCountSQL(name string) string {
	return fmt.Sprintf(`select count(*) from kind where name = '%s'`, name)
}

// requireSchemaWriteLockHeld blocks until kind registration is wedged under
// the schema write lock: the lock must be continuously unavailable to writers
// for a short sample window. Once one registration holds the write lock and
// waits for a second connection, no other kind-mapping or graph-resolution
// path can proceed.
func requireSchemaWriteLockHeld(t *testing.T, manager *SchemaManager) {
	t.Helper()

	const (
		sampleAttempts = 10
		sampleInterval = 2 * time.Millisecond
	)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		unavailable := 0
		for i := 0; i < sampleAttempts; i++ {
			if manager.lock.TryLock() {
				manager.lock.Unlock()
				unavailable = 0
			} else {
				unavailable++
			}
			time.Sleep(sampleInterval)
		}
		if unavailable == sampleAttempts {
			return
		}
	}
	t.Fatalf("schema write lock was never continuously held; kind registration did not wedge")
}

// TestKindRegistrationWaitsForSecondConnectionOnSingleConnectionPool pins
// the single-call shape from the original probe: with an effective pool
// maximum of one, CreateNode with an uncached kind blocks inside the nested
// registration write until its context expires, and commits nothing.
func TestKindRegistrationWaitsForSecondConnectionOnSingleConnectionPool(t *testing.T) {
	const registrationTimeout = 750 * time.Millisecond

	driver := newDAWGS065Driver(t, 1)
	warmDAWGS065Pool(t, driver.pool, 1)
	setupDAWGS065Schema(t, driver)

	freshKind := graph.StringKind("DAWGS065FreshSingle")

	ctx, cancel := context.WithTimeout(context.Background(), registrationTimeout)
	defer cancel()

	started := time.Now()
	err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("probe", "single"), freshKind)
		return err
	})

	// The registration wait, not some other failure, must consume the budget.
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(started), registrationTimeout)

	// The transaction rolled back and the nested registration never executed.
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL(freshKind.String())))
}

// TestKindMappingMissWaitsForConnectionWhilePoolIsHeld pins the read-side
// family: MapKinds on a cache miss refreshes through a new read transaction
// under the schema write lock, so it also needs a second connection even
// though it performs no writes.
func TestKindMappingMissWaitsForConnectionWhilePoolIsHeld(t *testing.T) {
	const mappingTimeout = 750 * time.Millisecond

	driver := newDAWGS065Driver(t, 1)
	warmDAWGS065Pool(t, driver.pool, 1)
	setupDAWGS065Schema(t, driver)

	var (
		acquired  = make(chan struct{})
		release   = make(chan struct{})
		holderErr = make(chan error, 1)
	)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		holderErr <- driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			acquired <- struct{}{}
			<-release

			_, err := tx.WithGraph(graph.Graph{Name: dawgs065GraphName}).Nodes().Count()
			return err
		})
	}()
	<-acquired

	freshKind := graph.StringKind("DAWGS065FreshMapMiss")

	ctx, cancel := context.WithTimeout(context.Background(), mappingTimeout)
	defer cancel()

	started := time.Now()
	_, err := driver.MapKinds(ctx, graph.Kinds{freshKind})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(started), mappingTimeout)

	// Once the holder releases its connection, the same miss completes
	// promptly and reports the unmappable kind rather than waiting again.
	close(release)
	require.NoError(t, <-holderErr)

	mapCtx, mapCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer mapCancel()

	_, err = driver.MapKinds(mapCtx, graph.Kinds{freshKind})
	require.ErrorContains(t, err, "unable to map kinds")
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL(freshKind.String())))
}

// TestKindRegistrationAtPoolMaximumWedgesSchemaLockAndUnrelatedReads pins
// the saturated-pool shape: every connection is held by a transaction whose
// CreateNode needs to register a fresh kind. One registration holds the
// schema write lock and waits for a second connection; the other
// registrations block on the schema lock. An unrelated read that already
// holds its own connection then stalls behind the same lock until a
// registration context expires.
func TestKindRegistrationAtPoolMaximumWedgesSchemaLockAndUnrelatedReads(t *testing.T) {
	const (
		writerCount    = 2
		readerCount    = 1
		writerDeadline = 2 * time.Second
	)

	driver := newDAWGS065Driver(t, writerCount+readerCount)
	warmDAWGS065Pool(t, driver.pool, writerCount+readerCount)
	setupDAWGS065Schema(t, driver)

	var (
		acquired      = make(chan struct{}, writerCount+readerCount)
		releaseAll    = make(chan struct{})
		writerResults = make(chan error, writerCount)
		readerResults = make(chan readerOutcome, readerCount)
	)

	for i := 0; i < writerCount; i++ {
		go func(writerIndex int) {
			ctx, cancel := context.WithTimeout(context.Background(), writerDeadline)
			defer cancel()

			err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				acquired <- struct{}{}
				<-releaseAll

				_, err := tx.CreateNode(
					graph.NewProperties().Set("writer", writerIndex),
					graph.StringKind(fmt.Sprintf("DAWGS065FreshBarrier%d", writerIndex)),
				)
				return err
			})
			writerResults <- err
		}(i)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		outcome := readerOutcome{started: time.Now()}
		outcome.err = driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			acquired <- struct{}{}
			<-releaseAll

			count, err := tx.WithGraph(graph.Graph{Name: dawgs065GraphName}).Nodes().Count()
			outcome.count = count
			return err
		})
		outcome.elapsed = time.Since(outcome.started)
		readerResults <- outcome
	}()

	// All three operations hold their connections before anything proceeds.
	for i := 0; i < writerCount+readerCount; i++ {
		<-acquired
	}
	close(releaseAll)

	// Kind registration must wedge under the schema write lock while every
	// pool connection is held.
	requireSchemaWriteLockHeld(t, driver.SchemaManager)

	// Both registrations fail through their context deadlines: each already
	// holds a connection, but registration needs a second one.
	for i := 0; i < writerCount; i++ {
		select {
		case err := <-writerResults:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(10 * time.Second):
			t.Fatalf("writer %d did not return after its registration deadline", i)
		}
	}

	// The unrelated reader held its connection from the start, so its stall
	// is the schema lock, not pool acquisition. It completes only after the
	// registration contexts expire and the write lock is released.
	select {
	case outcome := <-readerResults:
		require.NoError(t, outcome.err)
		require.Equal(t, int64(0), outcome.count)
		require.GreaterOrEqual(t, outcome.elapsed, writerDeadline-500*time.Millisecond)
	case <-time.After(15 * time.Second):
		t.Fatal("unrelated reader did not complete after the wedge cleared")
	}

	// Nothing was committed and no wedge kind was defined.
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL("DAWGS065FreshBarrier0")))
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL("DAWGS065FreshBarrier1")))

	// The wait cycle is released by context expiry, not by pool damage: a
	// fresh registration now completes because a connection is free.
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer recoveryCancel()

	require.NoError(t, driver.WriteTransaction(recoveryCtx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("probe", "recovery"), graph.StringKind("DAWGS065FreshRecovery"))
		return err
	}))
	require.Equal(t, int64(1), dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
	require.Equal(t, int64(1), dawgs065Scalar(t, driver, dawgs065KindCountSQL("DAWGS065FreshRecovery")))
}

// TestConcurrentWritesWithCachedKindsCompleteAtPoolMaximum isolates the
// trigger: the same saturated concurrency with already-cached kinds takes the
// AssertKinds fast path and never needs a second connection.
func TestConcurrentWritesWithCachedKindsCompleteAtPoolMaximum(t *testing.T) {
	const (
		writerCount     = 2
		completionWait  = 10 * time.Second
		barrierDeadline = 5 * time.Second
	)

	driver := newDAWGS065Driver(t, writerCount)
	warmDAWGS065Pool(t, driver.pool, writerCount)
	setupDAWGS065Schema(t, driver)

	var (
		acquired   = make(chan struct{}, writerCount)
		releaseAll = make(chan struct{})
		results    = make(chan error, writerCount)
	)

	for i := 0; i < writerCount; i++ {
		go func(writerIndex int) {
			ctx, cancel := context.WithTimeout(context.Background(), completionWait)
			defer cancel()

			err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				acquired <- struct{}{}
				<-releaseAll

				_, err := tx.CreateNode(
					graph.NewProperties().Set("writer", writerIndex),
					dawgs065BaseNodeKind,
				)
				return err
			})
			results <- err
		}(i)
	}

	for i := 0; i < writerCount; i++ {
		select {
		case <-acquired:
		case <-time.After(barrierDeadline):
			t.Fatal("writer did not acquire its connection")
		}
	}
	close(releaseAll)

	for i := 0; i < writerCount; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(completionWait):
			t.Fatalf("cached-kind writer %d did not complete", i)
		}
	}
	require.Equal(t, int64(writerCount), dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
}

type readerOutcome struct {
	started time.Time
	elapsed time.Duration
	count   int64
	err     error
}
