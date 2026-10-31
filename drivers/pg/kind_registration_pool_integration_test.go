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

// DAWGS-065: registration and mapping must not lease a second connection.

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

// Registration must use the connection already owned by the transaction.
func TestKindRegistrationSingleConnectionPool(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	warmDAWGS065Pool(t, driver.pool, 1)
	setupDAWGS065Schema(t, driver)
	fresh := graph.StringKind("DAWGS065FreshSingle")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.CreateNode(graph.NewProperties().Set("probe", "single"), fresh)
		if err != nil {
			return err
		}
		require.Equal(t, graph.Kinds{fresh}, node.Kinds)
		return nil
	}))
	require.EqualValues(t, 1, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
	require.EqualValues(t, 1, dawgs065Scalar(t, driver, dawgs065KindCountSQL(fresh.String())))
}

func TestKindRegistrationRollbackDoesNotPublishIDs(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fresh := graph.StringKind("DAWGS065Rollback")
	rollback := fmt.Errorf("rollback fixture")
	require.ErrorIs(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), fresh)
		if err != nil {
			return err
		}
		return rollback
	}), rollback)
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL(fresh.String())))
	_, err := driver.MapKinds(ctx, graph.Kinds{fresh})
	require.ErrorContains(t, err, "unable to map kinds")
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), fresh)
		return err
	}))
}

func TestKindMappingMissOnOwnedConnection(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.(*transaction).kindMapper.MapKinds(ctx, graph.Kinds{graph.StringKind("DAWGS065Missing")})
		require.ErrorContains(t, err, "unable to map kinds")
		_, err = tx.WithGraph(graph.Graph{Name: dawgs065GraphName}).Nodes().Count()
		return err
	}))
}

func TestKindRegistrationAtPoolMaximum(t *testing.T) {
	driver := newDAWGS065Driver(t, 3)
	warmDAWGS065Pool(t, driver.pool, 3)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acquired := make(chan struct{}, 3)
	release := make(chan struct{})
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			delegate := func(tx graph.Transaction) error {
				acquired <- struct{}{}
				<-release
				if i == 2 {
					_, err := tx.WithGraph(graph.Graph{Name: dawgs065GraphName}).Nodes().Count()
					return err
				}
				_, err := tx.CreateNode(graph.NewProperties(), graph.StringKind(fmt.Sprintf("DAWGS065Concurrent%d", i)))
				return err
			}
			if i == 2 {
				results <- driver.ReadTransaction(ctx, delegate)
			} else {
				results <- driver.WriteTransaction(ctx, delegate)
			}
		}(i)
	}
	for i := 0; i < 3; i++ {
		<-acquired
	}
	close(release)
	for i := 0; i < 3; i++ {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 2, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
}

func TestBatchKindRegistrationSingleConnectionPool(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.CreateNode(graph.PrepareNode(graph.NewProperties(), graph.StringKind("DAWGS065Batch")))
	}))
	require.EqualValues(t, 1, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
}

func TestCompiledKindRegistrationRollbackDoesNotPoisonCache(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rollback := fmt.Errorf("rollback compiled registration")
	for pass := 0; pass < 2; pass++ {
		err := driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Query("CREATE (n:DAWGS065CompiledRollback {value: $value}) RETURN n", map[string]any{"value": pass})
			defer result.Close()
			require.True(t, result.Next(), "%v", result.Error())
			var node graph.Node
			require.NoError(t, result.Scan(&node))
			require.Equal(t, graph.Kinds{graph.StringKind("DAWGS065CompiledRollback")}, node.Kinds)
			require.EqualValues(t, pass, node.Properties.Map["value"])
			require.False(t, result.Next())
			if err := result.Error(); err != nil {
				return err
			}
			if pass == 0 {
				return rollback
			}
			return nil
		})
		if pass == 0 {
			require.ErrorIs(t, err, rollback)
		} else {
			require.NoError(t, err)
		}
	}
	require.EqualValues(t, 1, dawgs065Scalar(t, driver, dawgs065KindCountSQL("DAWGS065CompiledRollback")))
	require.EqualValues(t, 1, dawgs065Scalar(t, driver, dawgs065NodeCountSQL()))
}

func TestStandaloneKindRefreshAndResolutionUseNoMetadataLockDuringFetch(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, driver.RefreshKinds(ctx))
	missing := graph.StringKind("DAWGS065StandaloneMissing")
	ids, unresolved, err := driver.resolveKindIDs(ctx, graph.Kinds{dawgs065BaseNodeKind, missing})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, graph.Kinds{missing}, unresolved)
	require.Zero(t, dawgs065Scalar(t, driver, dawgs065KindCountSQL(missing.String())))
}
