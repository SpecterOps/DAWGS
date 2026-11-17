//go:build manual_integration

package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const (
	// traversalLimitFixtureGraph isolates constructor-budget fixtures from other integration graphs.
	traversalLimitFixtureGraph = "traversal_limit_config"

	// traversalLimitFixtureQuery requires four admitted trail states on the test chain.
	traversalLimitFixtureQuery = `MATCH (a:TraversalLimitStart)-[:TraversalLimitStep*]->(b) RETURN count(b)`

	// deleteTraversalLimitEdgesSQL removes only relationships belonging to the constructor-budget fixture.
	deleteTraversalLimitEdgesSQL = `delete from edge where graph_id = (select id from graph where name = @graph_name)`

	// deleteTraversalLimitNodesSQL removes only nodes belonging to the constructor-budget fixture.
	deleteTraversalLimitNodesSQL = `delete from node where graph_id = (select id from graph where name = @graph_name)`
)

// newTraversalLimitPool creates a single-connection pool with known inherited traversal settings.
func newTraversalLimitPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	poolConfig, err := pgxpool.ParseConfig(postgresV2IntegrationConnectionString(t))
	require.NoError(t, err)
	poolConfig.ConnConfig.RuntimeParams[traversalStateLimitSetting] = "10000"
	poolConfig.ConnConfig.RuntimeParams[traversalByteLimitSetting] = "1048576"

	pool, err := NewPoolWithRuntimeConfig(context.Background(), poolConfig, RuntimeConfig{
		Pool: &PoolConfig{MinConnections: 0, MaxConnections: 1},
	})
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// openTraversalLimitDriver exercises the public constructor and retains driver cleanup until the fixture finishes.
func openTraversalLimitDriver(t *testing.T, pool *pgxpool.Pool, limits traversalLimits) *Driver {
	t.Helper()
	database, err := dawgs.Open(context.Background(), DriverName, dawgs.Config{
		Pool:                pool,
		TraversalStateLimit: limits.stateLimit,
		TraversalByteLimit:  limits.byteLimit,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close(context.Background())) })

	driver, ok := database.(*Driver)
	require.True(t, ok)
	return driver
}

// setupTraversalLimitFixture creates a four-edge chain and registers cleanup before its drivers close.
func setupTraversalLimitFixture(t *testing.T, driver *Driver) {
	t.Helper()
	var (
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		definition  = graph.Graph{Name: traversalLimitFixtureGraph}
	)
	defer cancel()
	require.NoError(t, driver.AssertSchema(ctx, graph.Schema{
		Graphs:       []graph.Graph{definition},
		DefaultGraph: definition,
	}))

	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		parameters := map[string]any{"graph_name": traversalLimitFixtureGraph}
		require.NoError(t, driver.Run(cleanupCtx, deleteTraversalLimitEdgesSQL, parameters))
		require.NoError(t, driver.Run(cleanupCtx, deleteTraversalLimitNodesSQL, parameters))
	}
	t.Cleanup(cleanup)
	cleanup()

	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var previous *graph.Node
		for index := 0; index < 5; index++ {
			kinds := graph.Kinds{graph.StringKind("TraversalLimitNode")}
			if index == 0 {
				kinds = append(kinds, graph.StringKind("TraversalLimitStart"))
			}

			if node, err := tx.CreateNode(graph.NewProperties(), kinds...); err != nil {
				return err
			} else {
				if previous != nil {
					if _, err := tx.CreateRelationshipByIDs(previous.ID, node.ID, graph.StringKind("TraversalLimitStep"), graph.NewProperties()); err != nil {
						return err
					}
				}

				previous = node
			}
		}

		return nil
	}))
}

// assertTraversalLimitSession checks that pooled session defaults are restored and returns its physical connection ID.
func assertTraversalLimitSession(t *testing.T, pool *pgxpool.Pool) uint32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	var settings traversalLimitSettings
	require.NoError(t, conn.QueryRow(ctx, readTraversalLimitsSQL,
		traversalStateLimitSetting, traversalByteLimitSetting).Scan(&settings.stateLimit, &settings.byteLimit))
	require.Equal(t, traversalLimitSettings{stateLimit: "10000", byteLimit: "1048576"}, settings)
	return conn.Conn().PgConn().PID()
}

// TestTraversalLimitConfiguredBudgets verifies constructor overrides affect real queries in all transaction modes.
func TestTraversalLimitConfiguredBudgets(t *testing.T) {
	var (
		pool    = newTraversalLimitPool(t)
		drivers = map[string]*Driver{}
		limits  = map[string]traversalLimits{
			"inherited": {},
			"states":    {stateLimit: 2},
			"bytes":     {byteLimit: 150},
			"raised":    {stateLimit: 100, byteLimit: 4096},
		}
	)
	for name, config := range limits {
		drivers[name] = openTraversalLimitDriver(t, pool, config)
	}

	setupTraversalLimitFixture(t, drivers["inherited"])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, driver := range drivers {
		require.NoError(t, driver.SetDefaultGraph(ctx, graph.Graph{Name: traversalLimitFixtureGraph}))
	}

	connectionID := assertTraversalLimitSession(t, pool)
	for mode, run := range map[string]func(*Driver, graph.TransactionDelegate) error{
		"autocommit": func(driver *Driver, delegate graph.TransactionDelegate) error {
			return driver.ReadTransaction(ctx, delegate)
		},
		"explicit read": func(driver *Driver, delegate graph.TransactionDelegate) error {
			return driver.ReadTransaction(ctx, delegate, OptionSetTransactionIsolation(pgx.ReadCommitted))
		},
		"write": func(driver *Driver, delegate graph.TransactionDelegate) error {
			return driver.WriteTransaction(ctx, delegate)
		},
	} {
		t.Run(mode, func(t *testing.T) {
			for name, driver := range drivers {
				t.Run(name, func(t *testing.T) {
					var counts []any
					err := run(driver, func(tx graph.Transaction) error {
						traversal := tx.Query(traversalLimitFixtureQuery, nil)
						defer traversal.Close()
						for traversal.Next() {
							counts = append(counts, traversal.Values()[0])
						}

						return traversal.Error()
					})
					if name == "states" || name == "bytes" {
						var pgError *pgconn.PgError
						require.ErrorAs(t, err, &pgError)
						require.Equal(t, "54000", pgError.Code)
						require.Contains(t, pgError.Message, "traversal resource limit")
					} else {
						require.NoError(t, err)
						require.Equal(t, []any{int64(4)}, counts)
					}

					require.Equal(t, connectionID, assertTraversalLimitSession(t, pool))
				})
			}
		})
	}

	// Replacing the physical connection must retain the constructor override.
	pool.Reset()
	require.NoError(t, drivers["raised"].ReadTransaction(ctx, func(tx graph.Transaction) error {
		var settings traversalLimitSettings
		err := tx.(*transaction).driver().QueryRow(ctx, readTraversalLimitsSQL,
			traversalStateLimitSetting, traversalByteLimitSetting).Scan(&settings.stateLimit, &settings.byteLimit)
		require.Equal(t, traversalLimitSettings{stateLimit: "100", byteLimit: "4096"}, settings)
		return err
	}))
	require.NotEqual(t, connectionID, assertTraversalLimitSession(t, pool))
}

// TestTraversalLimitOperationCleanup verifies delegate errors and cancellation preserve reusable pooled settings.
func TestTraversalLimitOperationCleanup(t *testing.T) {
	var (
		pool    = newTraversalLimitPool(t)
		driver  = openTraversalLimitDriver(t, pool, traversalLimits{stateLimit: 100, byteLimit: 4096})
		failure = errors.New("delegate failed")
	)
	setupTraversalLimitFixture(t, driver)
	connectionID := assertTraversalLimitSession(t, pool)

	for _, mode := range []string{"delegate error", "query error", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				switch mode {
				case "query error":
					query := tx.Raw("select 1 / 0", nil)
					defer query.Close()
					for query.Next() {
					}

					return query.Error()

				case "cancellation":
					cancel()
					return ctx.Err()

				default:
					return failure
				}
			})
			switch mode {
			case "cancellation":
				require.ErrorIs(t, err, context.Canceled)

			case "delegate error":
				require.ErrorIs(t, err, failure)

			default:
				var pgError *pgconn.PgError
				require.ErrorAs(t, err, &pgError)
				require.Equal(t, "22012", pgError.Code)
			}

			require.Equal(t, connectionID, assertTraversalLimitSession(t, pool))
		})
	}
}

// TestTraversalLimitCleanupDiscardsBusyConnection verifies failed restoration cannot return changed settings to the pool.
func TestTraversalLimitCleanupDiscardsBusyConnection(t *testing.T) {
	var (
		pool   = newTraversalLimitPool(t)
		driver = openTraversalLimitDriver(t, pool, traversalLimits{stateLimit: 100})
		query  graph.Result
	)
	setupTraversalLimitFixture(t, driver)
	connectionID := assertTraversalLimitSession(t, pool)
	require.NoError(t, driver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		// Deliberately retain unread rows to make restoration fail with a busy connection.
		query = tx.Raw("select 1", nil)
		return nil
	}))
	query.Close()

	require.NotEqual(t, connectionID, assertTraversalLimitSession(t, pool))
}
