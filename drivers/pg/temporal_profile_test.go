//go:build manual_integration

package pg_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/temporaltest"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func TestReconstructionProfile(t *testing.T) {
	// Set up a 10k/50k graph
	env := temporaltest.New(t)
	defer env.Close()

	t.Log("Populating 10k nodes, 50k edges...")
	env.CreateNodes(10000)
	env.CreateRandomEdges(50000)
	actualEdges := env.CurrentEdgeCount()
	t.Logf("Actual edges created: %d", actualEdges)

	snap := env.Snapshot()

	env.CreateRandomEdges(5000)
	env.DeleteEdges(2000)

	// Now manually profile the reconstruction
	pool, err := pg.NewPool(os.Getenv("PG_CONNECTION_STRING"))
	require.NoError(t, err)

	db, err := dawgs.Open(context.Background(), pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pool,
	})
	require.NoError(t, err)
	defer db.Close(context.Background())

	_ = db.(*pg.Driver)
	require.NoError(t, db.AssertSchema(context.Background(), graph.Schema{
		DefaultGraph: graph.Graph{Name: "temporal_TestReconstructionProfile_placeholder"},
	}))

	defGraph, ok := env.Driver.DefaultGraph()
	require.True(t, ok)

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadWrite})
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	// Time node temp table
	start := time.Now()
	_, err = tx.Exec(ctx, `create temp table node on commit drop as
		select n.id, n.graph_id, n.kind_ids, n.properties, n.created_at
		from node n where n.graph_id = $1 and n.created_at <= $2
		union all
		select d.node_id as id, d.graph_id, d.kind_ids, d.properties, d.created_at
		from node_deletion_log d where d.graph_id = $1 and d.deleted_at > $2 and d.created_at <= $2`, defGraph.ID, snap)
	require.NoError(t, err)
	t.Logf("Node temp table creation: %v", time.Since(start))

	// Time edge temp table
	start = time.Now()
	_, err = tx.Exec(ctx, `create temp table edge on commit drop as
		select e.id, e.graph_id, e.start_id, e.end_id, e.kind_id, e.properties, e.created_at
		from edge e where e.graph_id = $1 and e.created_at <= $2
		union all
		select d.edge_id as id, d.graph_id, d.start_id, d.end_id, d.kind_id, d.properties, d.created_at
		from edge_deletion_log d where d.graph_id = $1 and d.deleted_at > $2 and d.created_at <= $2`, defGraph.ID, snap)
	require.NoError(t, err)
	t.Logf("Edge temp table creation: %v", time.Since(start))

	// Time raw count (no indexes)
	start = time.Now()
	var count int64
	require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge").Scan(&count))
	t.Logf("Count without indexes (%d edges): %v", count, time.Since(start))

	// Time index creation
	start = time.Now()
	_, err = tx.Exec(ctx, "create index on edge (start_id)")
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "create index on edge (end_id)")
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "create index on edge (kind_id)")
	require.NoError(t, err)
	t.Logf("Index creation (3 indexes): %v", time.Since(start))

	// Time count with indexes
	start = time.Now()
	require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge").Scan(&count))
	t.Logf("Count with indexes (%d edges): %v", count, time.Since(start))

	// Time a realistic join query (would an index help?)
	start = time.Now()
	require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge e1 join edge e2 on e1.end_id = e2.start_id").Scan(&count))
	t.Logf("2-hop join with indexes (%d paths): %v", count, time.Since(start))

	require.NoError(t, tx.Commit(ctx))
}
