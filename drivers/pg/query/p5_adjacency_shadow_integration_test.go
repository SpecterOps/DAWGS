//go:build manual_integration && integration

package query

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/stretchr/testify/require"
)

// TestP5AdjacencyShadowLifecycle verifies the opt-in P5 relation backfills
// existing edges and remains transactionally synchronized without changing the
// normal graph schema.
func TestP5AdjacencyShadowLifecycle(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, sqlP5AdjacencyShadowDown)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, cleanupErr := pool.Exec(ctx, sqlP5AdjacencyShadowDown)
		require.NoError(t, cleanupErr)
	})

	graphName := fmt.Sprintf("p5_adjacency_shadow_%d", time.Now().UnixNano())
	var graphID int32
	require.NoError(t, pool.QueryRow(ctx, `insert into graph(name) values ($1) returning id`, graphName).Scan(&graphID))
	_, err = pool.Exec(ctx, fmt.Sprintf("create table node_%d partition of node for values in (%d)", graphID, graphID))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf("create table edge_%d partition of edge for values in (%d)", graphID, graphID))
	require.NoError(t, err)

	insertNode := func() int64 {
		var nodeID int64
		require.NoError(t, pool.QueryRow(ctx,
			`insert into node(graph_id, kind_ids, properties) values ($1, array[1]::int2[], '{}'::jsonb) returning id`,
			graphID,
		).Scan(&nodeID))
		return nodeID
	}
	insertEdge := func(startID, endID int64) int64 {
		var edgeID int64
		require.NoError(t, pool.QueryRow(ctx,
			`insert into edge(graph_id, start_id, end_id, kind_id, properties) values ($1, $2, $3, 1, '{}'::jsonb) returning id`,
			graphID, startID, endID,
		).Scan(&edgeID))
		return edgeID
	}

	nodeOne := insertNode()
	nodeTwo := insertNode()
	nodeThree := insertNode()
	nodeFour := insertNode()
	edgeOne := insertEdge(nodeOne, nodeTwo)
	edgeTwo := insertEdge(nodeTwo, nodeThree)

	_, err = pool.Exec(ctx, sqlP5AdjacencyShadowUp)
	require.NoError(t, err)

	assertExactShadow := func(expectedEdges int64) {
		var rows, mismatches int64
		require.NoError(t, pool.QueryRow(ctx,
			`select count(*) from public.p5_adjacency_v1 where graph_id = $1`, graphID).Scan(&rows))
		require.Equal(t, expectedEdges*2, rows)
		require.NoError(t, pool.QueryRow(ctx, `
			select
			  (select count(*)
			   from edge e
			   where e.graph_id = $1
			     and not exists (
			       select 1 from public.p5_adjacency_v1 a
			       where a.graph_id = e.graph_id and a.edge_id = e.id
			         and a.direction = 1 and a.anchor_id = e.start_id
			         and a.neighbor_id = e.end_id and a.kind_id = e.kind_id
			     )
			     or not exists (
			       select 1 from public.p5_adjacency_v1 a
			       where a.graph_id = e.graph_id and a.edge_id = e.id
			         and a.direction = -1 and a.anchor_id = e.end_id
			         and a.neighbor_id = e.start_id and a.kind_id = e.kind_id
			     ))
			  +
			  (select count(*)
			   from public.p5_adjacency_v1 a
			   where a.graph_id = $1
			     and not exists (
			       select 1 from edge e
			       where e.graph_id = a.graph_id and e.id = a.edge_id
			         and ((a.direction = 1 and a.anchor_id = e.start_id and a.neighbor_id = e.end_id)
			           or (a.direction = -1 and a.anchor_id = e.end_id and a.neighbor_id = e.start_id))
			         and a.kind_id = e.kind_id
			     ))
		`, graphID).Scan(&mismatches))
		require.Zero(t, mismatches)
	}

	assertExactShadow(2)

	var beforePropertyUpdate string
	require.NoError(t, pool.QueryRow(ctx,
		`select string_agg(ctid::text, ',' order by direction) from public.p5_adjacency_v1 where graph_id = $1 and edge_id = $2`,
		graphID, edgeOne,
	).Scan(&beforePropertyUpdate))
	_, err = pool.Exec(ctx,
		`update edge set properties = properties || '{"touch": true}'::jsonb where graph_id = $1 and id = $2`,
		graphID, edgeOne,
	)
	require.NoError(t, err)
	var afterPropertyUpdate string
	require.NoError(t, pool.QueryRow(ctx,
		`select string_agg(ctid::text, ',' order by direction) from public.p5_adjacency_v1 where graph_id = $1 and edge_id = $2`,
		graphID, edgeOne,
	).Scan(&afterPropertyUpdate))
	require.Equal(t, beforePropertyUpdate, afterPropertyUpdate)

	_, err = pool.Exec(ctx, `update edge set start_id = $1 where graph_id = $2 and id = $3`, nodeFour, graphID, edgeOne)
	require.NoError(t, err)
	assertExactShadow(2)
	var updatedOutbound int64
	require.NoError(t, pool.QueryRow(ctx, `
		select count(*) from public.p5_adjacency_v1
		where graph_id = $1 and edge_id = $2 and direction = 1 and anchor_id = $3 and neighbor_id = $4`,
		graphID, edgeOne, nodeFour, nodeTwo,
	).Scan(&updatedOutbound))
	require.Equal(t, int64(1), updatedOutbound)

	_, err = pool.Exec(ctx, `delete from node where graph_id = $1 and id = $2`, graphID, nodeThree)
	require.NoError(t, err)
	assertExactShadow(1)
	var deletedEdgeRows int64
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from public.p5_adjacency_v1 where graph_id = $1 and edge_id = $2`, graphID, edgeTwo,
	).Scan(&deletedEdgeRows))
	require.Zero(t, deletedEdgeRows)

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	var rolledBackEdge int64
	require.NoError(t, tx.QueryRow(ctx,
		`insert into edge(graph_id, start_id, end_id, kind_id, properties) values ($1, $2, $3, 1, '{}'::jsonb) returning id`,
		graphID, nodeFour, nodeOne,
	).Scan(&rolledBackEdge))
	require.NoError(t, tx.Rollback(ctx))
	var rollbackRows int64
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from public.p5_adjacency_v1 where graph_id = $1 and edge_id = $2`, graphID, rolledBackEdge,
	).Scan(&rollbackRows))
	require.Zero(t, rollbackRows)
	assertExactShadow(1)

	cancelledTx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	cancelledContext, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(20*time.Millisecond, cancel)
	_, err = cancelledTx.Exec(cancelledContext, `
		with delayed as materialized (select pg_sleep(2))
		insert into edge(graph_id, start_id, end_id, kind_id, properties)
		select $1, $2, $3, 1, '{}'::jsonb from delayed`,
		graphID, nodeOne, nodeFour,
	)
	timer.Stop()
	cancel()
	require.Error(t, err)
	_ = cancelledTx.Rollback(ctx)
	assertExactShadow(1)

	reusedConnection, err := pool.Acquire(ctx)
	require.NoError(t, err)
	var reusedRows int64
	require.NoError(t, reusedConnection.QueryRow(ctx,
		`select count(*) from public.p5_adjacency_v1 where graph_id = $1`, graphID,
	).Scan(&reusedRows))
	reusedConnection.Release()
	require.Equal(t, int64(2), reusedRows)

	_, err = pool.Exec(ctx, `delete from graph where id = $1`, graphID)
	require.NoError(t, err)
	var graphRows int64
	require.NoError(t, pool.QueryRow(ctx,
		`select count(*) from public.p5_adjacency_v1 where graph_id = $1`, graphID,
	).Scan(&graphRows))
	require.Zero(t, graphRows)

	_, err = pool.Exec(ctx, sqlP5AdjacencyShadowDown)
	require.NoError(t, err)
	var shadowPresent, edgePresent bool
	require.NoError(t, pool.QueryRow(ctx,
		`select to_regclass('public.p5_adjacency_v1') is not null, to_regclass('public.edge') is not null`,
	).Scan(&shadowPresent, &edgePresent))
	require.False(t, shadowPresent)
	require.True(t, edgePresent)
}
