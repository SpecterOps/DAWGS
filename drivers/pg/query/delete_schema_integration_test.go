//go:build manual_integration

package query

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPostgreSQLNodeDeleteSchema exercises both fresh partition creation and
// repeat schema upgrades, with overlapping node/edge IDs in distinct graphs.
func TestPostgreSQLNodeDeleteSchema(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" || !(strings.HasPrefix(connection, "postgres://") || strings.HasPrefix(connection, "postgresql://")) {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, connection)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	ddl, _, found := strings.Cut(sqlSchemaUp, "create or replace function public.kind_name")
	require.True(t, found)
	for _, upgrade := range []bool{false, true} {
		for _, partition := range []bool{false, true} {
			t.Run(fmt.Sprintf("upgrade=%t/partition=%t", upgrade, partition), func(t *testing.T) {
				tx, err := conn.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				schema := pgx.Identifier{fmt.Sprintf("dawgs_node_delete_%d", time.Now().UnixNano())}.Sanitize()
				execEdgeSchemaSQL(t, ctx, tx, "create schema "+schema)
				execEdgeSchemaSQL(t, ctx, tx, "set local search_path to "+schema)
				execEdgeSchemaSQL(t, ctx, tx, ddl)
				execEdgeSchemaSQL(t, ctx, tx, "insert into graph (id,name) values (1,'first'),(2,'second')")
				for _, id := range []int32{1, 2} {
					execEdgeSchemaSQL(t, ctx, tx, formatCreatePartitionTable(fmt.Sprintf("node_%d", id), "node", id))
					execEdgeSchemaSQL(t, ctx, tx, formatCreateNodeDeleteTrigger(fmt.Sprintf("node_%d", id)))
					execEdgeSchemaSQL(t, ctx, tx, formatCreatePartitionTable(fmt.Sprintf("edge_%d", id), "edge", id))
				}
				if upgrade {
					execEdgeSchemaSQL(t, ctx, tx, "drop trigger delete_node_edges on node_1; drop trigger delete_node_edges on node_2")
					execEdgeSchemaSQL(t, ctx, tx, ddl)
					execEdgeSchemaSQL(t, ctx, tx, ddl)
				}
				execEdgeSchemaSQL(t, ctx, tx, `insert into node (id,graph_id,kind_ids,properties)
                    select id, graph_id, array[1]::int2[], '{}'::jsonb from unnest(array[100,101,102]) id cross join unnest(array[1,2]) graph_id;
                    insert into edge(id,graph_id,start_id,end_id,kind_id,properties)
                    select e.id,graph_id,e.start_id,e.end_id,1,'{}'::jsonb
                    from (values (10,100,101),(11,101,100),(12,100,100),(13,101,102)) e(id,start_id,end_id)
                    cross join unnest(array[1,2]) graph_id`)
				execEdgeSchemaSQL(t, ctx, tx, "set local enable_seqscan = off")
				const scanCounts = `select coalesce(sum(seq_scan),0)::int8,coalesce(sum(idx_scan),0)::int8
					from pg_stat_xact_user_tables where schemaname=current_schema() and relname in ('edge_1','edge_2')`
				var beforeSeqScans, beforeIndexScans int64
				require.NoError(t, tx.QueryRow(ctx, scanCounts).Scan(&beforeSeqScans, &beforeIndexScans))
				var allowed bool
				require.NoError(t, tx.QueryRow(ctx, "select cypher_assert_node_deletable(1,999,array[]::int8[],array[]::int8[])").Scan(&allowed))
				require.True(t, allowed)
				var seqScans, indexScans int64
				require.NoError(t, tx.QueryRow(ctx, scanCounts).Scan(&seqScans, &indexScans))
				require.Equal(t, beforeSeqScans, seqScans, "the plain-DELETE guard must use endpoint indexes")
				require.Greater(t, indexScans, beforeIndexScans)
				target := "node"
				if partition {
					target = "node_1"
				}
				deletion := "delete from " + target + " where graph_id=1 and id=100"
				assertCounts := func(deleted bool) {
					t.Helper()
					wantNodes, wantEdges := int64(3), int64(4)
					if deleted {
						wantNodes, wantEdges = 2, 1
					}
					var nodesA, edgesA, nodesB, edgesB int64
					err := tx.QueryRow(ctx, `select
                        (select count(*) from node where graph_id=1), (select count(*) from edge where graph_id=1),
                        (select count(*) from node where graph_id=2), (select count(*) from edge where graph_id=2)`).Scan(&nodesA, &edgesA, &nodesB, &edgesB)
					require.NoError(t, err)
					require.Equal(t, []int64{wantNodes, wantEdges, 3, 4}, []int64{nodesA, edgesA, nodesB, edgesB})
					var survivors int64
					require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge where graph_id=1 and id=13 and start_id=101 and end_id=102").Scan(&survivors))
					require.Equal(t, int64(1), survivors)
				}
				execEdgeSchemaSQL(t, ctx, tx, "savepoint before_delete")
				execEdgeSchemaSQL(t, ctx, tx, deletion)
				assertCounts(true)
				require.NoError(t, tx.QueryRow(ctx, "select cypher_assert_node_deletable(1,100,array[]::int8[],array[]::int8[])").Scan(&allowed))
				require.True(t, allowed, "another graph's incident edges must not block deletion")
				execEdgeSchemaSQL(t, ctx, tx, "rollback to savepoint before_delete")
				assertCounts(false)
				execEdgeSchemaSQL(t, ctx, tx, deletion)
				assertCounts(true)
				_, triggerDown, found := strings.Cut(sqlSchemaDown, "drop trigger if exists delete_node_edges on node;")
				require.True(t, found)
				triggerDown, _, found = strings.Cut(triggerDown, "drop function if exists delete_node_edges;")
				require.True(t, found)
				execEdgeSchemaSQL(t, ctx, tx, "drop trigger if exists delete_node_edges on node;"+triggerDown)
				// Dropping without CASCADE proves no partition trigger was left behind.
				execEdgeSchemaSQL(t, ctx, tx, "drop function delete_node_edges()")
			})
		}
	}
}
