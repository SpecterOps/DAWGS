//go:build manual_integration

package pg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func seedBoundedShortestChain(t *testing.T, tx graph.Transaction, depth int) ([]*graph.Node, []*graph.Relationship) {
	t.Helper()
	nodes := make([]*graph.Node, depth+1)
	edges := make([]*graph.Relationship, depth)
	for i := range nodes {
		node, err := tx.CreateNode(graph.NewProperties().Set("position", i), graph.StringKind("DAWGS065BoundedNode"))
		require.NoError(t, err)
		nodes[i] = node
		if i > 0 {
			edges[i-1], err = tx.CreateRelationshipByIDs(nodes[i-1].ID, node.ID, graph.StringKind("DAWGS065BoundedStep"), graph.NewProperties())
			require.NoError(t, err)
		}
	}
	return nodes, edges
}

func requireBoundedShortestWitness(t *testing.T, result graph.Result, nodes []*graph.Node, edges []*graph.Relationship) {
	t.Helper()
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var path graph.Path
	require.NoError(t, result.Scan(&path))
	require.Len(t, path.Nodes, len(nodes))
	require.Len(t, path.Edges, len(edges))
	for i, node := range nodes {
		require.Equal(t, node.ID, path.Nodes[i].ID, "path node %d", i)
	}
	for i, edge := range edges {
		require.Equal(t, edge.ID, path.Edges[i].ID, "path edge %d", i)
	}
	require.False(t, result.Next())
	require.NoError(t, result.Error())
}

// The declared bound selects the executor even when the actual path is one edge.
func TestBoundedShortestPathColdConnection(t *testing.T) {
	for _, test := range []struct{ bound, depth int }{{64, 1}, {65, 1}, {128, 1}, {128, 128}} {
		for _, projection := range []string{"length(p)", "p"} {
			t.Run(fmt.Sprintf("bound=%d/depth=%d/%s", test.bound, test.depth, projection), func(t *testing.T) {
				driver := newDAWGS065Driver(t, 1)
				setupDAWGS065Schema(t, driver)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				rollback := fmt.Errorf("rollback cold shortest fixture")
				require.ErrorIs(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					nodes, edges := seedBoundedShortestChain(t, tx, test.depth)
					result := tx.Query(fmt.Sprintf("MATCH p=shortestPath((s:DAWGS065BoundedNode)-[:DAWGS065BoundedStep*1..%d]->(e:DAWGS065BoundedNode)) WHERE id(s)=$start_id AND id(e)=$end_id RETURN %s", test.bound, projection), map[string]any{"start_id": nodes[0].ID, "end_id": nodes[test.depth].ID})
					if projection == "p" {
						requireBoundedShortestWitness(t, result, nodes, edges)
					} else {
						require.True(t, result.Next(), "%v", result.Error())
						require.EqualValues(t, test.depth, result.Values()[0])
						require.False(t, result.Next())
						result.Close()
						require.NoError(t, result.Error())
					}
					return rollback
				}), rollback)
			})
		}
	}
}

// A legitimate text-filter call leaves terminal 17 in the session workspace.
// Subsequent singleton calls must use their current endpoints across leases.
func TestBoundedShortestPathReusedConnection(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var nodes []*graph.Node
	var edges []*graph.Relationship
	require.NoError(t, driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodes, edges = seedBoundedShortestChain(t, tx, 128)
		return nil
	}))
	require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH p=shortestPath((s:DAWGS065BoundedNode)-[:DAWGS065BoundedStep*1..128]->(e:DAWGS065BoundedNode)) WHERE id(s)=$start_id AND e.position=17 RETURN p", map[string]any{"start_id": nodes[0].ID})
		requireBoundedShortestWitness(t, result, nodes[:18], edges[:17])
		return nil
	}))
	query := "MATCH p=shortestPath((s:DAWGS065BoundedNode)-[:DAWGS065BoundedStep*1..128]->(e:DAWGS065BoundedNode)) WHERE id(s)=$start_id AND id(e)=$end_id RETURN p"
	for _, pair := range [][2]int{{0, 128}, {0, 17}, {64, 128}, {0, 128}} {
		require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Query(query, map[string]any{"start_id": nodes[pair[0]].ID, "end_id": nodes[pair[1]].ID})
			requireBoundedShortestWitness(t, result, nodes[pair[0]:pair[1]+1], edges[pair[0]:pair[1]])
			return nil
		}))
	}
}

// Deliberately malformed fragments exercise the SQL safety boundary separately
// from the compiler. Invalid successes must neither escape nor stop the BFS.
func TestBoundedShortestPathHarnessEndpointGuards(t *testing.T) {
	driver := newDAWGS065Driver(t, 1)
	setupDAWGS065Schema(t, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	front := func(rows string) string {
		return "insert into next_front (root_id,next_id,depth,satisfied,is_cycle,path) values " + rows
	}
	empty := "insert into next_front select 1::int8,2::int8,1::int4,false,false,array[]::int8[] where false"
	for _, test := range []struct {
		name, forward, forwardRecursive, backward, backwardRecursive string
	}{
		{"forward", front("(1,17,1,true,false,array[101]::int8[])"), front("(1,128,2,true,false,array[101,102]::int8[])"), front("(128,90,1,false,false,array[201]::int8[]),(128,91,1,false,false,array[202]::int8[])"), empty},
		{"backward", front("(1,10,1,false,false,array[101]::int8[]),(1,11,1,false,false,array[102]::int8[])"), empty, front("(128,17,1,true,false,array[201]::int8[])"), front("(128,1,2,true,false,array[202,201]::int8[])")},
		{"midpoint", front("(99,10,1,false,false,array[101]::int8[]),(1,11,1,false,false,array[102]::int8[])"), front("(1,128,2,true,false,array[102,103]::int8[])"), front("(128,10,1,false,false,array[201]::int8[]),(77,11,1,false,false,array[202]::int8[])"), empty},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result := tx.Raw("select root_id,next_id,depth from bidirectional_sp_harness(@fp::text,@fr::text,@bp::text,@br::text,4,array[1]::int8[],array[128]::int8[])", map[string]any{"fp": test.forward, "fr": test.forwardRecursive, "bp": test.backward, "br": test.backwardRecursive})
				defer result.Close()
				require.True(t, result.Next(), "%v", result.Error())
				var root, terminal int64
				var depth int
				require.NoError(t, result.Scan(&root, &terminal, &depth))
				require.EqualValues(t, 1, root)
				require.EqualValues(t, 128, terminal)
				require.Equal(t, 2, depth)
				require.False(t, result.Next())
				return result.Error()
			}))
		})
	}
}
