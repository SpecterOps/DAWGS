//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func drainMerge(tx graph.Transaction, query string, params map[string]any) error {
	result := tx.Query(query, params)
	defer result.Close()
	for result.Next() {
	}
	return result.Error()
}

func mergeCount(t *testing.T, tx graph.Transaction, query string) int64 {
	t.Helper()
	result := tx.Query(query, nil)
	defer result.Close()
	require.True(t, result.Next(), "%v", result.Error())
	var count int64
	require.NoError(t, result.Scan(&count))
	require.False(t, result.Next())
	require.NoError(t, result.Error())
	return count
}

// Shared assertions inspect stored state in subsequent database commands.
// The MVP deliberately does not require visibility across clauses of one query.
func TestMergePersistedState(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeNode")}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeEdge")}})
	for _, optimized := range []bool{true, false} {
		previous := pg.SetOptimizedTranslation(optimized)
		t.Run(map[bool]string{true: "optimized", false: "baseline"}[optimized], func(t *testing.T) {
			for _, tail := range []string{"", " RETURN n LIMIT 0", " RETURN n"} {
				t.Run(tail, func(t *testing.T) {
					require.NoError(t, session.WithRollbackFixture(t, &opengraph.Graph{}, true, func(tx graph.Transaction, _ opengraph.IDMap) error {
						require.NoError(t, drainMerge(tx, `MERGE (n:MergeNode {name:'a'}) ON CREATE SET n.score=1`+tail, nil))
						require.EqualValues(t, 1, mergeCount(t, tx, `MATCH (n:MergeNode {name:'a',score:1}) RETURN count(n)`))
						require.NoError(t, drainMerge(tx, `MERGE (n:MergeNode {name:'a'}) ON MATCH SET n.score=2`+tail, nil))
						require.EqualValues(t, 1, mergeCount(t, tx, `MATCH (n:MergeNode {name:'a',score:2}) RETURN count(n)`))
						require.NoError(t, drainMerge(tx, `UNWIND [] AS name MERGE (n:MergeNode {name:name}) RETURN n`, nil))
						require.EqualValues(t, 1, mergeCount(t, tx, `MATCH (n:MergeNode) RETURN count(n)`))
						return nil
					}))
				})
			}
			t.Run("whole pattern creation", func(t *testing.T) {
				require.NoError(t, session.WithRollbackFixture(t, &opengraph.Graph{}, true, func(tx graph.Transaction, _ opengraph.IDMap) error {
					require.NoError(t, drainMerge(tx, `CREATE (:MergeNode {name:'a'})`, nil))
					require.NoError(t, drainMerge(tx, `MERGE p=(a:MergeNode {name:'a'})-[:MergeEdge]->(b:MergeNode {name:'b'}) RETURN p`, nil))
					require.EqualValues(t, 2, mergeCount(t, tx, `MATCH (n:MergeNode {name:'a'}) RETURN count(n)`))
					require.EqualValues(t, 1, mergeCount(t, tx, `MATCH ()-[r:MergeEdge]->() RETURN count(r)`))
					return nil
				}))
			})
		})
		pg.SetOptimizedTranslation(previous)
	}
}

func TestMergeFailureRollsBack(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeRollback")}})
	err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		if err := drainMerge(tx, `MERGE (:MergeRollback {name:'rollback'})`, nil); err != nil {
			return err
		}
		return drainMerge(tx, `MERGE (:MergeRollback {name:null})`, nil)
	})
	require.Error(t, err)
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.Zero(t, mergeCount(t, tx, `MATCH (n:MergeRollback) RETURN count(n)`))
		return nil
	}))
}
