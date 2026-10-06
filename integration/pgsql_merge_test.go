//go:build manual_integration

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLMergeMaterializedStringValues(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeMaterialized")}})
	driver := session.DB.(*pg.Driver)
	graphSchema, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, mode := range []translate.OptimizerMode{translate.OptimizerEnabled, translate.OptimizerDisabled} {
		for _, query := range []string{
			`MERGE (n:MergeMaterialized {name:'a'}) ON CREATE SET n.status='created' ON MATCH SET n.status='matched' RETURN 1`,
			`MERGE (n:MergeMaterialized {name:$name}) ON CREATE SET n.status=$created ON MATCH SET n.status=$matched RETURN 1`,
		} {
			t.Run(string(mode)+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				translation, err := translate.TranslateWithOptions(session.Ctx, parsed, driver.KindMapper(), map[string]any{"name": "a", "created": "created", "matched": "matched"}, graphSchema.ID, translate.Options{OptimizerMode: mode})
				require.NoError(t, err)
				sql, err := format.Statement(translation.Statement, format.NewOutputBuilder().WithMaterializedParameters(translation.Parameters))
				require.NoError(t, err)
				for _, expected := range []string{"created", "matched"} {
					require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
						if expected == "created" {
							if err := tx.Nodes().Delete(); err != nil {
								return err
							}
						}
						result := tx.Raw(sql.Statement, nil)
						defer result.Close()
						rows := 0
						for result.Next() {
							rows++
						}
						if err := result.Error(); err != nil {
							return err
						}
						require.Equal(t, 1, rows)
						return nil
					}))
					var name, status string
					require.NoError(t, session.PGPool.QueryRow(session.Ctx, `select properties->>'name',properties->>'status' from node where graph_id=$1`, graphSchema.ID).Scan(&name, &status))
					require.Equal(t, "a", name)
					require.Equal(t, expected, status)
				}
			})
		}
	}
}

func TestPostgreSQLMergeCacheAndUnchangedRows(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeCacheNode")}})
	driver := session.DB.(*pg.Driver)
	previous := pg.SetOptimizedTranslation(true)
	defer pg.SetOptimizedTranslation(previous)
	query := `MERGE (n:MergeCacheNode {name:$name}) RETURN n`
	before := driver.TranslationCacheStats()
	for _, name := range []string{"first", "second", "first"} {
		require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error { return drainMerge(tx, query, map[string]any{"name": name}) }))
	}
	after := driver.TranslationCacheStats()
	require.GreaterOrEqual(t, after.Hits, before.Hits+2)
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.EqualValues(t, 2, mergeCount(t, tx, `MATCH (n:MergeCacheNode) RETURN count(n)`))
		return nil
	}))
	// xmin and ctid remain unchanged: an unchanged match does not issue UPDATE.
	var original, current string
	graphSchema, _ := driver.DefaultGraph()
	require.NoError(t, session.PGPool.QueryRow(session.Ctx, `select xmin::text || ':' || ctid::text from node where graph_id=$1 and properties->>'name'='first'`, graphSchema.ID).Scan(&original))
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error { return drainMerge(tx, query, map[string]any{"name": "first"}) }))
	require.NoError(t, session.PGPool.QueryRow(session.Ctx, `select xmin::text || ':' || ctid::text from node where graph_id=$1 and properties->>'name'='first'`, graphSchema.ID).Scan(&current))
	require.Equal(t, original, current)
	// Warm compilation must validate values, rather than trusting the first map.
	mapQuery := `MERGE (n:MergeCacheNode $props) RETURN n`
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, mapQuery, map[string]any{"props": map[string]any{"name": "map"}})
	}))
	require.Error(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, mapQuery, map[string]any{"props": map[string]any{"name": nil}})
	}))
}

func TestPostgreSQLMergeRelationshipStorageConflict(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeStorageNode")}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeStorageEdge")}})
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `CREATE (a:MergeStorageNode {name:'a'})-[:MergeStorageEdge {score:1}]->(b:MergeStorageNode {name:'b'})`, nil)
	}))
	err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `MATCH (a:MergeStorageNode {name:'a'}),(b:MergeStorageNode {name:'b'}) MERGE (a)-[r:MergeStorageEdge {score:2}]->(b) ON CREATE SET a.changed=true RETURN r`, nil)
	})
	require.ErrorContains(t, err, "23505")
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.EqualValues(t, 1, mergeCount(t, tx, `MATCH ()-[r:MergeStorageEdge {score:1}]->() RETURN count(r)`))
		require.Zero(t, mergeCount(t, tx, `MATCH (n:MergeStorageNode {changed:true}) RETURN count(n)`))
		return nil
	}))
}

func TestPostgreSQLMergeUsesPropertyIndex(t *testing.T) {
	db := setupIndexedPostgresDB(t, "integration_merge_index_test", []graph.Index{{Field: "name", Type: graph.BTreeIndex}})
	loadPropertyIndexFixture(t, db)
	analyzeIndexedNodePartition(t, db)
	assertTranslatedPlanUsesIndex(t, db, `MERGE (n:IndexedNode {name:$name}) RETURN n`, map[string]any{"name": "indexed-name"}, db.nodeIndexes["name"])
}

func TestPostgreSQLMergeGraphIsolation(t *testing.T) {
	kind := graph.StringKind("MergeIsolationNode")
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{kind}})
	other := graph.Graph{Name: "merge_isolation_other", Nodes: graph.Kinds{kind}}
	require.NoError(t, session.DB.AssertSchema(session.Ctx, graph.Schema{Graphs: []graph.Graph{other}}))
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error { return tx.WithGraph(other).Nodes().Delete() }))
	t.Cleanup(func() {
		_ = session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error { return tx.WithGraph(other).Nodes().Delete() })
	})
	for _, target := range []bool{true, false} {
		require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
			if target {
				tx = tx.WithGraph(other)
			}
			return drainMerge(tx, `MERGE (:MergeIsolationNode {name:'same'})`, nil)
		}))
	}
	// Inspect storage directly so unrelated read optimizations do not hide the
	// graph boundary being exercised by the MERGE compiler.
	defaultGraph, _ := session.DB.(*pg.Driver).DefaultGraph()
	for _, graphName := range []string{other.Name, defaultGraph.Name} {
		var count int64
		require.NoError(t, session.PGPool.QueryRow(session.Ctx, `select count(*) from node n join graph g on g.id=n.graph_id where g.name=$1 and n.properties->>'name'='same'`, graphName).Scan(&count))
		require.EqualValues(t, 1, count)
	}

}

func TestPostgreSQLMergeConcurrentRelationshipConflict(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeConcurrentNode")}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeConcurrentEdge")}})
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `CREATE (:MergeConcurrentNode {name:'a'}),(:MergeConcurrentNode {name:'b'})`, nil)
	}))
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			results <- session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				return drainMerge(tx, `MATCH (a:MergeConcurrentNode {name:'a'}),(b:MergeConcurrentNode {name:'b'}) MERGE (a)-[r:MergeConcurrentEdge]->(b) RETURN r`, nil)
			})
		}()
	}
	close(start)
	successes := 0
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			successes++
		} else {
			require.ErrorContains(t, err, "23505")
		}
	}
	require.GreaterOrEqual(t, successes, 1)
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.EqualValues(t, 1, mergeCount(t, tx, `MATCH ()-[r:MergeConcurrentEdge]->() RETURN count(r)`))
		return nil
	}))
}

func TestPostgreSQLMergeRejectsUnorderedCandidates(t *testing.T) {
	kind := graph.StringKind("MergeOrderedNode")
	for _, test := range []struct {
		name, query, message string
	}{
		{"repeated bound updates", `MATCH (n:MergeOrderedNode {name:'existing'}) UNWIND [1,2] AS value MERGE (n) ON MATCH SET n.score=value RETURN n`, "repeated writes to the same target"},
		{"repeated updates", `UNWIND [1,2] AS value MERGE (n:MergeOrderedNode {name:'existing'}) ON MATCH SET n.score=value RETURN n`, "repeated writes to the same target"},
		{"repeated absent inputs", `UNWIND [1,2] AS value MERGE (n:MergeOrderedNode {name:'absent'}) ON CREATE SET n.score=value RETURN n`, "overlapping absent node inputs"},
		{"distinct bindings", `MERGE (a:MergeOrderedNode {name:'existing'})-[:MergeOrderedEdge]->(b:MergeOrderedNode {name:'existing'}) ON MATCH SET a.score=1,b.score=2 RETURN a,b`, "repeated writes to the same target"},
		{"repeated updates without return", `UNWIND [1,2] AS value MERGE (n:MergeOrderedNode {name:'existing'}) ON MATCH SET n.score=value`, "repeated writes to the same target"},
		{"repeated absent inputs with limit zero", `UNWIND [1,2] AS value MERGE (n:MergeOrderedNode {name:'absent'}) RETURN n LIMIT 0`, "overlapping absent node inputs"},
		{"creation changes matching key", `UNWIND ['a','b'] AS name MERGE (n:MergeOrderedNode {name:name}) ON CREATE SET n.name='b' RETURN n`, "overlapping absent node inputs"},
		{"multiple absent complete patterns", `UNWIND ['a','b'] AS name MERGE (:MergeOrderedNode {name:name})-[:MergeOrderedEdge]->(:MergeOrderedNode) RETURN name`, "multiple absent complete-pattern inputs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{kind}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeOrderedEdge")}})
			require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				return drainMerge(tx, `CREATE (n:MergeOrderedNode {name:'existing',score:0})-[:MergeOrderedEdge]->(n)`, nil)
			}))
			err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				return drainMerge(tx, test.query, nil)
			})
			require.ErrorContains(t, err, test.message)
			require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
				require.EqualValues(t, 1, mergeCount(t, tx, `MATCH (n:MergeOrderedNode) RETURN count(n)`))
				require.EqualValues(t, 1, mergeCount(t, tx, `MATCH (n:MergeOrderedNode {name:'existing',score:0}) RETURN count(n)`))
				return nil
			}))
		})
	}
}

func TestPostgreSQLMergeValidationDemand(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeDemand"), graph.StringKind("MergeDemandSource")}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeDemandEdge")}})
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		for idx := 0; idx < 513; idx++ {
			props := graph.NewProperties()
			if idx < 512 {
				props.Set("name", idx)
			}
			if _, err := tx.CreateNode(props, graph.StringKind("MergeDemandSource")); err != nil {
				return err
			}
		}
		return nil
	}))
	for _, optimized := range []bool{true, false} {
		previous := pg.SetOptimizedTranslation(optimized)
		for _, tail := range []string{"", " RETURN n LIMIT 0", " RETURN 1 LIMIT 0", " RETURN count(*)"} {
			for _, query := range []string{
				`MATCH (value:MergeDemandSource) WITH value ORDER BY id(value) MERGE (n:MergeDemand {name:value.name})` + tail,
				`MERGE (n:MergeDemand {name:'valid'})-[:MergeDemandEdge {value:null}]->(:MergeDemand {name:'other'})` + tail,
				`MERGE (n:MergeDemand $props)` + tail,
			} {
				t.Run(query, func(t *testing.T) {
					values := make([]int64, 513)
					for idx := range values {
						values[idx] = int64(idx)
					}
					err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
						return drainMerge(tx, query, map[string]any{"values": values, "props": map[string]any{"name": nil}})
					})
					require.ErrorContains(t, err, "null property value")
					require.ErrorContains(t, err, "22023")
				})
			}
		}
		for _, props := range []any{nil, "not a map", []any{1, 2}} {
			err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				return drainMerge(tx, `MERGE (n:MergeDemand $props) RETURN 1 LIMIT 0`, map[string]any{"props": props})
			})
			require.ErrorContains(t, err, "non-null map")
		}
		require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
			return drainMerge(tx, `UNWIND [] AS value MERGE (n:MergeDemand {name:null}) RETURN n LIMIT 0`, nil)
		}))
		pg.SetOptimizedTranslation(previous)
	}
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.Zero(t, mergeCount(t, tx, `MATCH (n:MergeDemand) RETURN count(n)`))
		return nil
	}))
}

func TestPostgreSQLMergeKeyOverlap(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeKeys")}})
	for _, test := range []struct {
		query    string
		params   map[string]any
		conflict bool
	}{
		{`UNWIND ['a','a'] AS value MERGE (n:MergeKeys {name:value}) ON CREATE SET n.name='away' RETURN n`, nil, false},
		{`UNWIND ['a','b'] AS value MERGE (n:MergeKeys {name:value}) ON CREATE SET n.name='a' RETURN n`, nil, true},
		{`UNWIND ['a','b'] AS value MERGE (n:MergeKeys {name:value}) ON CREATE SET n.name='b' RETURN n`, nil, true},
		{`UNWIND ['a','a'] AS value MERGE (n:MergeKeys {name:value}) ON CREATE SET n.name=null RETURN n`, nil, false},
		{`UNWIND [1,2] AS value MERGE (n:MergeKeys {}) RETURN n`, nil, true},
		{`UNWIND [1,2] AS value MERGE (n:MergeKeys {name:'same',score:value}) RETURN n`, nil, false},
		{`UNWIND ['a','b'] AS value MERGE (n:MergeKeys $props) ON CREATE SET n.name=value RETURN n`, map[string]any{"props": map[string]any{"name": "a"}}, true},
		{`UNWIND ['a','b'] AS value MERGE (n:MergeKeys $props) ON CREATE SET n.name=value RETURN n`, map[string]any{"props": map[string]any{"name": "away", "values": []any{1, 2}}}, false},
		{`UNWIND ['a','b'] AS value MERGE (n:MergeKeys $props) ON CREATE SET n.name=value RETURN n`, map[string]any{"props": map[string]any{}}, true},
	} {
		t.Run(test.query, func(t *testing.T) {
			err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
				if err := tx.Nodes().Delete(); err != nil {
					return err
				}
				return drainMerge(tx, test.query, test.params)
			})
			if test.conflict {
				require.ErrorContains(t, err, "overlapping absent node inputs")
				require.ErrorContains(t, err, "22023")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPostgreSQLMergeReadOnlyBindings(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeReadOnly")}, ExtraEdgeKinds: graph.Kinds{graph.StringKind("MergeReadOnlyEdge")}})
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `CREATE (:MergeReadOnly {name:'a'}),(:MergeReadOnly {name:'b'})`, nil)
	}))
	graphSchema, _ := session.DB.(*pg.Driver).DefaultGraph()
	versions := func() []string {
		rows, err := session.PGPool.Query(session.Ctx, `select xmin::text || ':' || ctid::text from node where graph_id=$1 order by id`, graphSchema.ID)
		require.NoError(t, err)
		defer rows.Close()
		var result []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			result = append(result, value)
		}
		require.NoError(t, rows.Err())
		return result
	}
	before := versions()
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `MATCH (a:MergeReadOnly {name:'a'}),(b:MergeReadOnly {name:'b'}) MERGE (a)-[r:MergeReadOnlyEdge]->(b) RETURN r`, nil)
	}))
	require.Equal(t, before, versions())
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, `MATCH (a:MergeReadOnly) UNWIND [1,2] AS input MERGE (a) RETURN a`, nil)
	}))
	require.Equal(t, before, versions())
}

func TestPostgreSQLMergeDynamicCacheShapes(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeDynamicCache")}})
	previous := pg.SetOptimizedTranslation(true)
	defer pg.SetOptimizedTranslation(previous)
	driver := session.DB.(*pg.Driver)
	query := `MERGE (n:MergeDynamicCache $props) RETURN n`
	before := driver.TranslationCacheStats()
	for _, props := range []map[string]any{{"name": "first", "active": true}, {"score": 1}, {"values": []any{1, 2}}, {"values": []any{2, 1}}, {"values": []any{1, 1}}, {"values": []any{1}}, {"nested": map[string]any{"value": nil}}, {}} {
		require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error { return drainMerge(tx, query, map[string]any{"props": props}) }))
	}
	after := driver.TranslationCacheStats()
	require.GreaterOrEqual(t, after.Hits, before.Hits+7)
	err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return drainMerge(tx, query, map[string]any{"props": map[string]any{"score": nil}})
	})
	require.ErrorContains(t, err, "22023")
	require.ErrorContains(t, err, "null property value")
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		require.EqualValues(t, 7, mergeCount(t, tx, `MATCH (n:MergeDynamicCache) RETURN count(n)`))
		return nil
	}))
}

func TestPostgreSQLMergeInputEvaluationCount(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("MergeEvaluation")}})
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		schema := fmt.Sprintf("merge_evaluation_%d", time.Now().UnixNano())
		// Instrumentation is removed before this transaction commits.
		statements := []string{
			"create schema " + schema,
			"set local search_path to " + schema + ", public",
			`create temporary table merge_evaluations (fixed int,dynamic int)`,
			`insert into merge_evaluations values (0,0)`,
			`create function ` + schema + `.cypher_merge_value(value jsonb) returns jsonb language plpgsql volatile as $$begin update merge_evaluations set fixed=fixed+1; return public.cypher_merge_value(value); end$$`,
			`create function ` + schema + `.cypher_merge_properties(value jsonb) returns jsonb language plpgsql volatile as $$begin update merge_evaluations set dynamic=dynamic+1; return public.cypher_merge_properties(value); end$$`,
		}
		for _, statement := range statements {
			result := tx.Raw(statement, nil)
			result.Close()
			if err := result.Error(); err != nil {
				return err
			}
		}
		if err := drainMerge(tx, `UNWIND ['a','b'] AS name MERGE (n:MergeEvaluation {name:name,score:1}) RETURN n LIMIT 0`, nil); err != nil {
			return err
		}
		if err := drainMerge(tx, `UNWIND [1,2] AS input MERGE (n:MergeEvaluation $props) RETURN n LIMIT 0`, map[string]any{"props": map[string]any{"name": "a"}}); err != nil {
			return err
		}
		result := tx.Raw("select fixed,dynamic from merge_evaluations", nil)
		defer result.Close()
		require.True(t, result.Next(), "%v", result.Error())
		var fixed, dynamic int64
		require.NoError(t, result.Scan(&fixed, &dynamic))
		require.EqualValues(t, 4, fixed)
		require.EqualValues(t, 2, dynamic)
		result.Close()
		cleanup := tx.Raw("drop schema "+schema+" cascade", nil)
		cleanup.Close()
		return cleanup.Error()
	}))
}
