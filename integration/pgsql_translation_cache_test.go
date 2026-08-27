//go:build manual_integration

package integration

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLFetchStartNodesUsesBuilderCompilationCache(t *testing.T) {
	previous := pg.SetOptimizedTranslation(true)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	userKind := graph.StringKind("CacheUser")
	groupKind := graph.StringKind("CacheGroup")
	memberKind := graph.StringKind("CacheMember")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{userKind, groupKind},
		ExtraEdgeKinds:       graph.Kinds{memberKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	var (
		first  *graph.Node
		second *graph.Node
	)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		var err error
		if first, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "first"}), userKind); err != nil {
			return err
		}
		if second, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "second"}), userKind); err != nil {
			return err
		}
		target, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "target"}), groupKind)
		if err != nil {
			return err
		}
		if _, err = tx.CreateRelationshipByIDs(first.ID, target.ID, memberKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(second.ID, target.ID, memberKind, graph.NewProperties())
		return err
	}))

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		firstNodes, err := ops.FetchStartNodes(tx.Relationships().Filter(query.InIDs(query.Start(), first.ID)))
		if err != nil {
			return err
		}
		secondNodes, err := ops.FetchStartNodes(tx.Relationships().Filter(query.InIDs(query.Start(), second.ID)))
		if err != nil {
			return err
		}
		if _, found := firstNodes[first.ID]; !found {
			t.Fatalf("first result omitted node %d", first.ID)
		}
		if _, found := secondNodes[second.ID]; !found {
			t.Fatalf("second result omitted node %d", second.ID)
		}
		return nil
	}))

	after := driver.TranslationCacheStats()
	require.Equal(t, before.Misses+1, after.Misses)
	require.GreaterOrEqual(t, after.Hits, before.Hits+1)
}

func TestPostgreSQLFetchStartNodesUnoptimizedBypassesCache(t *testing.T) {
	previous := pg.SetOptimizedTranslation(false)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	userKind := graph.StringKind("BaselineCacheUser")
	groupKind := graph.StringKind("BaselineCacheGroup")
	memberKind := graph.StringKind("BaselineCacheMember")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{userKind, groupKind},
		ExtraEdgeKinds:       graph.Kinds{memberKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	var (
		first  *graph.Node
		second *graph.Node
	)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		var err error
		if first, err = tx.CreateNode(graph.NewProperties(), userKind); err != nil {
			return err
		}
		if second, err = tx.CreateNode(graph.NewProperties(), userKind); err != nil {
			return err
		}
		target, err := tx.CreateNode(graph.NewProperties(), groupKind)
		if err != nil {
			return err
		}
		if _, err = tx.CreateRelationshipByIDs(first.ID, target.ID, memberKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(second.ID, target.ID, memberKind, graph.NewProperties())
		return err
	}))

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		if _, err := ops.FetchStartNodes(tx.Relationships().Filter(query.InIDs(query.Start(), first.ID))); err != nil {
			return err
		}
		_, err := ops.FetchStartNodes(tx.Relationships().Filter(query.InIDs(query.Start(), second.ID)))
		return err
	}))

	after := driver.TranslationCacheStats()
	require.Equal(t, before.Hits, after.Hits)
	require.Equal(t, before.Misses, after.Misses)
	require.Equal(t, before.Insertions, after.Insertions)
	require.Equal(t, before.Bypasses+2, after.Bypasses)
	require.Equal(t, before.UnoptimizedCompilations+2, after.UnoptimizedCompilations)
}

func TestPostgreSQLRawCypherQueryRebindsCachedParameters(t *testing.T) {
	previous := pg.SetOptimizedTranslation(true)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	nodeKind := graph.StringKind("RawCypherCacheNode")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	var (
		first  *graph.Node
		second *graph.Node
	)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		var err error
		if first, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "first"}), nodeKind); err != nil {
			return err
		}
		second, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "second"}), nodeKind)
		return err
	}))

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		firstID, err := rawCypherNodeID(tx, "first")
		if err != nil {
			return err
		}
		secondID, err := rawCypherNodeID(tx, "second")
		if err != nil {
			return err
		}
		if firstID != first.ID || secondID != second.ID {
			return fmt.Errorf("cached raw Cypher query returned incorrect node IDs: %d, %d", firstID, secondID)
		}
		return nil
	}))

	after := driver.TranslationCacheStats()
	require.Equal(t, before.Misses+1, after.Misses)
	require.GreaterOrEqual(t, after.Hits, before.Hits+1)
}

func TestPostgreSQLRawCypherQueryUnoptimizedBypassesCache(t *testing.T) {
	previous := pg.SetOptimizedTranslation(false)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	nodeKind := graph.StringKind("RawCypherCacheBaselineNode")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		for _, name := range []string{"first", "second"} {
			result := tx.Query("RETURN $name", map[string]any{"name": name})
			if !result.Next() {
				result.Close()
				return result.Error()
			}
			result.Close()
		}
		return nil
	}))

	after := driver.TranslationCacheStats()
	require.Equal(t, before.Hits, after.Hits)
	require.Equal(t, before.Misses, after.Misses)
	require.Equal(t, before.Bypasses+2, after.Bypasses)
	require.Equal(t, before.UnoptimizedCompilations+2, after.UnoptimizedCompilations)
}

func rawCypherNodeID(tx graph.Transaction, name string) (graph.ID, error) {
	result := tx.Query("MATCH (n:RawCypherCacheNode) WHERE n.name = $name RETURN n", map[string]any{"name": name})
	defer result.Close()
	if !result.Next() {
		return 0, result.Error()
	}

	var node graph.Node
	if err := result.Scan(&node); err != nil {
		return 0, err
	}
	return node.ID, result.Error()
}

func TestPostgreSQLNodeUpdateRebindsCachedBuilderParameters(t *testing.T) {
	previous := pg.SetOptimizedTranslation(true)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	nodeKind := graph.StringKind("CacheUpdateNode")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	var (
		first  *graph.Node
		second *graph.Node
	)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		var err error
		if first, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "before-first"}), nodeKind); err != nil {
			return err
		}
		second, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "before-second"}), nodeKind)
		return err
	}))

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		firstProperties := graph.NewProperties().Set("name", "after-first")
		if err := tx.Nodes().Filter(query.InIDs(query.Node(), first.ID)).Update(firstProperties); err != nil {
			return err
		}

		secondProperties := graph.NewProperties().Set("name", "after-second")
		return tx.Nodes().Filter(query.InIDs(query.Node(), second.ID)).Update(secondProperties)
	}))
	after := driver.TranslationCacheStats()
	require.Equal(t, before.Misses+1, after.Misses)
	require.GreaterOrEqual(t, after.Hits, before.Hits+1)

	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		updatedFirst, err := tx.Nodes().Filter(query.InIDs(query.Node(), first.ID)).First()
		if err != nil {
			return err
		}
		updatedSecond, err := tx.Nodes().Filter(query.InIDs(query.Node(), second.ID)).First()
		if err != nil {
			return err
		}
		firstName, err := updatedFirst.Properties.Get("name").String()
		if err != nil {
			return err
		}
		secondName, err := updatedSecond.Properties.Get("name").String()
		if err != nil {
			return err
		}
		if firstName != "after-first" || secondName != "after-second" {
			return fmt.Errorf("cached update bound wrong values: got %q and %q", firstName, secondName)
		}
		return nil
	}))
}

func TestPostgreSQLRelationshipUpdateRebindsCachedBuilderParameters(t *testing.T) {
	previous := pg.SetOptimizedTranslation(true)
	t.Cleanup(func() {
		pg.SetOptimizedTranslation(previous)
	})

	nodeKind := graph.StringKind("CacheUpdateRelationshipNode")
	relationshipKind := graph.StringKind("CacheUpdateRelationship")
	session := Open(t, Options{
		RequireDriver:        pg.DriverName,
		SkipIfNoConnection:   true,
		SkipIfDriverMismatch: true,
		CleanupMode:          CleanupGraph,
		ExtraNodeKinds:       graph.Kinds{nodeKind},
		ExtraEdgeKinds:       graph.Kinds{relationshipKind},
	})
	driver, ok := session.DB.(*pg.Driver)
	require.True(t, ok)

	var (
		first  *graph.Relationship
		second *graph.Relationship
	)
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		start, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		end, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		secondEnd, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		if first, err = tx.CreateRelationshipByIDs(start.ID, end.ID, relationshipKind, graph.AsProperties(map[string]any{"name": "before-first"})); err != nil {
			return err
		}
		second, err = tx.CreateRelationshipByIDs(start.ID, secondEnd.ID, relationshipKind, graph.AsProperties(map[string]any{"name": "before-second"}))
		return err
	}))

	before := driver.TranslationCacheStats()
	require.NoError(t, session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		if err := tx.Relationships().Filter(query.InIDs(query.Relationship(), first.ID)).Update(graph.NewProperties().Set("name", "after-first")); err != nil {
			return err
		}
		return tx.Relationships().Filter(query.InIDs(query.Relationship(), second.ID)).Update(graph.NewProperties().Set("name", "after-second"))
	}))
	after := driver.TranslationCacheStats()
	require.Equal(t, before.Misses+1, after.Misses)
	require.GreaterOrEqual(t, after.Hits, before.Hits+1)

	require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
		updatedFirst, err := tx.Relationships().Filter(query.InIDs(query.Relationship(), first.ID)).First()
		if err != nil {
			return err
		}
		updatedSecond, err := tx.Relationships().Filter(query.InIDs(query.Relationship(), second.ID)).First()
		if err != nil {
			return err
		}
		firstName, err := updatedFirst.Properties.Get("name").String()
		if err != nil {
			return err
		}
		secondName, err := updatedSecond.Properties.Get("name").String()
		if err != nil {
			return err
		}
		if firstName != "after-first" || secondName != "after-second" {
			return fmt.Errorf("cached relationship update bound wrong values: got %q and %q", firstName, secondName)
		}
		return nil
	}))
}
