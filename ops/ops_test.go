//go:build integration

package ops

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

type TestSuite struct {
	db  graph.Database
	ctx context.Context
}

func dbSetup(t *testing.T) *TestSuite {
	t.Helper()

	connStr := os.Getenv("Driver_CONNECTION_STRING")
	if connStr == "" {
		t.Fatal("No driver connection string specified. Set the Driver_CONNECTION_STRING environment variable.")
	}

	u, err := url.Parse(connStr)
	require.NoError(t, err)

	var db graph.Database
	var testSchema = graph.Schema{
		Graphs: []graph.Graph{{
			Name:  "ops_test",
			Nodes: graph.Kinds{graph.StringKind("User"), graph.StringKind("Group")},
			Edges: graph.Kinds{graph.StringKind("MemberOf")},
		}},
		DefaultGraph: graph.Graph{Name: "ops_test"},
	}

	switch strings.ToLower(u.Scheme) {
	case "postgres", "postgresql":
		// Create the connection pool
		pgxPool, poolErr := pg.NewPool(connStr)
		require.NoError(t, poolErr)

		// Open the database
		db, err = dawgs.Open(context.Background(), pg.DriverName, dawgs.Config{
			GraphQueryMemoryLimit: size.Gibibyte,
			Pool:                  pgxPool,
		})
		require.NoError(t, err)

		// Assert the test schema
		err := db.AssertSchema(context.Background(), testSchema)
		require.NoError(t, err)
	case "neo4j", "bolt":
		// The neo4j dawgs driver requires the "neo4j://" scheme; rewrite bolt:// transparently.
		if u.Scheme == "bolt" {
			u.Scheme = "neo4j"
			connStr = u.String()
		}
		// Open the database
		db, err = dawgs.Open(context.Background(), neo4j.DriverName, dawgs.Config{
			GraphQueryMemoryLimit: size.Gibibyte,
			ConnectionString:      connStr,
		})
		require.NoError(t, err)
	default:
		t.Fatalf("Unsupported connection string scheme %q. Use postgres:// or neo4j:// (or bolt://).", u.Scheme)
	}

	t.Cleanup(func() { db.Close(context.Background()) })
	return &TestSuite{
		db:  db,
		ctx: context.Background(),
	}
}

// cleanDB deletes all relationships then all nodes in the test graph.
// Call it at the START of each test so dirty data from a previous failed
// run never pollutes the current one.
func cleanDB(t *testing.T, ctx context.Context, db graph.Database) {
	t.Helper()
	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Relationships().Delete(); err != nil {
			return err
		}
		return tx.Nodes().Delete()
	})
	require.NoError(t, err)
}

func TestDeleteRelationshipsQuery(t *testing.T) {
	testSuite := dbSetup(t)

	cleanDB(t, testSuite.ctx, testSuite.db)
	t.Cleanup(func() { cleanDB(t, testSuite.ctx, testSuite.db) })

	// Seed one User node
	var (
		startNode       *graph.Node
		endNode         *graph.Node
		relationship    *graph.Relationship
		relationshipIDs []graph.ID
	)

	err := testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		startNode, err = tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Alice",
			"objectid": "1",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		if err != nil {
			return err
		}
		endNode, err = tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Bob",
			"objectid": "2",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		if err != nil {
			return err
		}

		relationship, err = tx.CreateRelationshipByIDs(startNode.ID, endNode.ID, graph.StringKind("MemberOf"), graph.NewProperties())
		if err != nil {
			return err
		}
		relationshipIDs = append(relationshipIDs, relationship.ID)
		return nil
	})
	require.NoError(t, err)

	err = testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		for _, id := range relationshipIDs {
			err = DeleteRelationships(tx, id)
			if err != nil {
				return err
			}
		}
		return err
	})
	require.NoError(t, err)

	err = testSuite.db.ReadTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		relationshipIDs, err = FetchRelationshipIDs(tx.Relationships().Filter(query.KindIn(query.Relationship(), graph.StringKind("MemberOf"))))
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(relationshipIDs))
}

func TestFetchRelationships(t *testing.T) {
	testSuite := dbSetup(t)

	cleanDB(t, testSuite.ctx, testSuite.db)
	t.Cleanup(func() { cleanDB(t, testSuite.ctx, testSuite.db) })

	// Seed one User node
	var startNode *graph.Node
	var endNode *graph.Node
	err := testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		startNode, err = tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Alice",
			"objectid": "1",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		if err != nil {
			return err
		}
		endNode, err = tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Bob",
			"objectid": "2",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		if err != nil {
			return err
		}

		_, err = tx.CreateRelationshipByIDs(startNode.ID, endNode.ID, graph.StringKind("MemberOf"), graph.NewProperties())
		return err
	})
	require.NoError(t, err)

	var relationships []*graph.Relationship
	err = testSuite.db.ReadTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		relationships, err = FetchRelationships(tx.Relationships().Filter(query.KindIn(query.Relationship(), graph.StringKind("MemberOf"))))
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(relationships))
}
func TestFetchNodeSet(t *testing.T) {
	testSuite := dbSetup(t)

	cleanDB(t, testSuite.ctx, testSuite.db)
	t.Cleanup(func() { cleanDB(t, testSuite.ctx, testSuite.db) })

	// Seed one User node
	err := testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Alice",
			"objectid": "1",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		return err
	})
	require.NoError(t, err)

	var nodes graph.NodeSet
	err = testSuite.db.ReadTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		nodes, err = FetchNodeSet(tx.Nodes().Filter(query.Equals(query.NodeProperty("name"), "Alice")))
		return err
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(nodes))
	require.Equal(t, "Alice", nodes.Pick().Properties.Get("name").Any())
}

func TestFetchNodes(t *testing.T) {
	testSuite := dbSetup(t)

	cleanDB(t, testSuite.ctx, testSuite.db)
	t.Cleanup(func() { cleanDB(t, testSuite.ctx, testSuite.db) })

	// Seed one User node
	err := testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Alice",
			"objectid": "1",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		return err
	})
	require.NoError(t, err)

	var nodes []*graph.Node
	err = testSuite.db.ReadTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		nodes, err = FetchNodes(tx.Nodes().Filter(query.Equals(query.NodeProperty("name"), "Alice")))
		return err
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(nodes))
	require.Equal(t, "Alice", nodes[0].Properties.Get("name").Any())
}

func TestFetchNodesByQuery(t *testing.T) {
	testSuite := dbSetup(t)

	cleanDB(t, testSuite.ctx, testSuite.db)
	t.Cleanup(func() { cleanDB(t, testSuite.ctx, testSuite.db) })

	// Seed one User node
	err := testSuite.db.WriteTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"name":     "Alice",
			"objectid": "1",
		}), graph.StringKind("Entity"), graph.StringKind("User"))
		return err
	})
	require.NoError(t, err)

	var nodes graph.NodeSet
	err = testSuite.db.ReadTransaction(testSuite.ctx, func(tx graph.Transaction) error {
		var err error
		nodes, err = FetchNodesByQuery(tx, `MATCH (n:User) WHERE n.name = "Alice" RETURN n`, 10)
		return err
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(nodes))
	name, err := nodes.Pick().Properties.Get("name").String()
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
}
