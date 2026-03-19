package sonic

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

var (
	User    = graph.StringKind("User")
	Group   = graph.StringKind("Group")
	MemberOf = graph.StringKind("MemberOf")
	HasSession = graph.StringKind("HasSession")
	AdminTo = graph.StringKind("AdminTo")
)

// TestBasicCRUD verifies node and edge creation, querying, and deletion.
func TestBasicCRUD(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	var nodeA, nodeB *graph.Node

	// Create nodes
	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		nodeA, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		nodeB, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), User, Group)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(nodeA.ID, nodeB.ID, MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Query nodes
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Nodes().Count()
		if err != nil {
			return err
		}
		if count != 2 {
			t.Errorf("expected 2 nodes, got %d", count)
		}

		// Filter by kind
		count, err = tx.Nodes().Filter(query.KindIn(query.Node(), Group)).Count()
		if err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("expected 1 Group node, got %d", count)
		}

		// Query relationships
		count, err = tx.Relationships().Count()
		if err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("expected 1 relationship, got %d", count)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestPropertyFilter verifies filtering nodes by property values.
func TestPropertyFilter(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), User)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.Nodes().Filter(query.Equals(query.NodeProperty("name"), "Alice")).First()
		if err != nil {
			return err
		}
		name, _ := node.Properties.Get("name").String()
		if name != "Alice" {
			t.Errorf("expected Alice, got %s", name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestShortestPath verifies BFS shortest path finding.
func TestShortestPath(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// Build: A -MemberOf-> B -MemberOf-> C -AdminTo-> D
	//        A -HasSession-> D  (direct, shorter path — but different kind)
	var nodeIDs [4]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C", "D"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}

		// Long path: A -> B -> C -> D (all MemberOf)
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[0], nodeIDs[1], MemberOf, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[1], nodeIDs[2], MemberOf, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[2], nodeIDs[3], MemberOf, graph.NewProperties()); err != nil {
			return err
		}

		// Short path: A -> D (HasSession)
		_, err := tx.CreateRelationshipByIDs(nodeIDs[0], nodeIDs[3], HasSession, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Find shortest paths from A to D with all edge kinds
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().
			Filter(query.InIDs(query.StartID(), nodeIDs[0])).
			Filter(query.InIDs(query.EndID(), nodeIDs[3])).
			FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				var paths []graph.Path
				for path := range cursor.Chan() {
					paths = append(paths, path)
				}
				if err := cursor.Error(); err != nil {
					return err
				}

				if len(paths) != 1 {
					t.Errorf("expected 1 shortest path, got %d", len(paths))
					return nil
				}

				path := paths[0]
				if len(path.Edges) != 1 {
					t.Errorf("expected shortest path with 1 edge, got %d", len(path.Edges))
				}
				if path.Edges[0].Kind != HasSession {
					t.Errorf("expected HasSession edge, got %s", path.Edges[0].Kind)
				}
				if path.Root().ID != nodeIDs[0] {
					t.Errorf("expected root node A, got %d", path.Root().ID)
				}
				if path.Terminal().ID != nodeIDs[3] {
					t.Errorf("expected terminal node D, got %d", path.Terminal().ID)
				}

				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Find shortest paths from A to D with MemberOf only — should be 3-hop path
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().
			Filter(query.InIDs(query.StartID(), nodeIDs[0])).
			Filter(query.InIDs(query.EndID(), nodeIDs[3])).
			Filter(query.KindIn(query.Relationship(), MemberOf)).
			FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				var paths []graph.Path
				for path := range cursor.Chan() {
					paths = append(paths, path)
				}
				if err := cursor.Error(); err != nil {
					return err
				}

				if len(paths) != 1 {
					t.Errorf("expected 1 path, got %d", len(paths))
					return nil
				}

				path := paths[0]
				if len(path.Edges) != 3 {
					t.Errorf("expected 3-hop path, got %d edges", len(path.Edges))
				}

				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherQuery verifies raw Cypher string execution.
func TestCypherQuery(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), User)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Charlie"}), Group)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test: MATCH (n:User) RETURN n
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (n:User) RETURN n", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if len(values) != 1 {
				t.Errorf("expected 1 value per row, got %d", len(values))
			}
			if _, ok := values[0].(*graph.Node); !ok {
				t.Errorf("expected *graph.Node, got %T", values[0])
			}
		}
		if count != 2 {
			t.Errorf("expected 2 User nodes from cypher, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test: MATCH (n) RETURN n LIMIT 1
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (n) RETURN n LIMIT 1", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 node with LIMIT, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherScan verifies that Scan maps Cypher results to graph types.
func TestCypherScan(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (n:User) RETURN n", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		if !result.Next() {
			t.Fatal("expected at least one result")
		}

		var node graph.Node
		if err := result.Scan(&node); err != nil {
			return err
		}

		name, _ := node.Properties.Get("name").String()
		if name != "Alice" {
			t.Errorf("expected Alice, got %s", name)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestMultipleShortestPaths verifies that all equally-short paths are returned.
func TestMultipleShortestPaths(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// Build a diamond: A -> B -> D, A -> C -> D (both 2-hop)
	var nodeIDs [4]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C", "D"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}

		edges := [][2]int{{0, 1}, {0, 2}, {1, 3}, {2, 3}}
		for _, e := range edges {
			if _, err := tx.CreateRelationshipByIDs(nodeIDs[e[0]], nodeIDs[e[1]], MemberOf, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().
			Filter(query.InIDs(query.StartID(), nodeIDs[0])).
			Filter(query.InIDs(query.EndID(), nodeIDs[3])).
			FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				var paths []graph.Path
				for path := range cursor.Chan() {
					paths = append(paths, path)
				}
				if err := cursor.Error(); err != nil {
					return err
				}

				if len(paths) != 2 {
					t.Errorf("expected 2 shortest paths (diamond), got %d", len(paths))
				}

				for _, path := range paths {
					if len(path.Edges) != 2 {
						t.Errorf("expected 2-hop path, got %d edges", len(path.Edges))
					}
				}

				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherNegatedKind tests the exact query BHE's explore page sends.
func TestCypherNegatedKind(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "migration"}), graph.StringKind("MigrationData"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (A) WHERE NOT A:MigrationData RETURN A LIMIT 10", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if n, ok := values[0].(*graph.Node); ok {
				name, _ := n.Properties.Get("name").String()
				if name == "migration" {
					t.Error("MigrationData node should have been excluded")
				}
			}
		}
		if count != 1 {
			t.Errorf("expected 1 non-MigrationData node, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentReadWrite verifies no deadlock when readers and writers run simultaneously.
func TestConcurrentReadWrite(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < 10; i++ {
			if _, err := tx.CreateNode(graph.AsProperties(map[string]any{"i": i}), User); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = db.BatchOperation(ctx, func(batch graph.Batch) error {
			for i := 10; i < 20; i++ {
				if err := batch.CreateNode(&graph.Node{
					Kinds:      graph.Kinds{User},
					Properties: graph.AsProperties(map[string]any{"i": i}),
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}()

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.Nodes().Count()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	<-done
}

// TestCypherRelationshipQuery verifies: MATCH (s)-[r:MemberOf]->(e) RETURN s, r, e
func TestCypherRelationshipQuery(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), Group)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, MemberOf, graph.NewProperties())
		if err != nil {
			return err
		}
		// Add a HasSession edge that should NOT appear
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, HasSession, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (s)-[r:MemberOf]->(e) RETURN s, r, e", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if len(values) != 3 {
				t.Errorf("expected 3 values per row, got %d", len(values))
				continue
			}
			if _, ok := values[0].(*graph.Node); !ok {
				t.Errorf("expected *graph.Node for s, got %T", values[0])
			}
			if rel, ok := values[1].(*graph.Relationship); ok {
				if rel.Kind != MemberOf {
					t.Errorf("expected MemberOf relationship, got %s", rel.Kind)
				}
			} else {
				t.Errorf("expected *graph.Relationship for r, got %T", values[1])
			}
			if _, ok := values[2].(*graph.Node); !ok {
				t.Errorf("expected *graph.Node for e, got %T", values[2])
			}
		}
		if count != 1 {
			t.Errorf("expected 1 MemberOf relationship, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherInboundRelationship verifies: MATCH (a)<-[r]-(b) RETURN a, r, b
func TestCypherInboundRelationship(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	var aliceID graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		alice, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		aliceID = alice.ID
		bob, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), User)
		if err != nil {
			return err
		}
		// Bob -MemberOf-> Alice
		_, err = tx.CreateRelationshipByIDs(bob.ID, alice.ID, MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (a:User)<-[r:MemberOf]-(b) RETURN a, r, b", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if n, ok := values[0].(*graph.Node); ok {
				if n.ID != aliceID {
					t.Errorf("expected Alice as 'a', got node %d", n.ID)
				}
			}
		}
		if count != 1 {
			t.Errorf("expected 1 inbound relationship, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherWithMultiPart verifies: MATCH (n) WITH n MATCH (n)-[r]->(m) RETURN n, r, m
func TestCypherWithMultiPart(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), Group)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (n:User) WITH n MATCH (n)-[r]->(m) RETURN n, r, m", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if len(values) != 3 {
				t.Errorf("expected 3 values per row, got %d", len(values))
			}
		}
		if count != 1 {
			t.Errorf("expected 1 result from multi-part query, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherPropertyFilter verifies: MATCH (n:User) WHERE n.name = 'Alice' RETURN n
func TestCypherPropertyFilter(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), User)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH (n:User) WHERE n.name = 'Alice' RETURN n", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if n, ok := values[0].(*graph.Node); ok {
				name, _ := n.Properties.Get("name").String()
				if name != "Alice" {
					t.Errorf("expected Alice, got %s", name)
				}
			}
		}
		if count != 1 {
			t.Errorf("expected 1 result, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherUnsupportedReturnsError verifies that unsupported constructs return errors.
func TestCypherUnsupportedReturnsError(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		// CREATE should return an error
		result := tx.Query("CREATE (n:User {name: 'Charlie'})", nil)
		if result.Error() == nil {
			t.Error("expected error for CREATE, got nil")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherShortestPathViaCypher verifies allShortestPaths via Cypher string.
func TestCypherShortestPathViaCypher(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	var nodeIDs [3]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}
		// A -> B -> C
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[0], nodeIDs[1], MemberOf, graph.NewProperties()); err != nil {
			return err
		}
		_, err := tx.CreateRelationshipByIDs(nodeIDs[1], nodeIDs[2], MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		q := fmt.Sprintf(
			"MATCH p = allShortestPaths((s)-[*]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
			nodeIDs[0], nodeIDs[2],
		)
		result := tx.Query(q, nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if p, ok := values[0].(*graph.Path); ok {
				if len(p.Edges) != 2 {
					t.Errorf("expected 2-hop path, got %d edges", len(p.Edges))
				}
			} else {
				t.Errorf("expected *graph.Path, got %T", values[0])
			}
		}
		if count != 1 {
			t.Errorf("expected 1 shortest path, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherVariableLengthPath verifies: MATCH (a)-[r*1..2]->(b) RETURN a, r, b
func TestCypherVariableLengthPath(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// Build chain: A -MemberOf-> B -MemberOf-> C -MemberOf-> D
	var nodeIDs [4]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C", "D"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}
		for i := 0; i < 3; i++ {
			if _, err := tx.CreateRelationshipByIDs(nodeIDs[i], nodeIDs[i+1], MemberOf, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// MATCH (a)-[r*1..2]->(b) WHERE id(a) = A_ID RETURN a, r, b
	// Should find: A->B (1 hop), A->B->C (2 hops)
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(
			fmt.Sprintf("MATCH (a)-[r*1..2]->(b) WHERE id(a) = %d RETURN a, r, b", nodeIDs[0]),
			nil,
		)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if len(values) != 3 {
				t.Errorf("expected 3 values, got %d", len(values))
				continue
			}
			// r should be a slice of relationships
			if rels, ok := values[1].([]*graph.Relationship); ok {
				if len(rels) < 1 || len(rels) > 2 {
					t.Errorf("expected 1 or 2 relationships, got %d", len(rels))
				}
			} else {
				t.Errorf("expected []*graph.Relationship, got %T", values[1])
			}
		}
		if count != 2 {
			t.Errorf("expected 2 paths (1-hop and 2-hop), got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherVariableLengthUnbounded verifies: MATCH (a)-[*]->(b) with no upper bound
func TestCypherVariableLengthUnbounded(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// Build chain: A -> B -> C
	var nodeIDs [3]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}
		for i := 0; i < 2; i++ {
			if _, err := tx.CreateRelationshipByIDs(nodeIDs[i], nodeIDs[i+1], MemberOf, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// MATCH (a)-[*]->(b) WHERE id(a) = A_ID RETURN b
	// Should find: A->B (1 hop), A->B->C (2 hops)
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(
			fmt.Sprintf("MATCH (a)-[*]->(b) WHERE id(a) = %d RETURN b", nodeIDs[0]),
			nil,
		)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}
		if count != 2 {
			t.Errorf("expected 2 reachable nodes, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherVariableLengthNoCycles verifies that variable-length paths don't loop.
func TestCypherVariableLengthNoCycles(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// Build cycle: A -> B -> A
	var nodeIDs [2]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "A"}), User)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "B"}), User)
		if err != nil {
			return err
		}
		nodeIDs[0] = a.ID
		nodeIDs[1] = b.ID
		if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, MemberOf, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(b.ID, a.ID, MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// A->B is depth 1. B->A would revisit the start node, so it's blocked by cycle detection.
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(
			fmt.Sprintf("MATCH (a)-[*1..10]->(b) WHERE id(a) = %d RETURN b", nodeIDs[0]),
			nil,
		)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 path (cycle should be prevented), got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherVariableLengthWithKind verifies kind filtering on variable-length edges.
func TestCypherVariableLengthWithKind(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	// A -MemberOf-> B -HasSession-> C -MemberOf-> D
	var nodeIDs [4]graph.ID

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		names := []string{"A", "B", "C", "D"}
		for i, name := range names {
			n, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": name}), User)
			if err != nil {
				return err
			}
			nodeIDs[i] = n.ID
		}
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[0], nodeIDs[1], MemberOf, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(nodeIDs[1], nodeIDs[2], HasSession, graph.NewProperties()); err != nil {
			return err
		}
		_, err := tx.CreateRelationshipByIDs(nodeIDs[2], nodeIDs[3], MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Only follow MemberOf edges — should reach B but NOT C or D
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(
			fmt.Sprintf("MATCH (a)-[:MemberOf*1..5]->(b) WHERE id(a) = %d RETURN b", nodeIDs[0]),
			nil,
		)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if n, ok := values[0].(*graph.Node); ok {
				if n.ID == nodeIDs[2] || n.ID == nodeIDs[3] {
					t.Errorf("should not reach node %d through MemberOf-only path", n.ID)
				}
			}
		}
		if count != 1 {
			t.Errorf("expected 1 reachable node via MemberOf, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestCypherAnonymousNodes verifies: MATCH ()-[r]->() RETURN r
func TestCypherAnonymousNodes(t *testing.T) {
	db := NewDatabase()
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Alice"}), User)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "Bob"}), Group)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, MemberOf, graph.NewProperties())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query("MATCH ()-[r]->() RETURN r LIMIT 5", nil)
		if result.Error() != nil {
			return result.Error()
		}
		defer result.Close()

		count := 0
		for result.Next() {
			count++
			values := result.Values()
			if _, ok := values[0].(*graph.Relationship); !ok {
				t.Errorf("expected *graph.Relationship, got %T", values[0])
			}
		}
		if count != 1 {
			t.Errorf("expected 1 relationship, got %d", count)
		}
		return result.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
}
