package sonic_test

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/drivers/sonic"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
)

// These tests exercise the sonic driver through the ops package — the same
// code paths BloodHound Enterprise uses for graph operations.

var (
	User       = graph.StringKind("User")
	Group      = graph.StringKind("Group")
	Computer   = graph.StringKind("Computer")
	Domain     = graph.StringKind("Domain")
	MemberOf   = graph.StringKind("MemberOf")
	HasSession = graph.StringKind("HasSession")
	AdminTo    = graph.StringKind("AdminTo")
	GenericAll = graph.StringKind("GenericAll")
)

func setupTestGraph(t *testing.T) (*sonic.Database, map[string]graph.ID) {
	t.Helper()

	db := sonic.NewDatabase()
	ctx := context.Background()
	ids := make(map[string]graph.ID)

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		type nodeSpec struct {
			name  string
			kinds []graph.Kind
		}
		nodes := []nodeSpec{
			{"jsmith", []graph.Kind{User}},
			{"Domain Admins", []graph.Kind{Group}},
			{"Domain Users", []graph.Kind{Group}},
			{"IT Admins", []graph.Kind{Group}},
			{"DC01", []graph.Kind{Computer}},
			{"WS01", []graph.Kind{Computer}},
			{"corp.local", []graph.Kind{Domain}},
		}

		for _, n := range nodes {
			node, err := tx.CreateNode(graph.AsProperties(map[string]any{
				"name": n.name,
			}), n.kinds...)
			if err != nil {
				return err
			}
			ids[n.name] = node.ID
		}

		edges := []struct {
			from, to string
			kind     graph.Kind
		}{
			{"jsmith", "Domain Users", MemberOf},
			{"jsmith", "IT Admins", MemberOf},
			{"IT Admins", "Domain Admins", MemberOf},
			{"Domain Admins", "corp.local", GenericAll},
			{"jsmith", "WS01", HasSession},
			{"Domain Admins", "DC01", AdminTo},
			{"IT Admins", "WS01", AdminTo},
		}

		for _, e := range edges {
			if _, err := tx.CreateRelationshipByIDs(ids[e.from], ids[e.to], e.kind, graph.NewProperties()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	return db, ids
}

// TestOpsCountNodes tests ops.CountNodes against sonic.
func TestOpsCountNodes(t *testing.T) {
	db, _ := setupTestGraph(t)
	ctx := context.Background()

	total, err := ops.CountNodes(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if total != 7 {
		t.Errorf("expected 7 nodes, got %d", total)
	}

	groups, err := ops.CountNodes(ctx, db, query.KindIn(query.Node(), Group))
	if err != nil {
		t.Fatal(err)
	}
	if groups != 3 {
		t.Errorf("expected 3 groups, got %d", groups)
	}
}

// TestOpsFetchNode tests ops.FetchNode (single node by ID).
func TestOpsFetchNode(t *testing.T) {
	db, ids := setupTestGraph(t)
	ctx := context.Background()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		node, err := ops.FetchNode(tx, ids["jsmith"])
		if err != nil {
			return err
		}

		name, _ := node.Properties.Get("name").String()
		if name != "jsmith" {
			t.Errorf("expected jsmith, got %s", name)
		}
		if !node.Kinds.ContainsOneOf(User) {
			t.Error("expected node to have User kind")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpsFetchNodeSet tests ops.FetchNodeSet with kind filtering.
func TestOpsFetchNodeSet(t *testing.T) {
	db, _ := setupTestGraph(t)
	ctx := context.Background()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		computers, err := ops.FetchNodeSet(tx.Nodes().Filter(
			query.KindIn(query.Node(), Computer),
		))
		if err != nil {
			return err
		}
		if computers.Len() != 2 {
			t.Errorf("expected 2 computers, got %d", computers.Len())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpsFetchRelationships tests fetching relationships with kind filter.
func TestOpsFetchRelationships(t *testing.T) {
	db, _ := setupTestGraph(t)
	ctx := context.Background()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		memberOfRels, err := ops.FetchRelationships(
			tx.Relationships().Filter(query.KindIn(query.Relationship(), MemberOf)),
		)
		if err != nil {
			return err
		}
		if len(memberOfRels) != 3 {
			t.Errorf("expected 3 MemberOf relationships, got %d", len(memberOfRels))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpsFetchNodeRelationships tests ops.FetchNodeRelationships (outbound/inbound from a node).
func TestOpsFetchNodeRelationships(t *testing.T) {
	db, ids := setupTestGraph(t)
	ctx := context.Background()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		jsmith, err := ops.FetchNode(tx, ids["jsmith"])
		if err != nil {
			return err
		}

		// jsmith has 3 outbound edges: MemberOf(Domain Users), MemberOf(IT Admins), HasSession(WS01)
		outbound, err := ops.FetchNodeRelationships(tx, jsmith, graph.DirectionOutbound)
		if err != nil {
			return err
		}
		if len(outbound) != 3 {
			t.Errorf("expected 3 outbound relationships from jsmith, got %d", len(outbound))
		}

		// jsmith has 0 inbound edges
		inbound, err := ops.FetchNodeRelationships(tx, jsmith, graph.DirectionInbound)
		if err != nil {
			return err
		}
		if len(inbound) != 0 {
			t.Errorf("expected 0 inbound relationships to jsmith, got %d", len(inbound))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpsForEachStartEndNode tests ops.ForEachStartNode and ForEachEndNode.
func TestOpsForEachStartEndNode(t *testing.T) {
	db, _ := setupTestGraph(t)
	ctx := context.Background()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		// Get start nodes of all AdminTo edges
		startNodes, err := ops.FetchStartNodes(
			tx.Relationships().Filter(query.KindIn(query.Relationship(), AdminTo)),
		)
		if err != nil {
			return err
		}
		if startNodes.Len() != 2 {
			t.Errorf("expected 2 AdminTo start nodes, got %d", startNodes.Len())
		}

		// Get end nodes of all AdminTo edges
		endNodes, err := ops.FetchEndNodes(
			tx.Relationships().Filter(query.KindIn(query.Relationship(), AdminTo)),
		)
		if err != nil {
			return err
		}
		if endNodes.Len() != 2 {
			t.Errorf("expected 2 AdminTo end nodes, got %d", endNodes.Len())
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpsDeleteNodes tests ops.DeleteNodes.
func TestOpsDeleteNodes(t *testing.T) {
	db, ids := setupTestGraph(t)
	ctx := context.Background()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return ops.DeleteNodes(tx, ids["WS01"])
	})
	if err != nil {
		t.Fatal(err)
	}

	count, err := ops.CountNodes(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Errorf("expected 6 nodes after delete, got %d", count)
	}
}

// TestOpsDeleteRelationships tests ops.DeleteRelationships.
func TestOpsDeleteRelationships(t *testing.T) {
	db, ids := setupTestGraph(t)
	ctx := context.Background()

	// Find the HasSession edge from jsmith -> WS01
	var hasSessionID graph.ID
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(
			query.And(
				query.KindIn(query.Relationship(), HasSession),
				query.Equals(query.StartID(), ids["jsmith"]),
			),
		).Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for rel := range cursor.Chan() {
				hasSessionID = rel.ID
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Delete it
	err = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return ops.DeleteRelationships(tx, hasSessionID)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Filter(
			query.KindIn(query.Relationship(), HasSession),
		).Count()
		if err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("expected 0 HasSession edges after delete, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestAttackPathFinding tests the core BHE use case: find attack paths through the graph.
func TestAttackPathFinding(t *testing.T) {
	db, ids := setupTestGraph(t)
	ctx := context.Background()

	// Attack path: jsmith -> IT Admins -> Domain Admins -> corp.local (via MemberOf, MemberOf, GenericAll)
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().
			Filter(query.InIDs(query.StartID(), ids["jsmith"])).
			Filter(query.InIDs(query.EndID(), ids["corp.local"])).
			Filter(query.KindIn(query.Relationship(), MemberOf, GenericAll)).
			FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
				var paths []graph.Path
				for path := range cursor.Chan() {
					paths = append(paths, path)
				}
				if err := cursor.Error(); err != nil {
					return err
				}

				if len(paths) != 1 {
					t.Fatalf("expected 1 attack path, got %d", len(paths))
				}

				path := paths[0]
				if len(path.Nodes) != 4 {
					t.Errorf("expected 4 nodes in path, got %d", len(path.Nodes))
				}
				if len(path.Edges) != 3 {
					t.Errorf("expected 3 edges in path, got %d", len(path.Edges))
				}

				// Verify path: jsmith -> IT Admins -> Domain Admins -> corp.local
				if path.Root().ID != ids["jsmith"] {
					t.Errorf("expected path to start at jsmith")
				}
				if path.Terminal().ID != ids["corp.local"] {
					t.Errorf("expected path to end at corp.local")
				}

				return nil
			})
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestBatchOperations tests batch create and upsert operations.
func TestBatchOperations(t *testing.T) {
	db := sonic.NewDatabase()
	ctx := context.Background()

	// Batch create
	err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		node := &graph.Node{
			Kinds:      graph.Kinds{User},
			Properties: graph.AsProperties(map[string]any{"name": "alice", "email": "alice@corp.local"}),
		}
		if err := batch.CreateNode(node); err != nil {
			return err
		}

		// Upsert same node — should update, not create duplicate
		upsertNode := &graph.Node{
			Kinds:      graph.Kinds{User},
			Properties: graph.AsProperties(map[string]any{"name": "alice", "email": "alice@newcorp.local"}),
		}
		return batch.UpdateNodeBy(graph.NodeUpdate{
			Node:               upsertNode,
			IdentityKind:       User,
			IdentityProperties: []string{"name"},
		})
	})
	if err != nil {
		t.Fatal(err)
	}

	// Should still be 1 node, with updated email
	count, err := ops.CountNodes(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 node after upsert, got %d", count)
	}

	err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.Nodes().First()
		if err != nil {
			return err
		}
		email, _ := node.Properties.Get("email").String()
		if email != "alice@newcorp.local" {
			t.Errorf("expected updated email, got %s", email)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestParallelFetchNodes tests the parallel fetch code path used by BHE.
func TestParallelFetchNodes(t *testing.T) {
	db, _ := setupTestGraph(t)
	ctx := context.Background()

	nodes, err := ops.ParallelFetchNodes(ctx, db, query.KindIn(query.Node(), User), 2)
	if err != nil {
		t.Fatal(err)
	}
	if nodes.Len() != 1 {
		t.Errorf("expected 1 user from parallel fetch, got %d", nodes.Len())
	}
}
