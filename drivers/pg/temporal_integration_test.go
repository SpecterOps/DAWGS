//go:build manual_integration

package pg_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

var (
	// Kind definitions for the test scenario
	kindUser        = graph.StringKind("User")
	kindGroup       = graph.StringKind("Group")
	kindComputer    = graph.StringKind("Computer")
	kindDomainAdmin = graph.StringKind("DomainAdmin")

	kindMemberOf   = graph.StringKind("MemberOf")
	kindAdminTo    = graph.StringKind("AdminTo")
	kindHasSession = graph.StringKind("HasSession")
)

func setupTestDriver(t *testing.T) (*pg.Driver, func()) {
	t.Helper()

	ctx := context.Background()
	pgConnectionStr := os.Getenv("PG_CONNECTION_STRING")
	require.NotEmpty(t, pgConnectionStr, "PG_CONNECTION_STRING must be set")

	pgxPool, err := pg.NewPool(pgConnectionStr)
	require.NoError(t, err)

	connection, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pgxPool,
	})
	require.NoError(t, err)

	pgDriver, typeOK := connection.(*pg.Driver)
	require.True(t, typeOK)

	graphSchema := graph.Schema{
		Graphs: []graph.Graph{{
			Name: "temporal_test",
			Nodes: graph.Kinds{
				kindUser, kindGroup, kindComputer, kindDomainAdmin,
			},
			Edges: graph.Kinds{
				kindMemberOf, kindAdminTo, kindHasSession,
			},
		}},
		DefaultGraph: graph.Graph{
			Name: "temporal_test",
		},
	}

	require.NoError(t, connection.AssertSchema(ctx, graphSchema))

	cleanup := func() {
		// Clean up test data
		connection.WriteTransaction(ctx, func(tx graph.Transaction) error {
			tx.Relationships().Delete()
			tx.Nodes().Delete()
			return nil
		})
		connection.Close(ctx)
	}

	return pgDriver, cleanup
}

// TestAsOfReadTransaction_SeesHistoricalState verifies that AsOfReadTransaction shows the graph
// as it existed at a past timestamp. New edges created after the timestamp should not be visible.
func TestAsOfReadTransaction_SeesHistoricalState(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	// Phase 1: Create the initial graph — User is a MemberOf Group
	var userID, groupID, computerID, domainAdminID graph.ID

	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		user, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "UserA"}), kindUser)
		if err != nil {
			return err
		}
		userID = user.ID

		grp, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "SecurityGroup"}), kindGroup)
		if err != nil {
			return err
		}
		groupID = grp.ID

		comp, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "WorkstationX"}), kindComputer)
		if err != nil {
			return err
		}
		computerID = comp.ID

		da, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "DomainAdmin"}), kindDomainAdmin)
		if err != nil {
			return err
		}
		domainAdminID = da.ID

		// Only edge in initial state: User -> Group
		_, err = tx.CreateRelationshipByIDs(userID, groupID, kindMemberOf, graph.NewProperties())
		return err
	}))

	// Record the timestamp — this is our "as-of" point
	snapshotTime := time.Now()

	// Small delay to ensure timestamp separation
	time.Sleep(50 * time.Millisecond)

	// Phase 2: Add edges that create an attack path (after the snapshot)
	// Group -> Computer (AdminTo) and Computer -> DomainAdmin (HasSession)
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateRelationshipByIDs(groupID, computerID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}
		_, err := tx.CreateRelationshipByIDs(computerID, domainAdminID, kindHasSession, graph.NewProperties())
		return err
	}))

	// Verify current state has 3 edges
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(3), count, "current graph should have 3 edges")
		return nil
	}))

	// Phase 3: AsOfReadTransaction at the snapshot time should only see 1 edge
	require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "historical graph should have only 1 edge (MemberOf)")

		// Verify the one edge is the MemberOf edge
		rel, err := tx.Relationships().Filter(query.Kind(query.Relationship(), kindMemberOf)).First()
		require.NoError(t, err)
		require.Equal(t, userID, rel.StartID)
		require.Equal(t, groupID, rel.EndID)

		// Verify the AdminTo and HasSession edges don't exist yet
		adminCount, err := tx.Relationships().Filter(query.Kind(query.Relationship(), kindAdminTo)).Count()
		require.NoError(t, err)
		require.Equal(t, int64(0), adminCount, "AdminTo edge should not exist in historical snapshot")

		return nil
	}))
}

// TestAsOfReadTransaction_SeesDeletedEdges verifies that edges deleted after the snapshot time
// are still visible in a historical query.
func TestAsOfReadTransaction_SeesDeletedEdges(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	// Phase 1: Create graph with an edge
	var userID, groupID graph.ID
	var memberOfRelID graph.ID

	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		user, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "UserA"}), kindUser)
		if err != nil {
			return err
		}
		userID = user.ID

		grp, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "SecurityGroup"}), kindGroup)
		if err != nil {
			return err
		}
		groupID = grp.ID

		rel, err := tx.CreateRelationshipByIDs(userID, groupID, kindMemberOf, graph.NewProperties())
		if err != nil {
			return err
		}
		memberOfRelID = rel.ID

		return nil
	}))

	// Record the snapshot time — edge exists at this point
	snapshotTime := time.Now()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: Delete the edge (via batch, which uses the CTE-based delete+log)
	require.NoError(t, pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(memberOfRelID)
	}))

	// Verify current state has 0 edges
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(0), count, "current graph should have 0 edges after deletion")
		return nil
	}))

	// Phase 3: AsOfReadTransaction should still see the deleted edge
	require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
		count, err := tx.Relationships().Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "historical graph should still have the deleted edge")

		rel, err := tx.Relationships().First()
		require.NoError(t, err)
		require.Equal(t, userID, rel.StartID)
		require.Equal(t, groupID, rel.EndID)

		return nil
	}))
}

// TestAsOfReadTransaction_NodesAlsoHistorical verifies that nodes created after the snapshot
// are not visible in historical queries.
func TestAsOfReadTransaction_NodesAlsoHistorical(t *testing.T) {
	pgDriver, cleanup := setupTestDriver(t)
	defer cleanup()

	ctx := context.Background()

	// Phase 1: Create initial node
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "UserA"}), kindUser)
		return err
	}))

	snapshotTime := time.Now()
	time.Sleep(50 * time.Millisecond)

	// Phase 2: Create more nodes after snapshot
	require.NoError(t, pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.AsProperties(map[string]any{"name": "UserB"}), kindUser)
		if err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.AsProperties(map[string]any{"name": "UserC"}), kindUser)
		return err
	}))

	// Current state: 3 nodes
	require.NoError(t, pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		count, err := tx.Nodes().Count()
		require.NoError(t, err)
		require.Equal(t, int64(3), count)
		return nil
	}))

	// Historical state: 1 node
	require.NoError(t, pgDriver.AsOfReadTransaction(ctx, snapshotTime, func(tx graph.Transaction) error {
		count, err := tx.Nodes().Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count, "historical graph should only have 1 node")
		return nil
	}))
}
