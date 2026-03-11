// Package temporaltest provides helpers for testing temporal graph features.
package temporaltest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

var (
	KindUser        = graph.StringKind("User")
	KindGroup       = graph.StringKind("Group")
	KindComputer    = graph.StringKind("Computer")
	KindDomainAdmin = graph.StringKind("DomainAdmin")

	KindMemberOf   = graph.StringKind("MemberOf")
	KindAdminTo    = graph.StringKind("AdminTo")
	KindHasSession = graph.StringKind("HasSession")

	nodeKinds = []graph.Kind{KindUser, KindGroup, KindComputer, KindDomainAdmin}
	edgeKinds = []graph.Kind{KindMemberOf, KindAdminTo, KindHasSession}
)

// Env is a test environment for temporal graph operations.
type Env struct {
	Driver  *pg.Driver
	db      graph.Database
	tb      testing.TB
	ctx     context.Context
	rng     *rand.Rand
	NodeIDs []graph.ID
	EdgeIDs []graph.ID
}

// New creates a test environment connected to the database specified by PG_CONNECTION_STRING.
func New(tb testing.TB) *Env {
	tb.Helper()

	connStr := os.Getenv("PG_CONNECTION_STRING")
	if connStr == "" {
		tb.Fatal("PG_CONNECTION_STRING must be set")
	}

	ctx := context.Background()
	pool, err := pg.NewPool(connStr)
	require.NoError(tb, err)

	db, err := dawgs.Open(ctx, pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pool,
	})
	require.NoError(tb, err)

	driver, ok := db.(*pg.Driver)
	require.True(tb, ok)

	graphName := fmt.Sprintf("temporal_%s_%d", tb.Name(), time.Now().UnixNano())
	require.NoError(tb, db.AssertSchema(ctx, graph.Schema{
		Graphs: []graph.Graph{{
			Name:  graphName,
			Nodes: graph.Kinds{KindUser, KindGroup, KindComputer, KindDomainAdmin},
			Edges: graph.Kinds{KindMemberOf, KindAdminTo, KindHasSession},
		}},
		DefaultGraph: graph.Graph{Name: graphName},
	}))

	return &Env{
		Driver: driver,
		db:     db,
		tb:     tb,
		ctx:    ctx,
		rng:    rand.New(rand.NewSource(42)),
	}
}

// Close tears down the test environment.
func (e *Env) Close() {
	e.db.WriteTransaction(e.ctx, func(tx graph.Transaction) error {
		tx.Relationships().Delete()
		tx.Nodes().Delete()
		return nil
	})
	e.db.Run(e.ctx, "DELETE FROM node_deletion_log", nil)
	e.db.Run(e.ctx, "DELETE FROM edge_deletion_log", nil)
	e.db.Close(e.ctx)
}

// CreateNodes creates n nodes, cycling through the provided kinds.
func (e *Env) CreateNodes(n int, kinds ...graph.Kind) {
	e.tb.Helper()
	if len(kinds) == 0 {
		kinds = nodeKinds
	}

	err := e.db.BatchOperation(e.ctx, func(batch graph.Batch) error {
		for i := 0; i < n; i++ {
			if err := batch.CreateNode(&graph.Node{
				Kinds:      graph.Kinds{kinds[i%len(kinds)]},
				Properties: graph.AsProperties(map[string]any{"name": fmt.Sprintf("node-%d", len(e.NodeIDs)+i)}),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(e.tb, err)

	// Refresh the full node ID list
	e.NodeIDs = e.NodeIDs[:0]
	err = e.db.ReadTransaction(e.ctx, func(tx graph.Transaction) error {
		return tx.Nodes().FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				e.NodeIDs = append(e.NodeIDs, id)
			}
			return cursor.Error()
		})
	})
	require.NoError(e.tb, err)
}

// CreateRandomEdges creates n random edges between existing nodes.
// Uses the batch path which handles duplicate (start, end, kind) gracefully via ON CONFLICT.
func (e *Env) CreateRandomEdges(n int) {
	e.tb.Helper()
	require.True(e.tb, len(e.NodeIDs) >= 2, "need at least 2 nodes to create edges")

	err := e.db.BatchOperation(e.ctx, func(batch graph.Batch) error {
		for i := 0; i < n; i++ {
			s, d := e.rng.Intn(len(e.NodeIDs)), e.rng.Intn(len(e.NodeIDs))
			if s == d {
				d = (d + 1) % len(e.NodeIDs)
			}
			if err := batch.CreateRelationshipByIDs(e.NodeIDs[s], e.NodeIDs[d], edgeKinds[i%len(edgeKinds)], graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(e.tb, err)

	// Refresh edge IDs from the database
	e.EdgeIDs = e.EdgeIDs[:0]
	err = e.db.ReadTransaction(e.ctx, func(tx graph.Transaction) error {
		return tx.Relationships().FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				e.EdgeIDs = append(e.EdgeIDs, id)
			}
			return cursor.Error()
		})
	})
	require.NoError(e.tb, err)
}

// DeleteEdges deletes the first n edges from the tracked edge list.
func (e *Env) DeleteEdges(n int) {
	e.tb.Helper()
	if n > len(e.EdgeIDs) {
		n = len(e.EdgeIDs)
	}

	toDelete := e.EdgeIDs[:n]
	err := e.db.BatchOperation(e.ctx, func(batch graph.Batch) error {
		for _, id := range toDelete {
			if err := batch.DeleteRelationship(id); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(e.tb, err)
	e.EdgeIDs = e.EdgeIDs[n:]
}

// Snapshot records the current time as a temporal snapshot point.
func (e *Env) Snapshot() time.Time {
	t := time.Now()
	time.Sleep(50 * time.Millisecond) // ensure timestamp separation
	return t
}

// CurrentEdgeCount returns the number of edges in the current graph.
func (e *Env) CurrentEdgeCount() int64 {
	e.tb.Helper()
	var count int64
	err := e.db.ReadTransaction(e.ctx, func(tx graph.Transaction) error {
		var err error
		count, err = tx.Relationships().Count()
		return err
	})
	require.NoError(e.tb, err)
	return count
}

// CurrentNodeCount returns the number of nodes in the current graph.
func (e *Env) CurrentNodeCount() int64 {
	e.tb.Helper()
	var count int64
	err := e.db.ReadTransaction(e.ctx, func(tx graph.Transaction) error {
		var err error
		count, err = tx.Nodes().Count()
		return err
	})
	require.NoError(e.tb, err)
	return count
}

// HistoricalEdgeCount returns the number of edges visible at the given snapshot time.
func (e *Env) HistoricalEdgeCount(snap time.Time) int64 {
	e.tb.Helper()
	var count int64
	err := e.Driver.AsOfReadTransaction(e.ctx, snap, func(tx graph.Transaction) error {
		var err error
		count, err = tx.Relationships().Count()
		return err
	})
	require.NoError(e.tb, err)
	return count
}

// HistoricalNodeCount returns the number of nodes visible at the given snapshot time.
func (e *Env) HistoricalNodeCount(snap time.Time) int64 {
	e.tb.Helper()
	var count int64
	err := e.Driver.AsOfReadTransaction(e.ctx, snap, func(tx graph.Transaction) error {
		var err error
		count, err = tx.Nodes().Count()
		return err
	})
	require.NoError(e.tb, err)
	return count
}

// AssertCurrentEdgeCount asserts the current edge count equals expected.
func (e *Env) AssertCurrentEdgeCount(expected int64) {
	e.tb.Helper()
	require.Equal(e.tb, expected, e.CurrentEdgeCount(), "current edge count")
}

// AssertHistoricalEdgeCount asserts the historical edge count at snap equals expected.
func (e *Env) AssertHistoricalEdgeCount(snap time.Time, expected int64) {
	e.tb.Helper()
	require.Equal(e.tb, expected, e.HistoricalEdgeCount(snap), "historical edge count")
}

// AssertCurrentNodeCount asserts the current node count equals expected.
func (e *Env) AssertCurrentNodeCount(expected int64) {
	e.tb.Helper()
	require.Equal(e.tb, expected, e.CurrentNodeCount(), "current node count")
}

// AssertHistoricalNodeCount asserts the historical node count at snap equals expected.
func (e *Env) AssertHistoricalNodeCount(snap time.Time, expected int64) {
	e.tb.Helper()
	require.Equal(e.tb, expected, e.HistoricalNodeCount(snap), "historical node count")
}
