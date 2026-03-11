//go:build manual_integration

package pg_test

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg/temporaltest"
	"github.com/stretchr/testify/require"
)

func TestTemporalSmoke(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	env.CreateNodes(20)
	env.CreateRandomEdges(30)
	snapshotEdges := env.CurrentEdgeCount()
	snap := env.Snapshot()

	env.CreateRandomEdges(10)
	env.DeleteEdges(5)

	current := env.CurrentEdgeCount()
	historical := env.HistoricalEdgeCount(snap)

	require.NotEqual(t, current, historical, "current and historical should differ")
	require.Equal(t, snapshotEdges, historical, "historical should match snapshot-time count")

	t.Logf("current=%d historical=%d (snapshot had %d)", current, historical, snapshotEdges)
}

func TestTemporalDeletedEdgesVisible(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	env.CreateNodes(10)
	env.CreateRandomEdges(20)
	edgesBeforeDelete := env.CurrentEdgeCount()
	snap := env.Snapshot()

	env.DeleteEdges(int(edgesBeforeDelete)) // delete all tracked edges

	env.AssertCurrentEdgeCount(0)
	env.AssertHistoricalEdgeCount(snap, edgesBeforeDelete)
}

func TestTemporalNodesHistorical(t *testing.T) {
	env := temporaltest.New(t)
	defer env.Close()

	env.CreateNodes(5)
	snap := env.Snapshot()
	env.CreateNodes(10)

	env.AssertCurrentNodeCount(15)
	env.AssertHistoricalNodeCount(snap, 5)
}

func TestReconstructionAtScale(t *testing.T) {
	sizes := []struct {
		name              string
		nodes, edges      int
		deletes, newEdges int
	}{
		{"10k_50k", 10000, 50000, 5000, 10000},
		{"50k_250k", 50000, 250000, 25000, 50000},
		{"100k_500k", 100000, 500000, 50000, 100000},
	}

	for _, sz := range sizes {
		t.Run(sz.name, func(t *testing.T) {
			env := temporaltest.New(t)
			defer env.Close()

			setupStart := time.Now()
			env.CreateNodes(sz.nodes)
			env.CreateRandomEdges(sz.edges)
			actualEdges := env.CurrentEdgeCount()
			t.Logf("Setup: %d nodes, %d edges in %v", sz.nodes, actualEdges, time.Since(setupStart))

			snap := env.Snapshot()

			env.CreateRandomEdges(sz.newEdges)
			env.DeleteEdges(sz.deletes)
			t.Logf("Post-mutation: %d edges", env.CurrentEdgeCount())

			const runs = 5
			var total time.Duration
			for i := 0; i < runs; i++ {
				start := time.Now()
				_ = env.HistoricalEdgeCount(snap)
				total += time.Since(start)
			}

			t.Logf("Reconstruction avg (%d runs): %v", runs, total/time.Duration(runs))
		})
	}
}
