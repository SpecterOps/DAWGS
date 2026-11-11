package main

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/graph"
)

const retrieverProgressEntityInterval int64 = 250_000

type graphEntitySnapshot struct {
	NodeCount int64
	EdgeCount int64
}

func countGraphEntities(ctx context.Context, db graph.Database, targetGraph graph.Graph) (int64, int64, error) {
	entitySnapshot, err := countGraphEntitySnapshot(ctx, db, targetGraph)
	if err != nil {
		return 0, 0, err
	}

	return entitySnapshot.NodeCount, entitySnapshot.EdgeCount, nil
}

// countGraphEntitySnapshot counts nodes and relationships within the selected graph in one read transaction.
func countGraphEntitySnapshot(ctx context.Context, db graph.Database, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	var entitySnapshot graphEntitySnapshot
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		scopedTx := tx.WithGraph(targetGraph)

		var err error
		if entitySnapshot.NodeCount, err = scopedTx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}

		if entitySnapshot.EdgeCount, err = scopedTx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}

		return nil
	}); err != nil {
		return graphEntitySnapshot{}, err
	}

	return entitySnapshot, nil
}

func retrieverInitialProgressAt(planned int64) int64 {
	if planned <= retrieverProgressEntityInterval {
		return 0
	}

	return retrieverProgressEntityInterval
}

func retrieverBatchLimit(remaining int64, batchSize int) int {
	if remaining <= 0 {
		return 0
	}

	if int64(batchSize) > remaining {
		return int(remaining)
	}

	return batchSize
}

// retrieverNextProgressAt advances the progress checkpoint past processed entities or disables it at the end.
func retrieverNextProgressAt(processed int64, planned int64, nextProgressAt int64) int64 {
	nextCheckpoint := nextProgressAt

	if nextCheckpoint <= processed {
		nextCheckpoint += ((processed-nextCheckpoint)/retrieverProgressEntityInterval + 1) * retrieverProgressEntityInterval
	}
	if nextCheckpoint >= planned {
		return 0
	}

	return nextCheckpoint
}

func perSecond(count int64, elapsed time.Duration) float64 {
	if count == 0 || elapsed <= 0 {
		return 0
	}

	return float64(count) / elapsed.Seconds()
}
