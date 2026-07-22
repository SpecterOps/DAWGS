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

func countGraphEntitySnapshot(ctx context.Context, db graph.Database, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	var entitySnapshot graphEntitySnapshot
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)

		var err error
		if entitySnapshot.NodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}

		if entitySnapshot.EdgeCount, err = tx.Relationships().Count(); err != nil {
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

func retrieverNextProgressAt(processed int64, planned int64, nextProgressAt int64) int64 {
	if nextProgressAt <= processed {
		nextProgressAt += ((processed-nextProgressAt)/retrieverProgressEntityInterval + 1) * retrieverProgressEntityInterval
	}
	if nextProgressAt >= planned {
		return 0
	}

	return nextProgressAt
}

func perSecond(count int64, elapsed time.Duration) float64 {
	if count == 0 || elapsed <= 0 {
		return 0
	}

	return float64(count) / elapsed.Seconds()
}
