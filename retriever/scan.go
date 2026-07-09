package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/graph"
)

type entityBatchReader[T any] func(afterID graph.ID, hasAfterID bool, batchSize int) ([]T, error)
type entityBatchHandler[T any] func([]T) error
type entityIDFunc[T any] func(T) graph.ID
type entityProgressLogger func(processed int64, startedAt time.Time, nextProgressAt int64) int64

type entityScanOptions[T any] struct {
	Total            int64
	BatchSize        int
	ProgressInterval int64
	EntityName       string
	Read             entityBatchReader[T]
	ID               entityIDFunc[T]
	Handle           entityBatchHandler[T]
	LogProgress      entityProgressLogger
}

func scanEntityBatches[T any](options entityScanOptions[T]) (int64, error) {
	entityName := options.EntityName
	if entityName == "" {
		entityName = "entity"
	}

	if options.Total <= 0 {
		return 0, nil
	}

	if options.BatchSize <= 0 {
		return 0, fmt.Errorf("%s batch size must be > 0", entityName)
	}

	if options.Read == nil {
		return 0, fmt.Errorf("%s batch reader is required", entityName)
	}

	if options.ID == nil {
		return 0, fmt.Errorf("%s ID accessor is required", entityName)
	}

	var (
		lastID    graph.ID
		hasLastID bool
		processed int64

		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAtInterval(options.Total, options.ProgressInterval)
	)

	for processed < options.Total {
		remaining := options.Total - processed
		batch, err := options.Read(lastID, hasLastID, retrieverBatchLimit(remaining, options.BatchSize))
		if err != nil {
			return processed, err
		}

		if int64(len(batch)) > remaining {
			batch = batch[:int(remaining)]
		}

		if len(batch) == 0 {
			break
		}

		nextID := options.ID(batch[len(batch)-1])
		if hasLastID && nextID <= lastID {
			return processed, fmt.Errorf("%s keyset scan did not advance after ID %d", entityName, lastID.Uint64())
		}

		if options.Handle != nil {
			if err := options.Handle(batch); err != nil {
				return processed, err
			}
		}

		processed += int64(len(batch))
		lastID = nextID
		hasLastID = true

		if options.LogProgress != nil {
			nextProgressAt = options.LogProgress(processed, startedAt, nextProgressAt)
		}
	}

	return processed, nil
}

func scanDatabaseNodes(
	ctx context.Context,
	db graph.Database,
	targetGraph graph.Graph,
	total int64,
	batchSize int,
	handle entityBatchHandler[*graph.Node],
	logProgress entityProgressLogger,
) (int64, error) {
	return scanDatabaseNodesWithProgressInterval(ctx, db, targetGraph, total, batchSize, 0, handle, logProgress)
}

func scanDatabaseNodesWithProgressInterval(
	ctx context.Context,
	db graph.Database,
	targetGraph graph.Graph,
	total int64,
	batchSize int,
	progressInterval int64,
	handle entityBatchHandler[*graph.Node],
	logProgress entityProgressLogger,
) (int64, error) {
	return scanEntityBatches(entityScanOptions[*graph.Node]{
		Total:            total,
		BatchSize:        batchSize,
		ProgressInterval: progressInterval,
		EntityName:       "node",
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Node, error) {
			return readDatabaseNodes(ctx, db, targetGraph, afterID, hasAfterID, limit)
		},
		ID: func(node *graph.Node) graph.ID {
			return node.ID
		},
		Handle:      handle,
		LogProgress: logProgress,
	})
}

func scanDatabaseRelationships(
	ctx context.Context,
	db graph.Database,
	targetGraph graph.Graph,
	total int64,
	batchSize int,
	handle entityBatchHandler[*graph.Relationship],
	logProgress entityProgressLogger,
) (int64, error) {
	return scanDatabaseRelationshipsWithProgressInterval(ctx, db, targetGraph, total, batchSize, 0, handle, logProgress)
}

func scanDatabaseRelationshipsWithProgressInterval(
	ctx context.Context,
	db graph.Database,
	targetGraph graph.Graph,
	total int64,
	batchSize int,
	progressInterval int64,
	handle entityBatchHandler[*graph.Relationship],
	logProgress entityProgressLogger,
) (int64, error) {
	return scanEntityBatches(entityScanOptions[*graph.Relationship]{
		Total:            total,
		BatchSize:        batchSize,
		ProgressInterval: progressInterval,
		EntityName:       "relationship",
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Relationship, error) {
			return readDatabaseRelationships(ctx, db, targetGraph, afterID, hasAfterID, limit)
		},
		ID: func(relationship *graph.Relationship) graph.ID {
			return relationship.ID
		},
		Handle:      handle,
		LogProgress: logProgress,
	})
}
