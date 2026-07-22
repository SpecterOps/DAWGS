package retriever

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type entityCursorReader[T any] func(afterID graph.ID, hasAfterID bool, batchSize int, visit func(T) error) error
type entityHandler[T any] func(T) error
type entityIDFunc[T any] func(T) graph.ID
type entityProgressLogger func(processed int64, startedAt time.Time, nextProgressAt int64) int64

type ScanBatchEvent struct {
	Count       int
	Processed   int64
	ReadElapsed time.Duration
}

type entityScanOptions[T any] struct {
	Context          context.Context
	Total            int64
	BatchSize        int
	ProgressInterval int64
	EntityName       string
	Read             entityCursorReader[T]
	ID               entityIDFunc[T]
	Handle           entityHandler[T]
	BatchComplete    func(ScanBatchEvent) error
	LogProgress      entityProgressLogger
	StartAfterID     graph.ID
	HasStartAfterID  bool
	AlreadyProcessed int64
}

func scanEntityBatches[T any](options entityScanOptions[T]) (int64, error) {
	entityName := options.EntityName
	if entityName == "" {
		entityName = "entity"
	}

	if options.Total <= 0 {
		return 0, nil
	}

	if options.AlreadyProcessed < 0 || options.AlreadyProcessed > options.Total {
		return 0, fmt.Errorf("%s already processed count %d is outside total %d", entityName, options.AlreadyProcessed, options.Total)
	}
	if options.AlreadyProcessed > 0 && !options.HasStartAfterID {
		return 0, fmt.Errorf("%s resume cursor is required after %d processed entities", entityName, options.AlreadyProcessed)
	}

	if options.BatchSize <= 0 {
		return 0, fmt.Errorf("%s batch size must be > 0", entityName)
	}

	if options.Read == nil {
		return 0, fmt.Errorf("%s cursor reader is required", entityName)
	}

	if options.ID == nil {
		return 0, fmt.Errorf("%s ID accessor is required", entityName)
	}

	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		lastID    = options.StartAfterID
		hasLastID = options.HasStartAfterID
		processed = options.AlreadyProcessed

		startedAt      = time.Now()
		nextProgressAt = retrieverInitialProgressAtInterval(options.Total, options.ProgressInterval)
	)

	for processed < options.Total {
		if err := ctx.Err(); err != nil {
			return processed, err
		}

		requested := retrieverBatchLimit(options.Total-processed, options.BatchSize)
		accepted := 0
		readStartedAt := time.Now()
		err := options.Read(lastID, hasLastID, requested, func(entity T) error {
			if accepted >= requested {
				return fmt.Errorf("%s database cursor exceeded requested limit %d", entityName, requested)
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			nextID := options.ID(entity)
			if hasLastID && nextID <= lastID {
				return fmt.Errorf("%s keyset scan is not strictly increasing: ID %d followed ID %d", entityName, nextID.Uint64(), lastID.Uint64())
			}

			if options.Handle != nil {
				if err := options.Handle(entity); err != nil {
					return err
				}
			}

			lastID = nextID
			hasLastID = true
			accepted++
			processed++

			return nil
		})
		readElapsed := time.Since(readStartedAt)
		if err != nil {
			return processed, err
		}

		if options.BatchComplete != nil {
			if err := options.BatchComplete(ScanBatchEvent{
				Count:       accepted,
				Processed:   processed,
				ReadElapsed: readElapsed,
			}); err != nil {
				return processed, err
			}
		}

		if options.LogProgress != nil {
			nextProgressAt = options.LogProgress(processed, startedAt, nextProgressAt)
		}

		if accepted < requested && processed < options.Total {
			return processed, fmt.Errorf("%s keyset scan ended after %d of %d entities: requested %d records after ID %d but received %d", entityName, processed, options.Total, requested, lastID.Uint64(), accepted)
		}
	}

	return processed, nil
}

func visitBoundedCursor[T any](ctx context.Context, cursor graph.Cursor[T], entityName string, limit int, visit func(T) error) (int, error) {
	if limit <= 0 {
		cursor.Close()
		return 0, fmt.Errorf("%s cursor limit must be > 0", entityName)
	}

	count := 0
	for {
		select {
		case <-ctx.Done():
			cursor.Close()
			return count, ctx.Err()

		case entity, ok := <-cursor.Chan():
			if !ok {
				return count, cursor.Error()
			}

			if count >= limit {
				cursor.Close()
				return count, fmt.Errorf("%s database cursor exceeded requested limit %d", entityName, limit)
			}

			if visit != nil {
				if err := visit(entity); err != nil {
					cursor.Close()
					return count, err
				}
			}

			count++
		}
	}
}

func ScanDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, handle func(*graph.Node) error, batchComplete func(ScanBatchEvent) error) (int64, error) {
	return scanDatabaseNodesWithProgressInterval(ctx, db, targetGraph, total, batchSize, 0, handle, batchComplete, nil)
}

func scanDatabaseNodesWithProgressInterval(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, handle entityHandler[*graph.Node], batchComplete func(ScanBatchEvent) error, logProgress entityProgressLogger) (int64, error) {
	return scanDatabaseNodesFrom(ctx, db, targetGraph, total, batchSize, progressInterval, 0, false, 0, handle, batchComplete, logProgress)
}

func scanDatabaseNodesFrom(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, afterID graph.ID, hasAfterID bool, alreadyProcessed int64, handle entityHandler[*graph.Node], batchComplete func(ScanBatchEvent) error, logProgress entityProgressLogger) (int64, error) {
	return scanEntityBatches(entityScanOptions[*graph.Node]{
		Context:          ctx,
		Total:            total,
		BatchSize:        batchSize,
		ProgressInterval: progressInterval,
		EntityName:       "node",
		Read: func(afterID graph.ID, hasAfterID bool, limit int, visit func(*graph.Node) error) error {
			return readDatabaseNodes(ctx, db, targetGraph, afterID, hasAfterID, limit, visit)
		},
		ID: func(node *graph.Node) graph.ID {
			return node.ID
		},
		Handle:           handle,
		BatchComplete:    batchComplete,
		LogProgress:      logProgress,
		StartAfterID:     afterID,
		HasStartAfterID:  hasAfterID,
		AlreadyProcessed: alreadyProcessed,
	})
}

func ScanDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, handle func(*graph.Relationship) error, batchComplete func(ScanBatchEvent) error) (int64, error) {
	return scanDatabaseRelationshipsWithProgressInterval(ctx, db, targetGraph, total, batchSize, 0, handle, batchComplete, nil)
}

func scanDatabaseRelationshipsWithProgressInterval(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, handle entityHandler[*graph.Relationship], batchComplete func(ScanBatchEvent) error, logProgress entityProgressLogger) (int64, error) {
	return scanDatabaseRelationshipsFrom(ctx, db, targetGraph, total, batchSize, progressInterval, 0, false, 0, handle, batchComplete, logProgress)
}

func scanDatabaseRelationshipsFrom(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, afterID graph.ID, hasAfterID bool, alreadyProcessed int64, handle entityHandler[*graph.Relationship], batchComplete func(ScanBatchEvent) error, logProgress entityProgressLogger) (int64, error) {
	return scanEntityBatches(entityScanOptions[*graph.Relationship]{
		Context:          ctx,
		Total:            total,
		BatchSize:        batchSize,
		ProgressInterval: progressInterval,
		EntityName:       "relationship",
		Read: func(afterID graph.ID, hasAfterID bool, limit int, visit func(*graph.Relationship) error) error {
			return readDatabaseRelationships(ctx, db, targetGraph, afterID, hasAfterID, limit, visit)
		},
		ID: func(relationship *graph.Relationship) graph.ID {
			return relationship.ID
		},
		Handle:           handle,
		BatchComplete:    batchComplete,
		LogProgress:      logProgress,
		StartAfterID:     afterID,
		HasStartAfterID:  hasAfterID,
		AlreadyProcessed: alreadyProcessed,
	})
}

func readDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int, visit func(*graph.Node) error) error {
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		nodeQuery := tx.Nodes().OrderBy(query.NodeID()).Limit(batchSize)

		if hasAfterID {
			nodeQuery = nodeQuery.Filter(query.GreaterThan(query.NodeID(), afterID))
		}

		return nodeQuery.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			_, err := visitBoundedCursor(ctx, cursor, "node", batchSize, visit)
			return err
		})
	})
	if err == nil {
		return nil
	}

	if hasAfterID {
		return fmt.Errorf("read node batch after ID %d: %w", afterID.Uint64(), err)
	}

	return fmt.Errorf("read initial node batch: %w", err)
}

func readDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int, visit func(*graph.Relationship) error) error {
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		relationshipQuery := tx.Relationships().OrderBy(query.RelationshipID()).Limit(batchSize)

		if hasAfterID {
			relationshipQuery = relationshipQuery.Filter(query.GreaterThan(query.RelationshipID(), afterID))
		}

		return relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			_, err := visitBoundedCursor(ctx, cursor, "relationship", batchSize, visit)
			return err
		})
	})
	if err == nil {
		return nil
	}

	if hasAfterID {
		return fmt.Errorf("read relationship batch after ID %d: %w", afterID.Uint64(), err)
	}

	return fmt.Errorf("read initial relationship batch: %w", err)
}
