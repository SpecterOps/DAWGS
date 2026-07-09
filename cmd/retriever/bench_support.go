package main

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

const retrieverProgressEntityInterval int64 = 250_000

type graphEntitySnapshot struct {
	NodeCount int64
	EdgeCount int64
}

type entityBatchReader[T any] func(afterID graph.ID, hasAfterID bool, batchSize int) ([]T, error)
type entityBatchHandler[T any] func([]T) error
type entityIDFunc[T any] func(T) graph.ID
type entityProgressLogger func(processed int64, startedAt time.Time, nextProgressAt int64) int64

type entityScanOptions[T any] struct {
	Total       int64
	BatchSize   int
	EntityName  string
	Read        entityBatchReader[T]
	ID          entityIDFunc[T]
	Handle      entityBatchHandler[T]
	LogProgress entityProgressLogger
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
		nextProgressAt = retrieverInitialProgressAt(options.Total)
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

func readDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		nodeQuery := tx.Nodes().
			OrderBy(query.NodeID()).
			Limit(batchSize)

		if hasAfterID {
			nodeQuery = nodeQuery.Filter(query.GreaterThan(query.NodeID(), afterID))
		}

		return nodeQuery.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				nodes = append(nodes, node)
			}

			return cursor.Error()
		})
	}); err != nil {
		if hasAfterID {
			return nil, fmt.Errorf("read node batch after ID %d: %w", afterID.Uint64(), err)
		}

		return nil, fmt.Errorf("read initial node batch: %w", err)
	}

	return nodes, nil
}

func readDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)
		relationshipQuery := tx.Relationships().
			OrderBy(query.RelationshipID()).
			Limit(batchSize)

		if hasAfterID {
			relationshipQuery = relationshipQuery.Filter(query.GreaterThan(query.RelationshipID(), afterID))
		}

		return relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for relationship := range cursor.Chan() {
				relationships = append(relationships, relationship)
			}

			return cursor.Error()
		})
	}); err != nil {
		if hasAfterID {
			return nil, fmt.Errorf("read relationship batch after ID %d: %w", afterID.Uint64(), err)
		}

		return nil, fmt.Errorf("read initial relationship batch: %w", err)
	}

	return relationships, nil
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
