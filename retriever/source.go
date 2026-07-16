package retriever

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type graphEntitySnapshot struct {
	NodeCount int64
	EdgeCount int64
}

type faucet[T any] interface {
	Run(context.Context, entityBatchHandler[T]) (int64, error)
}

type graphSource interface {
	Inventory(context.Context, graph.Graph) (graphEntitySnapshot, error)
	Nodes(graph.Graph, int64, int, int64, entityProgressLogger) faucet[*graph.Node]
	Edges(graph.Graph, int64, int, int64, entityProgressLogger) faucet[*graph.Relationship]
}

type databaseGraphSource struct {
	db graph.Database
}

func newDatabaseGraphSource(db graph.Database) databaseGraphSource {
	return databaseGraphSource{db: db}
}

func (s databaseGraphSource) Inventory(ctx context.Context, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	var snapshot graphEntitySnapshot
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(targetGraph)

		var err error
		if snapshot.NodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}

		if snapshot.EdgeCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}

		return nil
	}); err != nil {
		return graphEntitySnapshot{}, err
	}

	return snapshot, nil
}

func (s databaseGraphSource) Nodes(targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, logProgress entityProgressLogger) faucet[*graph.Node] {
	return keysetFaucet[*graph.Node]{
		total:            total,
		batchSize:        batchSize,
		progressInterval: progressInterval,
		entityName:       "node",
		read: func(ctx context.Context, afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Node, error) {
			return s.readNodes(ctx, targetGraph, afterID, hasAfterID, limit)
		},
		id: func(node *graph.Node) graph.ID {
			return node.ID
		},
		logProgress: logProgress,
	}
}

func (s databaseGraphSource) Edges(targetGraph graph.Graph, total int64, batchSize int, progressInterval int64, logProgress entityProgressLogger) faucet[*graph.Relationship] {
	return keysetFaucet[*graph.Relationship]{
		total:            total,
		batchSize:        batchSize,
		progressInterval: progressInterval,
		entityName:       "relationship",
		read: func(ctx context.Context, afterID graph.ID, hasAfterID bool, limit int) ([]*graph.Relationship, error) {
			return s.readRelationships(ctx, targetGraph, afterID, hasAfterID, limit)
		},
		id: func(relationship *graph.Relationship) graph.ID {
			return relationship.ID
		},
		logProgress: logProgress,
	}
}

type keysetFaucet[T any] struct {
	total            int64
	batchSize        int
	progressInterval int64
	entityName       string
	read             func(context.Context, graph.ID, bool, int) ([]T, error)
	id               entityIDFunc[T]
	logProgress      entityProgressLogger
}

func (s keysetFaucet[T]) Run(ctx context.Context, handle entityBatchHandler[T]) (int64, error) {
	return scanEntityBatches(entityScanOptions[T]{
		Total:            s.total,
		BatchSize:        s.batchSize,
		ProgressInterval: s.progressInterval,
		EntityName:       s.entityName,
		Read: func(afterID graph.ID, hasAfterID bool, limit int) ([]T, error) {
			return s.read(ctx, afterID, hasAfterID, limit)
		},
		ID:          s.id,
		Handle:      handle,
		LogProgress: s.logProgress,
	})
}

func (s databaseGraphSource) readNodes(ctx context.Context, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
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

func (s databaseGraphSource) readRelationships(ctx context.Context, targetGraph graph.Graph, afterID graph.ID, hasAfterID bool, batchSize int) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
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

func countGraphEntitySnapshot(ctx context.Context, db graph.Database, targetGraph graph.Graph) (graphEntitySnapshot, error) {
	return newDatabaseGraphSource(db).Inventory(ctx, targetGraph)
}
