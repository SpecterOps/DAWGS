package dawgs

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/ret/entity"
)

type Source struct {
	database           graph.Database
	graph              graph.Graph
	batchSize          int
	nodeCursor         uint64
	relationshipCursor uint64
}

func NewSource(database graph.Database, graphName string, batchSize int) (*Source, error) {
	if strings.TrimSpace(graphName) == "" {
		return nil, fmt.Errorf("graph name is required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}

	return &Source{
		database:  database,
		graph:     graph.Graph{Name: graphName},
		batchSize: batchSize,
	}, nil
}

func (s *Source) Snapshot(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot
	if err := s.database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(s.graph)

		var err error
		if snapshot.NodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("snapshot nodes for graph %q: %w", s.graph.Name, err)
		}
		if snapshot.RelationshipCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("snapshot relationships for graph %q: %w", s.graph.Name, err)
		}
		return nil
	}); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot graph %q: %w", s.graph.Name, err)
	}

	return snapshot, nil
}

func (s *Source) SetNodeCursor(lastID uint64) {
	s.nodeCursor = lastID
}

func (s *Source) SetRelationshipCursor(lastID uint64) {
	s.relationshipCursor = lastID
}

func (s *Source) NextNodes(ctx context.Context) (NodeBatch, error) {
	batch := NodeBatch{}
	if err := s.database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(s.graph)
		return tx.Nodes().
			OrderBy(query.NodeID()).
			Filter(query.GreaterThan(query.NodeID(), graph.ID(s.nodeCursor))).
			Limit(s.batchSize).
			Fetch(func(cursor graph.Cursor[*graph.Node]) error {
				for node := range cursor.Chan() {
					batch.Entities = append(batch.Entities, entity.Node{
						SourceID:   strconv.FormatUint(node.ID.Uint64(), 10),
						Kinds:      copyKinds(node.Kinds),
						Properties: copyProperties(node.Properties),
					})
					batch.LastID = node.ID.Uint64()
				}
				return cursor.Error()
			})
	}); err != nil {
		return NodeBatch{}, fmt.Errorf("read nodes for graph %q: %w", s.graph.Name, err)
	}

	if len(batch.Entities) > 0 {
		s.nodeCursor = batch.LastID
	}
	return batch, nil
}

func (s *Source) NextRelationships(ctx context.Context) (RelationshipBatch, error) {
	batch := RelationshipBatch{}
	if err := s.database.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(s.graph)
		return tx.Relationships().
			OrderBy(query.RelationshipID()).
			Filter(query.GreaterThan(query.RelationshipID(), graph.ID(s.relationshipCursor))).
			Limit(s.batchSize).
			Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
				for relationship := range cursor.Chan() {
					batch.Entities = append(batch.Entities, entity.Relationship{
						SourceID:   strconv.FormatUint(relationship.ID.Uint64(), 10),
						StartID:    strconv.FormatUint(relationship.StartID.Uint64(), 10),
						EndID:      strconv.FormatUint(relationship.EndID.Uint64(), 10),
						Kind:       relationship.Kind.String(),
						Properties: copyProperties(relationship.Properties),
					})
					batch.LastID = relationship.ID.Uint64()
				}
				return cursor.Error()
			})
	}); err != nil {
		return RelationshipBatch{}, fmt.Errorf("read relationships for graph %q: %w", s.graph.Name, err)
	}

	if len(batch.Entities) > 0 {
		s.relationshipCursor = batch.LastID
	}
	return batch, nil
}

func copyKinds(kinds graph.Kinds) []string {
	converted := make([]string, len(kinds))
	for index, kind := range kinds {
		converted[index] = kind.String()
	}
	return converted
}

func copyProperties(properties *graph.Properties) map[string]any {
	if properties == nil || properties.Map == nil {
		return nil
	}
	return entity.CloneProperties(properties.Map)
}
