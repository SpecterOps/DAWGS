package dawgs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/entity"
)

// ErrTargetNotEmpty indicates that an emptiness snapshot completed and found
// at least one node or relationship.
var ErrTargetNotEmpty = errors.New("target graph is not empty")

// Target writes canonical entities to one empty DAWGS graph.
type Target struct {
	database  graph.Database
	graphName string
	batchSize int
	source    *Source
}

func NewTarget(database graph.Database, graphName string, batchSize int) (*Target, error) {
	if strings.TrimSpace(graphName) == "" {
		return nil, fmt.Errorf("graph name is required")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}

	source, err := NewSource(database, graphName, batchSize)
	if err != nil {
		return nil, err
	}

	return &Target{
		database:  database,
		graphName: graphName,
		batchSize: batchSize,
		source:    source,
	}, nil
}

func (s *Target) RequireEmpty(ctx context.Context) error {
	snapshot, err := s.source.Snapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot.NodeCount != 0 || snapshot.RelationshipCount != 0 {
		return fmt.Errorf(
			"%w: graph %q is not empty: nodes=%d relationships=%d",
			ErrTargetNotEmpty,
			s.graphName,
			snapshot.NodeCount,
			snapshot.RelationshipCount,
		)
	}
	return nil
}

func (s *Target) AssertSchema(ctx context.Context, kindCatalog []string) error {
	seen := make(map[string]struct{}, len(kindCatalog))
	kinds := make(graph.Kinds, 0, len(kindCatalog))
	for _, kind := range kindCatalog {
		if _, found := seen[kind]; found {
			continue
		}
		seen[kind] = struct{}{}
		kinds = append(kinds, graph.StringKind(kind))
	}

	targetGraph := graph.Graph{
		Name:  s.graphName,
		Nodes: kinds,
		Edges: kinds.Copy(),
	}
	if err := s.database.AssertSchema(ctx, graph.Schema{
		Graphs:       []graph.Graph{targetGraph},
		DefaultGraph: targetGraph,
	}); err != nil {
		return fmt.Errorf("assert schema for graph %q: %w", s.graphName, err)
	}
	return nil
}

func (s *Target) CreateNodes(ctx context.Context, nodes []entity.Node, resolver *Resolver) error {
	if resolver == nil {
		return errors.New("source ID resolver is required")
	}
	if len(nodes) == 0 {
		return nil
	}
	if err := s.preflightNodeSourceIDs(nodes, resolver); err != nil {
		return err
	}

	staged := make([]resolvedNode, len(nodes))
	if err := s.database.BatchOperation(ctx, func(batch graph.Batch) error {
		batch = batch.WithGraph(graph.Graph{Name: s.graphName})
		creator, ok := batch.(graph.NodeBatchCreator)
		if !ok {
			return errors.New("database batch does not support correlated node creation")
		}

		graphNodes := make([]*graph.Node, len(nodes))
		for index, value := range nodes {
			graphNodes[index] = graph.NewNode(
				0,
				graph.AsProperties(entity.CloneProperties(value.Properties)),
				graph.StringsToKinds(entity.CloneKinds(value.Kinds))...,
			)
		}

		destinationIDs, err := creator.CreateNodes(graphNodes)
		if err != nil {
			return err
		}
		if len(destinationIDs) != len(nodes) {
			return fmt.Errorf("created node ID count: got %d want %d", len(destinationIDs), len(nodes))
		}
		for index, value := range nodes {
			staged[index] = resolvedNode{sourceID: value.SourceID, destinationID: destinationIDs[index]}
		}
		return nil
	}, graph.WithBatchSize(s.batchSize)); err != nil {
		return err
	}

	for _, value := range staged {
		if !resolver.Put(value.sourceID, value.destinationID) {
			return fmt.Errorf("duplicate source node ID %q in graph %q", value.sourceID, s.graphName)
		}
	}
	return nil
}

type resolvedNode struct {
	sourceID      string
	destinationID graph.ID
}

func (s *Target) preflightNodeSourceIDs(nodes []entity.Node, resolver *Resolver) error {
	pending := NewResolver(int64(len(nodes)))
	for _, value := range nodes {
		if _, found := resolver.Resolve(value.SourceID); found || !pending.Put(value.SourceID, 0) {
			return fmt.Errorf("duplicate source node ID %q in graph %q", value.SourceID, s.graphName)
		}
	}
	return nil
}

func (s *Target) CreateRelationships(ctx context.Context, relationships []entity.Relationship, resolver *Resolver) error {
	if resolver == nil {
		return errors.New("source ID resolver is required")
	}
	if len(relationships) == 0 {
		return nil
	}

	return s.database.BatchOperation(ctx, func(batch graph.Batch) error {
		batch = batch.WithGraph(graph.Graph{Name: s.graphName})
		for index, value := range relationships {
			startID, startOK := resolver.Resolve(value.StartID)
			endID, endOK := resolver.Resolve(value.EndID)
			if !startOK || !endOK {
				return fmt.Errorf(
					"graph %q relationship %d has unresolved endpoints %q -> %q",
					s.graphName,
					index,
					value.StartID,
					value.EndID,
				)
			}
			if err := batch.CreateRelationshipByIDs(
				startID,
				endID,
				graph.StringKind(value.Kind),
				graph.AsProperties(entity.CloneProperties(value.Properties)),
			); err != nil {
				return err
			}
		}
		return nil
	}, graph.WithBatchSize(s.batchSize))
}
