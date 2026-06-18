package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

const defaultDatabaseScrubBatchSize = 1000

type DatabaseScrubOptions struct {
	Graph     string
	AllGraphs bool
	BatchSize int
	Progress  func(DatabaseScrubProgress)
}

type DatabaseScrubProgress struct {
	Entity    PlanEntity `json:"entity"`
	Processed int        `json:"processed"`
	Total     int        `json:"total"`
	Updated   int        `json:"updated"`
}

type DatabaseScrubSummary struct {
	StartedAt           time.Time `json:"started_at"`
	CompletedAt         time.Time `json:"completed_at"`
	Manifest            Manifest  `json:"manifest"`
	NodeUpdates         int       `json:"node_updates"`
	RelationshipUpdates int       `json:"relationship_updates"`
}

func (s Scrubber) ScrubDatabase(ctx context.Context, db graph.Database, options DatabaseScrubOptions) (DatabaseScrubSummary, error) {
	if options.AllGraphs {
		return DatabaseScrubSummary{}, fmt.Errorf("all-graphs database scrubbing is not supported by Dawgs' portable graph API; pass a specific -graph")
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultDatabaseScrubBatchSize
	}

	targetGraph := graph.Graph{Name: strings.TrimSpace(options.Graph)}
	if targetGraph.Name != "" {
		if err := db.SetDefaultGraph(ctx, targetGraph); err != nil {
			return DatabaseScrubSummary{}, fmt.Errorf("set graph target %q: %w", targetGraph.Name, err)
		}
	}

	startedAt := time.Now().UTC()
	summary := DatabaseScrubSummary{
		StartedAt: startedAt,
		Manifest: Manifest{
			GeneratedAt:         startedAt,
			Mode:                ModeShapeOnly,
			RulesVersion:        RulesVersion,
			SaltProvided:        s.config.Salt != "",
			NodeActions:         map[PropertyAction]int{},
			RelationshipActions: map[PropertyAction]int{},
			TopologyPreserved:   true,
			Notes: []string{
				"Live database scrub mutated properties only.",
				"Node IDs were preserved.",
				"Relationship endpoints were preserved.",
				"Node and relationship kinds were preserved.",
			},
		},
	}

	nodeCount, relationshipCount, err := countDatabaseGraphEntities(ctx, db, targetGraph)
	if err != nil {
		return DatabaseScrubSummary{}, err
	}
	summary.Manifest.NodeCount = int(nodeCount)
	summary.Manifest.RelationshipCount = int(relationshipCount)

	identifierRegistry, err := s.collectDatabaseGraphIdentifierRegistry(ctx, db, targetGraph, int(nodeCount), options.BatchSize)
	if err != nil {
		return DatabaseScrubSummary{}, err
	}

	nodeUpdates, err := s.scrubDatabaseNodes(ctx, db, targetGraph, int(nodeCount), options, summary.Manifest.NodeActions, identifierRegistry)
	if err != nil {
		return DatabaseScrubSummary{}, err
	}
	summary.NodeUpdates = nodeUpdates

	relationshipUpdates, err := s.scrubDatabaseRelationships(ctx, db, targetGraph, int(relationshipCount), options, summary.Manifest.RelationshipActions)
	if err != nil {
		return DatabaseScrubSummary{}, err
	}
	summary.RelationshipUpdates = relationshipUpdates

	summary.CompletedAt = time.Now().UTC()
	return summary, nil
}

func countDatabaseGraphEntities(ctx context.Context, db graph.Database, targetGraph graph.Graph) (int64, int64, error) {
	var nodeCount, relationshipCount int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if targetGraph.Name != "" {
			tx = tx.WithGraph(targetGraph)
		}

		var err error
		if nodeCount, err = tx.Nodes().Count(); err != nil {
			return fmt.Errorf("count nodes: %w", err)
		}
		if relationshipCount, err = tx.Relationships().Count(); err != nil {
			return fmt.Errorf("count relationships: %w", err)
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}

	return nodeCount, relationshipCount, nil
}

func (s Scrubber) collectDatabaseGraphIdentifierRegistry(ctx context.Context, db graph.Database, targetGraph graph.Graph, total, batchSize int) (*graphIdentifierRegistry, error) {
	registry := newGraphIdentifierRegistry()
	nodes := make([]graphIdentifierNode, 0, total)
	lastID := graph.ID(0)

	for {
		nodeBatch, err := readDatabaseNodes(ctx, db, targetGraph, lastID, batchSize)
		if err != nil {
			return nil, err
		}
		if len(nodeBatch) == 0 {
			break
		}

		for _, node := range nodeBatch {
			properties := node.Properties.MapOrEmpty()
			nextNode := graphIdentifierNode{
				databaseID: node.ID,
				kinds:      graphKindStrings(node.Kinds),
			}
			nextNode.objectID, _ = stringPropertyValue(properties[s.config.GraphRules.ObjectIDKey])
			nextNode.domainName, _ = stringPropertyValue(properties[s.config.GraphRules.DomainNameKey])
			s.collectGraphIdentifierDomain(nextNode, registry)
			nodes = append(nodes, nextNode)
		}

		lastID = nodeBatch[len(nodeBatch)-1].ID
	}

	for _, node := range nodes {
		s.collectGraphIdentifierObjectID(node, registry, true)
	}

	return registry, nil
}

func (s Scrubber) scrubDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int, options DatabaseScrubOptions, actionCounts map[PropertyAction]int, identifierRegistry *graphIdentifierRegistry) (int, error) {
	updated := 0
	processed := 0
	lastID := graph.ID(0)

	for {
		nodes, err := readDatabaseNodes(ctx, db, targetGraph, lastID, options.BatchSize)
		if err != nil {
			return updated, err
		}
		if len(nodes) == 0 {
			break
		}

		updates := make([]*graph.Node, 0, len(nodes))
		for _, node := range nodes {
			originalProperties := node.Properties.MapOrEmpty()
			scrubbedProperties, plans := s.ScrubProperties(originalProperties)
			s.applyGraphIdentifierRewrites(originalProperties, scrubbedProperties, identifierRegistry)
			for _, plan := range plans {
				actionCounts[plan.Action] += 1
			}
			if !reflect.DeepEqual(originalProperties, scrubbedProperties) {
				if updateProperties, changed := changedProperties(originalProperties, scrubbedProperties); changed {
					nodeUpdate := *node
					nodeUpdate.Properties = updateProperties
					updates = append(updates, &nodeUpdate)
				}
			}
		}

		if len(updates) > 0 {
			if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
				if targetGraph.Name != "" {
					tx = tx.WithGraph(targetGraph)
				}
				for _, node := range updates {
					if err := tx.UpdateNode(node); err != nil {
						return fmt.Errorf("update node %d: %w", node.ID.Uint64(), err)
					}
				}
				return nil
			}); err != nil {
				return updated, err
			}
		}

		updated += len(updates)
		processed += len(nodes)
		lastID = nodes[len(nodes)-1].ID
		if options.Progress != nil {
			options.Progress(DatabaseScrubProgress{
				Entity:    PlanEntityNode,
				Processed: min(processed, total),
				Total:     total,
				Updated:   updated,
			})
		}
	}

	return updated, nil
}

func (s Scrubber) scrubDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, total int, options DatabaseScrubOptions, actionCounts map[PropertyAction]int) (int, error) {
	updated := 0
	processed := 0
	lastID := graph.ID(0)

	for {
		relationships, err := readDatabaseRelationships(ctx, db, targetGraph, lastID, options.BatchSize)
		if err != nil {
			return updated, err
		}
		if len(relationships) == 0 {
			break
		}

		updates := make([]*graph.Relationship, 0, len(relationships))
		for _, relationship := range relationships {
			originalProperties := relationship.Properties.MapOrEmpty()
			scrubbedProperties, plans := s.ScrubProperties(originalProperties)
			for _, plan := range plans {
				actionCounts[plan.Action] += 1
			}
			if !reflect.DeepEqual(originalProperties, scrubbedProperties) {
				if updateProperties, changed := changedProperties(originalProperties, scrubbedProperties); changed {
					relationshipUpdate := *relationship
					relationshipUpdate.Properties = updateProperties
					updates = append(updates, &relationshipUpdate)
				}
			}
		}

		if len(updates) > 0 {
			if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
				if targetGraph.Name != "" {
					tx = tx.WithGraph(targetGraph)
				}
				for _, relationship := range updates {
					if err := tx.UpdateRelationship(relationship); err != nil {
						return fmt.Errorf("update relationship %d: %w", relationship.ID.Uint64(), err)
					}
				}
				return nil
			}); err != nil {
				return updated, err
			}
		}

		updated += len(updates)
		processed += len(relationships)
		lastID = relationships[len(relationships)-1].ID
		if options.Progress != nil {
			options.Progress(DatabaseScrubProgress{
				Entity:    PlanEntityRelationship,
				Processed: min(processed, total),
				Total:     total,
				Updated:   updated,
			})
		}
	}

	return updated, nil
}

func changedProperties(original, scrubbed map[string]any) (*graph.Properties, bool) {
	updates := graph.NewProperties()
	changed := false
	for key, scrubbedValue := range scrubbed {
		if !reflect.DeepEqual(original[key], scrubbedValue) {
			if scrubbedValue == nil {
				continue
			}
			updates.Set(key, scrubbedValue)
			changed = true
		}
	}
	return updates, changed
}

func readDatabaseNodes(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, batchSize int) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if targetGraph.Name != "" {
			tx = tx.WithGraph(targetGraph)
		}

		nodeQuery := tx.Nodes().
			Filter(query.GreaterThan(query.NodeID(), afterID)).
			OrderBy(query.NodeID()).
			Limit(batchSize)

		return nodeQuery.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				nodes = append(nodes, node)
			}
			return cursor.Error()
		})
	}); err != nil {
		return nil, fmt.Errorf("read node batch after ID %d: %w", afterID.Uint64(), err)
	}
	return nodes, nil
}

func readDatabaseRelationships(ctx context.Context, db graph.Database, targetGraph graph.Graph, afterID graph.ID, batchSize int) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if targetGraph.Name != "" {
			tx = tx.WithGraph(targetGraph)
		}

		relationshipQuery := tx.Relationships().
			Filter(query.GreaterThan(query.RelationshipID(), afterID)).
			OrderBy(query.RelationshipID()).
			Limit(batchSize)

		return relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for relationship := range cursor.Chan() {
				relationships = append(relationships, relationship)
			}
			return cursor.Error()
		})
	}); err != nil {
		return nil, fmt.Errorf("read relationship batch after ID %d: %w", afterID.Uint64(), err)
	}
	return relationships, nil
}
