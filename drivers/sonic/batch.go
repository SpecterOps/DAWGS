package sonic

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type batch struct {
	db  *Database
	ctx context.Context
}

func (b *batch) WithGraph(graphSchema graph.Graph) graph.Batch {
	return b
}

func (b *batch) CreateNode(node *graph.Node) error {
	id := b.db.newID()
	node.ID = id

	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	b.db.nodes[id] = node
	return nil
}

func (b *batch) DeleteNode(id graph.ID) error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	for _, edgeID := range b.db.outEdges[id] {
		delete(b.db.edges, edgeID)
	}
	for _, edgeID := range b.db.inEdges[id] {
		delete(b.db.edges, edgeID)
	}
	delete(b.db.outEdges, id)
	delete(b.db.inEdges, id)
	delete(b.db.nodes, id)
	return nil
}

func (b *batch) Nodes() graph.NodeQuery {
	return &nodeQuery{db: b.db}
}

func (b *batch) Relationships() graph.RelationshipQuery {
	return &relQuery{db: b.db}
}

func (b *batch) UpdateNodeBy(update graph.NodeUpdate) error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	// Try to find an existing node that matches the identity criteria
	for _, existing := range b.db.nodes {
		if !existing.Kinds.ContainsOneOf(update.IdentityKind) {
			continue
		}

		if matchesIdentity(existing.Properties, update.Node.Properties, update.IdentityProperties) {
			// Update existing node: merge kinds and properties
			for _, kind := range update.Node.Kinds {
				if !existing.Kinds.ContainsOneOf(kind) {
					existing.Kinds = append(existing.Kinds, kind)
				}
			}
			if update.Node.Properties != nil {
				for key, val := range update.Node.Properties.Map {
					existing.Properties.Set(key, val)
				}
			}
			return nil
		}
	}

	// No match — create new node (inline to avoid double-lock)
	id := b.db.newID()
	update.Node.ID = id
	b.db.nodes[id] = update.Node
	return nil
}

func (b *batch) CreateRelationship(relationship *graph.Relationship) error {
	id := b.db.newID()
	relationship.ID = id

	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	b.db.edges[id] = relationship
	b.db.outEdges[relationship.StartID] = append(b.db.outEdges[relationship.StartID], id)
	b.db.inEdges[relationship.EndID] = append(b.db.inEdges[relationship.EndID], id)
	return nil
}

func (b *batch) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	if _, ok := b.db.nodes[startNodeID]; !ok {
		return fmt.Errorf("start node %d not found", startNodeID)
	}
	if _, ok := b.db.nodes[endNodeID]; !ok {
		return fmt.Errorf("end node %d not found", endNodeID)
	}

	id := b.db.newID()
	rel := &graph.Relationship{
		ID:         id,
		StartID:    startNodeID,
		EndID:      endNodeID,
		Kind:       kind,
		Properties: properties,
	}

	b.db.edges[id] = rel
	b.db.outEdges[startNodeID] = append(b.db.outEdges[startNodeID], id)
	b.db.inEdges[endNodeID] = append(b.db.inEdges[endNodeID], id)
	return nil
}

func (b *batch) DeleteRelationship(id graph.ID) error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	rel, ok := b.db.edges[id]
	if !ok {
		return fmt.Errorf("relationship %d not found", id)
	}

	delete(b.db.edges, id)

	// Clean up adjacency indexes
	b.db.outEdges[rel.StartID] = removeID(b.db.outEdges[rel.StartID], id)
	b.db.inEdges[rel.EndID] = removeID(b.db.inEdges[rel.EndID], id)
	return nil
}

func (b *batch) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	rel := update.Relationship

	// Try to find an existing relationship that matches
	for _, existing := range b.db.edges {
		if existing.Kind != rel.Kind {
			continue
		}
		if existing.StartID != rel.StartID || existing.EndID != rel.EndID {
			continue
		}
		if matchesIdentity(existing.Properties, rel.Properties, update.IdentityProperties) {
			// Update existing relationship properties
			if rel.Properties != nil {
				for key, val := range rel.Properties.Map {
					existing.Properties.Set(key, val)
				}
			}
			return nil
		}
	}

	// No match — create new relationship (inline to avoid double-lock)
	id := b.db.newID()
	rel.ID = id
	b.db.edges[id] = rel
	b.db.outEdges[rel.StartID] = append(b.db.outEdges[rel.StartID], id)
	b.db.inEdges[rel.EndID] = append(b.db.inEdges[rel.EndID], id)
	return nil
}

func (b *batch) Commit() error {
	return nil
}

func matchesIdentity(existing, candidate *graph.Properties, identityKeys []string) bool {
	if len(identityKeys) == 0 {
		return false
	}
	for _, key := range identityKeys {
		existingVal := existing.Get(key).Any()
		candidateVal := candidate.Get(key).Any()
		if existingVal == nil || candidateVal == nil {
			return false
		}
		if fmt.Sprint(existingVal) != fmt.Sprint(candidateVal) {
			return false
		}
	}
	return true
}

func removeID(ids []graph.ID, target graph.ID) []graph.ID {
	for i, id := range ids {
		if id == target {
			return append(ids[:i], ids[i+1:]...)
		}
	}
	return ids
}
