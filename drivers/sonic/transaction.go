package sonic

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

type transaction struct {
	db  *Database
	ctx context.Context
}

func (tx *transaction) WithGraph(graphSchema graph.Graph) graph.Transaction {
	return tx
}

func (tx *transaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	id := tx.db.newID()

	node := &graph.Node{
		ID:         id,
		Kinds:      kinds,
		Properties: properties,
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	tx.db.nodes[id] = node
	return node, nil
}

func (tx *transaction) UpdateNode(node *graph.Node) error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if _, ok := tx.db.nodes[node.ID]; !ok {
		return fmt.Errorf("node %d not found", node.ID)
	}
	tx.db.nodes[node.ID] = node
	return nil
}

func (tx *transaction) Nodes() graph.NodeQuery {
	return &nodeQuery{db: tx.db}
}

func (tx *transaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if _, ok := tx.db.nodes[startNodeID]; !ok {
		return nil, fmt.Errorf("start node %d not found", startNodeID)
	}
	if _, ok := tx.db.nodes[endNodeID]; !ok {
		return nil, fmt.Errorf("end node %d not found", endNodeID)
	}

	id := tx.db.newID()
	rel := &graph.Relationship{
		ID:         id,
		StartID:    startNodeID,
		EndID:      endNodeID,
		Kind:       kind,
		Properties: properties,
	}

	tx.db.edges[id] = rel
	tx.db.outEdges[startNodeID] = append(tx.db.outEdges[startNodeID], id)
	tx.db.inEdges[endNodeID] = append(tx.db.inEdges[endNodeID], id)
	return rel, nil
}

func (tx *transaction) UpdateRelationship(relationship *graph.Relationship) error {
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if _, ok := tx.db.edges[relationship.ID]; !ok {
		return fmt.Errorf("relationship %d not found", relationship.ID)
	}
	tx.db.edges[relationship.ID] = relationship
	return nil
}

func (tx *transaction) Relationships() graph.RelationshipQuery {
	return &relQuery{db: tx.db}
}

func (tx *transaction) Raw(query string, parameters map[string]any) graph.Result {
	// Raw SQL doesn't apply to sonic — treat it as a Cypher query
	return tx.Query(query, parameters)
}

func (tx *transaction) Query(queryStr string, parameters map[string]any) graph.Result {
	parsedQuery, err := frontend.ParseCypher(frontend.NewContext(), queryStr)
	if err != nil {
		slog.Error("sonic: failed to parse cypher", slog.String("query", queryStr), slog.String("error", err.Error()))
		return graph.NewErrorResult(fmt.Errorf("sonic: failed to parse cypher: %w", err))
	}

	result, err := tx.db.executeCypher(parsedQuery, parameters)
	if err != nil {
		slog.Error("sonic: cypher execution failed", slog.String("query", queryStr), slog.String("error", err.Error()))
		return graph.NewErrorResult(err)
	}

	slog.Info("sonic: cypher query executed", slog.String("query", queryStr), slog.Int("rows", len(result.rows)), slog.Any("keys", result.keys))
	return result
}

func (tx *transaction) Commit() error {
	return nil
}

func (tx *transaction) GraphQueryMemoryLimit() size.Size {
	return tx.db.queryMemoryLimit
}
