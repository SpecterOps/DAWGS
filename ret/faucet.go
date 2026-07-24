package ret

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type graphFaucet struct {
	db          graph.Database
	targetGraph graph.Graph
	batchSize   int

	lastNodeID    graph.ID
	hasLastNodeID bool

	lastRelID    graph.ID
	hasLastRelID bool
}

func newGraphFaucet(db graph.Database, targetGraph graph.Graph, batchSize int) graphFaucet {
	return graphFaucet{
		db:          db,
		targetGraph: targetGraph,
		batchSize:   batchSize,
	}
}

func (s *graphFaucet) NextNodeBatch(ctx context.Context) ([]*graph.Node, error) {
	var nodes []*graph.Node
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(s.targetGraph)
		nodeQuery := tx.Nodes().
			OrderBy(query.NodeID()).
			Limit(s.batchSize)

		if s.hasLastNodeID {
			nodeQuery = nodeQuery.Filter(query.GreaterThan(query.NodeID(), s.lastNodeID))
		}

		return nodeQuery.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				nodes = append(nodes, node)
			}

			return cursor.Error()
		})
	}); err != nil {
		return nil, err
	}

	if len(nodes) > 1 {
		s.lastNodeID = nodes[len(nodes)-1].ID
		s.hasLastNodeID = true
	}

	return nodes, nil
}

func (s *graphFaucet) NextRelationshipBatch(ctx context.Context) ([]*graph.Relationship, error) {
	var rels []*graph.Relationship
	if err := s.db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(s.targetGraph)
		relQuery := tx.Relationships().
			OrderBy(query.RelationshipID()).
			Limit(s.batchSize)

		if s.hasLastRelID {
			relQuery = relQuery.Filter(query.GreaterThan(query.RelationshipID(), s.lastRelID))
		}

		return relQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for rel := range cursor.Chan() {
				rels = append(rels, rel)
			}

			return cursor.Error()
		})
	}); err != nil {
		return nil, err
	}

	if len(rels) > 1 {
		s.lastRelID = rels[len(rels)-1].ID
		s.hasLastRelID = true
	}

	return rels, nil
}
