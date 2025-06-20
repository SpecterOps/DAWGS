package neo4j

import (
	"context"

	neo4j_core "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/query/neo4j"
)

func newPath(internalPath neo4j_core.Path) graph.Path {
	path := graph.Path{}

	for _, node := range internalPath.Nodes {
		path.Nodes = append(path.Nodes, newNode(node))
	}

	for _, relationship := range internalPath.Relationships {
		path.Edges = append(path.Edges, newRelationship(relationship))
	}

	return path
}

func newNode(internalNode neo4j_core.Node) *graph.Node {
	var propertiesInst = internalNode.Props

	if propertiesInst == nil {
		propertiesInst = make(map[string]any)
	}

	return graph.NewNode(graph.ID(internalNode.Id), graph.AsProperties(propertiesInst), graph.StringsToKinds(internalNode.Labels)...)
}

type NodeQuery struct {
	ctx          context.Context
	tx           innerTransaction
	queryBuilder *neo4j.QueryBuilder
}

func NewNodeQuery(ctx context.Context, tx innerTransaction) graph.NodeQuery {
	return &NodeQuery{
		ctx:          ctx,
		tx:           tx,
		queryBuilder: neo4j.NewEmptyQueryBuilder(),
	}
}

func (s *NodeQuery) run(statement string, parameters map[string]any) graph.Result {
	return s.tx.Raw(statement, parameters)
}

func (s *NodeQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	for _, criteria := range finalCriteria {
		s.queryBuilder.Apply(criteria)
	}

	if err := s.queryBuilder.Prepare(); err != nil {
		return err
	} else if statement, err := s.queryBuilder.Render(); err != nil {
		return err
	} else if result := s.run(statement, s.queryBuilder.Parameters); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s *NodeQuery) Debug() (string, map[string]any) {
	statement, _ := s.queryBuilder.Render()
	return statement, s.queryBuilder.Parameters
}

func (s *NodeQuery) Delete() error {
	s.queryBuilder.Apply(query.Delete(
		query.Node(),
	))

	if err := s.queryBuilder.Prepare(); err != nil {
		return err
	} else if statement, err := s.queryBuilder.Render(); err != nil {
		return err
	} else {
		result := s.run(statement, s.queryBuilder.Parameters)
		return result.Error()
	}
}

func (s *NodeQuery) OrderBy(criteria ...graph.Criteria) graph.NodeQuery {
	s.queryBuilder.Apply(query.OrderBy(criteria...))
	return s
}

func (s *NodeQuery) Offset(offset int) graph.NodeQuery {
	s.queryBuilder.Apply(query.Offset(offset))
	return s
}

func (s *NodeQuery) Limit(limit int) graph.NodeQuery {
	s.queryBuilder.Apply(query.Limit(limit))
	return s
}

func (s *NodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.queryBuilder.Apply(query.Where(criteria))
	return s
}

func (s *NodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	return s.Filter(criteriaDelegate())
}

func (s *NodeQuery) Count() (int64, error) {
	var count int64

	return count, s.Query(func(results graph.Result) error {
		if !results.Next() {
			return graph.ErrNoResultsFound
		}

		return results.Scan(&count)
	}, query.Returning(
		query.Count(query.Node()),
	))
}

func (s *NodeQuery) Update(properties *graph.Properties) error {
	s.queryBuilder.Apply(query.Updatef(func() graph.Criteria {
		var updateStatements []graph.Criteria

		if modifiedProperties := properties.ModifiedProperties(); len(modifiedProperties) > 0 {
			updateStatements = append(updateStatements, query.SetProperties(query.Node(), modifiedProperties))
		}

		if deletedProperties := properties.DeletedProperties(); len(deletedProperties) > 0 {
			updateStatements = append(updateStatements, query.DeleteProperties(query.Node(), deletedProperties...))
		}

		return updateStatements
	}))

	if err := s.queryBuilder.Prepare(); err != nil {
		return err
	} else if cypherQuery, err := s.queryBuilder.Render(); err != nil {
		strippedQuery := stripCypherQuery(cypherQuery)
		return graph.NewError(strippedQuery, err)
	} else if result := s.run(cypherQuery, s.queryBuilder.Parameters); result.Error() != nil {
		return result.Error()
	}

	return nil
}

func (s *NodeQuery) First() (*graph.Node, error) {
	var node graph.Node

	return &node, s.Query(func(results graph.Result) error {
		if !results.Next() {
			return graph.ErrNoResultsFound
		}

		return results.Scan(&node)
	}, query.Returning(
		query.Node(),
	), query.Limit(1))
}

func (s *NodeQuery) Fetch(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
	return s.Query(func(result graph.Result) error {
		cursor := graph.NewResultIterator(s.ctx, result, func(result graph.Result) (*graph.Node, error) {
			var node graph.Node
			return &node, result.Scan(&node)
		})

		defer cursor.Close()
		return delegate(cursor)
	}, append([]graph.Criteria{query.Returning(
		query.Node(),
	)}, finalCriteria...)...)
}

func (s *NodeQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	return s.Query(func(result graph.Result) error {
		cursor := graph.NewResultIterator(s.ctx, result, func(result graph.Result) (graph.ID, error) {
			var nodeID graph.ID
			return nodeID, result.Scan(&nodeID)
		})

		defer cursor.Close()
		return delegate(cursor)
	}, query.Returning(
		query.NodeID(),
	))
}

func (s *NodeQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.KindsResult]) error) error {
	return s.Query(func(result graph.Result) error {
		cursor := graph.NewResultIterator(s.ctx, result, func(result graph.Result) (graph.KindsResult, error) {
			var (
				nodeID    graph.ID
				nodeKinds graph.Kinds
				err       = result.Scan(&nodeID, &nodeKinds)
			)

			return graph.KindsResult{
				ID:    nodeID,
				Kinds: nodeKinds,
			}, err
		})

		defer cursor.Close()
		return delegate(cursor)
	}, query.Returning(
		query.NodeID(),
		query.KindsOf(query.Node()),
	))
}
