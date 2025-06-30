package v1compat

import (
	"context"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type nodeQuery struct {
	ctx     context.Context
	driver  database.Driver
	builder query.QueryBuilder
}

func newNodeQuery(ctx context.Context, driver database.Driver) NodeQuery {
	return &nodeQuery{
		ctx:     ctx,
		driver:  driver,
		builder: query.New(),
	}
}

func (s nodeQuery) Filter(criteria Criteria) NodeQuery {
	s.builder.Where(criteria)
	return s
}

func (s nodeQuery) Filterf(criteriaDelegate CriteriaProvider) NodeQuery {
	s.builder.Where(criteriaDelegate())
	return s
}

func (s nodeQuery) Query(delegate func(results Result) error, finalCriteria ...any) error {
	s.builder.Return(finalCriteria...)

	if result, err := s.exec(); err != nil {
		return err
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s nodeQuery) Delete() error {
	s.builder.Delete(query.Node())

	if result, err := s.exec(); err != nil {
		return err
	} else {
		result.Close()
		return nil
	}
}

func (s nodeQuery) Update(properties *graph.Properties) error {
	s.builder.Update(query.Node().SetProperties(properties.MapOrEmpty()))

	if result, err := s.exec(); err != nil {
		return err
	} else {
		result.Close()
		return nil
	}
}

func (s nodeQuery) OrderBy(criteria ...Criteria) NodeQuery {
	s.builder.OrderBy(criteria...)
	return s
}

func (s nodeQuery) Offset(skip int) NodeQuery {
	s.builder.Skip(skip)
	return s
}

func (s nodeQuery) Limit(limit int) NodeQuery {
	s.builder.Limit(limit)
	return s
}

func (s nodeQuery) exec() (Result, error) {
	if builtQuery, err := s.builder.Build(); err != nil {
		return nil, err
	} else {
		result := s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters)
		return wrapResult(s.ctx, result, s.driver.Mapper()), nil
	}
}

func (s nodeQuery) Count() (int64, error) {
	s.builder.Return(query.Node().Count())

	if result, err := s.exec(); err != nil {
		return 0, err
	} else {
		defer result.Close()

		if result.Next() {
			var count int64

			if err := result.Scan(&count); err != nil {
				return 0, err
			}

			return count, result.Error()
		}

		if result.Error() != nil {
			return 0, result.Error()
		}

		return 0, ErrNoResultsFound
	}
}

func (s nodeQuery) First() (*graph.Node, error) {
	s.builder.Return(query.Node()).Limit(1)

	if result, err := s.exec(); err != nil {
		return nil, err
	} else {
		defer result.Close()

		if result.Next() {
			var node graph.Node

			if err := result.Scan(&node); err != nil {
				return nil, err
			}

			return &node, nil
		}

		if result.Error() != nil {
			return nil, result.Error()
		}

		return nil, ErrNoResultsFound
	}
}

func (s nodeQuery) Fetch(delegate func(cursor Cursor[*graph.Node]) error, finalCriteria ...Criteria) error {
	s.builder.Return(query.Node())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (*graph.Node, error) {
			var (
				node graph.Node
				err  = result.Scan(&node)
			)

			return &node, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s nodeQuery) FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error {
	s.builder.Return(query.Node().ID())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (graph.ID, error) {
			var nodeID graph.ID
			return nodeID, result.Scan(&nodeID)
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}

func (s nodeQuery) FetchKinds(delegate func(cursor Cursor[KindsResult]) error) error {
	s.builder.Return(query.Node().ID(), query.Node().Kinds())

	if builtQuery, err := s.builder.Build(); err != nil {
		return err
	} else {
		resultIter := NewResultIterator(s.ctx, s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters), func(result database.Result) (KindsResult, error) {
			var (
				nodeID    graph.ID
				nodeKinds graph.Kinds
				err       = result.Scan(&nodeID, &nodeKinds)
			)

			return KindsResult{
				ID:    nodeID,
				Kinds: nodeKinds,
			}, err
		})

		defer resultIter.Close()
		return delegate(resultIter)
	}
}
