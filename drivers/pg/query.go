package pg

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

type liveQuery struct {
	ctx             context.Context
	tx              *transaction
	graphIDResolver func() (int32, error)
	queryBuilder    *query.Builder
}

func newLiveQuery(ctx context.Context, tx *transaction, graphIDResolver func() (int32, error)) liveQuery {
	return liveQuery{
		ctx:             ctx,
		tx:              tx,
		graphIDResolver: graphIDResolver,
		queryBuilder:    query.NewBuilder(nil),
	}
}

func (s *liveQuery) runRegularQuery(allShortestPaths bool) graph.Result {
	if regularQuery, err := s.queryBuilder.Build(allShortestPaths); err != nil {
		return graph.NewErrorResult(err)
	} else if prepared, err := prepareRegularQuery(regularQuery); err != nil {
		return graph.NewErrorResult(err)
	} else if graphID, err := s.graphIDResolver(); err != nil {
		return graph.NewErrorResult(err)
	} else if sqlQuery, bindings, err := s.tx.schemaManager.compileRegularQuery(s.ctx, prepared, graphID); err != nil {
		return graph.NewErrorResult(err)
	} else {
		return s.tx.Raw(commentRegularQuery(prepared.commentSource, sqlQuery), bindings)
	}
}

func (s *liveQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(false); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s *liveQuery) QueryAllShortestPaths(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	s.queryBuilder.Apply(finalCriteria...)

	if result := s.runRegularQuery(true); result.Error() != nil {
		return result.Error()
	} else {
		defer result.Close()
		return delegate(result)
	}
}

func (s *liveQuery) exec(finalCriteria ...graph.Criteria) error {
	return s.Query(func(results graph.Result) error {
		return results.Error()
	}, finalCriteria...)
}
