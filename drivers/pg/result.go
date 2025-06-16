package pg

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
)

type queryResult struct {
	ctx        context.Context
	rows       pgx.Rows
	values     []any
	kindMapper KindMapper
}

func (s *queryResult) Values() []any {
	return s.values
}

func (s *queryResult) Next() bool {
	if s.rows.Next() {
		// This error check exists just as a guard for a successful return of this function. The expectation is that
		// the pgx type will have error information attached to it which is reflected by the Error receiver function
		// of this type
		if values, err := s.rows.Values(); err == nil {
			s.values = values
			return true
		}
	}

	return false
}

func (s *queryResult) Mapper() graph.ValueMapper {
	return NewValueMapper(s.ctx, s.kindMapper)
}

func (s *queryResult) Scan(targets ...any) error {
	return graph.ScanNextResult(s, targets...)
}

func (s *queryResult) Error() error {
	return s.rows.Err()
}

func (s *queryResult) Close() {
	s.rows.Close()
}
