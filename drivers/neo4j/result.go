package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
)

type internalResult struct {
	query        string
	err          error
	driverResult neo4j.Result
}

func NewResult(query string, err error, driverResult neo4j.Result) graph.Result {
	return &internalResult{
		query:        query,
		err:          err,
		driverResult: driverResult,
	}
}

func (s *internalResult) Mapper() graph.ValueMapper {
	return NewValueMapper()
}

func (s *internalResult) Keys() []string {
	return s.driverResult.Record().Keys
}

func (s *internalResult) Values() []any {
	return s.driverResult.Record().Values
}

func (s *internalResult) Scan(targets ...any) error {
	return graph.ScanNextResult(s, targets...)
}

func (s *internalResult) Next() bool {
	return s.driverResult.Next()
}

func (s *internalResult) Error() error {
	if s.err != nil {
		return s.err
	}

	if s.driverResult != nil && s.driverResult.Err() != nil {
		strippedQuery := stripCypherQuery(s.query)
		return graph.NewError(strippedQuery, s.driverResult.Err())
	}

	return nil
}

func (s *internalResult) Close() {
	if s.driverResult != nil {
		// Ignore the results of this call. This is called only as a best-effort attempt at a close
		s.driverResult.Consume()
	}
}
