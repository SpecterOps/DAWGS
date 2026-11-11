package query

import (
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

type lateKindRegistrationResult struct {
	graph.Result
	closed  bool
	failure error
}

func (s *lateKindRegistrationResult) Next() bool                { return true }
func (s *lateKindRegistrationResult) Scan(targets ...any) error { *targets[0].(*int16) = 7; return nil }
func (s *lateKindRegistrationResult) Close()                    { s.closed = true }
func (s *lateKindRegistrationResult) Error() error {
	if s.closed {
		return s.failure
	}
	return nil
}

type kindRegistrationTransaction struct {
	graph.Transaction
	result graph.Result
}

func (s kindRegistrationTransaction) Raw(string, map[string]any) graph.Result { return s.result }

func TestKindRegistrationWaitsForStatementCompletion(t *testing.T) {
	failure := errors.New("deferred registration constraint failed")
	result := &lateKindRegistrationResult{failure: failure}
	_, err := On(kindRegistrationTransaction{result: result}).InsertOrGetKind(graph.StringKind("Private"))
	require.ErrorIs(t, err, failure)
	require.True(t, result.closed)
}
