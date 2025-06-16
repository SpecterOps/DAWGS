package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type errorTransactionWrapper struct {
	err error
}

func newErrorTransactionWrapper(err error) errorTransactionWrapper {
	return errorTransactionWrapper{
		err: err,
	}
}

func (s errorTransactionWrapper) Run(cypher string, params map[string]any) (neo4j.Result, error) {
	return nil, s.err
}

func (s errorTransactionWrapper) Commit() error {
	return s.err
}

func (s errorTransactionWrapper) Rollback() error {
	return s.err
}

func (s errorTransactionWrapper) Close() error {
	return s.err
}
