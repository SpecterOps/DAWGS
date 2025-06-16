package util

import (
	"errors"
	"strings"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type ErrorCollector interface {
	Add(err error)
	Combined() error
}

type errorCollector struct {
	errors []error
	lock   *sync.Mutex
}

func NewErrorCollector() ErrorCollector {
	return &errorCollector{
		lock: &sync.Mutex{},
	}
}

func (s *errorCollector) Add(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.errors = append(s.errors, err)
}

func (s *errorCollector) Combined() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.errors) > 0 {
		return errors.Join(s.errors...)
	}

	return nil
}

func IsNeoTimeoutError(err error) bool {
	switch e := err.(type) {
	case *neo4j.Neo4jError:
		return strings.Contains(e.Code, "TransactionTimedOut")
	default:
		return strings.Contains(e.Error(), "Neo.ClientError.Transaction.TransactionTimedOut")
	}
}
