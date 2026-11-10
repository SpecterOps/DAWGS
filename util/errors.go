package util

import (
	"errors"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ErrorCollector serializes error collection and joins the errors accumulated so far.
type ErrorCollector interface {
	// Add appends an error to the shared collection; nil errors are ignored by Combined.
	Add(err error)
	// Combined returns a joined snapshot of accumulated errors, or nil when none are non-nil.
	Combined() error
}

// errorCollector guards an append-only error list with a shared mutex.
type errorCollector struct {
	// errors retains collected errors in lock-acquisition order.
	errors []error
	// lock serializes appends and snapshot creation.
	lock *sync.Mutex
}

// NewErrorCollector creates an empty collector safe for concurrent use.
func NewErrorCollector() ErrorCollector {
	return &errorCollector{
		lock: &sync.Mutex{},
	}
}

// Add appends err while holding the collection lock.
func (s *errorCollector) Add(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.errors = append(s.errors, err)
}

// Combined joins collected errors under the lock without clearing the collection.
func (s *errorCollector) Combined() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.errors) > 0 {
		return errors.Join(s.errors...)
	}

	return nil
}

// IsNeoTimeoutError recognizes Neo4j transaction-timeout codes or their textual form.
// The caller must supply a non-nil error.
func IsNeoTimeoutError(err error) bool {
	switch e := err.(type) {
	case *neo4j.Neo4jError:
		return strings.Contains(e.Code, "TransactionTimedOut")
	default:
		return strings.Contains(e.Error(), "Neo.ClientError.Transaction.TransactionTimedOut")
	}
}

// IsPostgresTimeoutError recognizes a wrapped PostgreSQL cancellation error whose message identifies a statement timeout.
func IsPostgresTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr != nil &&
		pgErr.Code == "57014" &&
		strings.Contains(strings.ToLower(pgErr.Message), "statement timeout")
}
