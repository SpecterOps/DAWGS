package pg

import (
	"context"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeDriver is a hand-rolled stand-in for the package-private `driver`
// interface used by optimize.go. It lets unit tests exercise the phase
// wiring (list -> assess -> exec) without standing up a *pgxpool.Pool.
//
// Tests register canned Query/QueryRow responses keyed by a substring of
// the SQL statement; the first registered substring that matches a given
// SQL is consumed. Exec responses are similarly keyed by substring; every
// Exec call is appended to execCalls for assertion.
type fakeDriver struct {
	queryRules    []queryRule
	queryRowRules []queryRowRule
	execRules     []execRule
	execCalls     []execCall
}

type queryRule struct {
	match    string
	response func() (pgx.Rows, error)
}

type queryRowRule struct {
	match    string
	response func() pgx.Row
}

type execRule struct {
	match    string
	response func() (pgconn.CommandTag, error)
}

type execCall struct {
	sql  string
	args []any
}

func (s *fakeDriver) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s.execCalls = append(s.execCalls, execCall{sql: sql, args: args})
	for i := range s.execRules {
		if strings.Contains(sql, s.execRules[i].match) {
			return s.execRules[i].response()
		}
	}
	return pgconn.CommandTag{}, nil
}

func (s *fakeDriver) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	for i, r := range s.queryRules {
		if strings.Contains(sql, r.match) {
			s.queryRules = append(s.queryRules[:i], s.queryRules[i+1:]...)
			return r.response()
		}
	}
	return &fakeRows{}, nil
}

func (s *fakeDriver) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	for i, r := range s.queryRowRules {
		if strings.Contains(sql, r.match) {
			s.queryRowRules = append(s.queryRowRules[:i], s.queryRowRules[i+1:]...)
			return r.response()
		}
	}
	return &fakeRow{err: pgx.ErrNoRows}
}

// fakeRows implements pgx.Rows over a slice of value tuples. Each tuple is
// a []any whose element types must match the pointer destinations passed to
// Scan (assignment is performed via reflect). closeErr is returned by Err()
// after the cursor is exhausted, simulating a partial-read failure.
type fakeRows struct {
	values   [][]any
	pos      int
	scanErr  error
	closeErr error
	closed   bool
}

func (s *fakeRows) Close()                                       { s.closed = true }
func (s *fakeRows) Err() error                                   { return s.closeErr }
func (s *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (s *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (s *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (s *fakeRows) RawValues() [][]byte                          { return nil }
func (s *fakeRows) Conn() *pgx.Conn                              { return nil }

func (s *fakeRows) Next() bool {
	if s.pos >= len(s.values) {
		return false
	}
	s.pos++
	return true
}

func (s *fakeRows) Scan(dest ...any) error {
	if s.scanErr != nil {
		return s.scanErr
	}
	row := s.values[s.pos-1]
	return assignRow(row, dest)
}

// fakeRow implements pgx.Row for a single QueryRow response.
type fakeRow struct {
	values []any
	err    error
}

func (s *fakeRow) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}
	return assignRow(s.values, dest)
}

// assignRow copies row[i] into the value pointed to by dest[i] using reflect,
// bridging the typed pointer destinations passed to Scan with the loosely
// typed []any values registered by the test. A length mismatch or assignment
// error returns a descriptive error to make scan misuse obvious in tests.
func assignRow(row []any, dest []any) error {
	if len(row) != len(dest) {
		return &scanMismatchError{want: len(row), got: len(dest)}
	}
	for i, v := range row {
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return &scanMismatchError{want: len(row), got: len(dest)}
		}
		dv.Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

type scanMismatchError struct{ want, got int }

func (s *scanMismatchError) Error() string {
	return "fake scan mismatch"
}
