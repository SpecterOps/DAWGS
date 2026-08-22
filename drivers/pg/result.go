package pg

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/graph"
)

// queryResult adapts pgx rows to graph.Result, caching column names and decoding JSON values for each current row.
type queryResult struct {
	// ctx supplies cancellation and request scope when decoded graph values require kind mapping.
	ctx context.Context

	// rows is the pgx result set being adapted.
	rows pgx.Rows

	// values contains the decoded values for the current row.
	values []any

	// keys caches immutable column names shared by every row in the result set.
	keys []string

	// kindMapper resolves database kind identifiers while scanning graph values.
	kindMapper KindMapper
}

func (s *queryResult) Values() []any {
	return s.values
}

func (s *queryResult) Keys() []string {
	return s.keys
}

// Next advances to the next row, caching its column names and decoding JSON values before exposing it.
func (s *queryResult) Next() bool {
	if s.rows.Next() {
		fields := s.rows.FieldDescriptions()
		s.cacheKeys(fields)

		// This error check exists just as a guard for a successful return of this function. The expectation is that
		// the pgx type will have error information attached to it which is reflected by the Error receiver function
		// of this type
		if values, err := s.rows.Values(); err == nil {
			s.values = decodeJSONValues(values, fields)
			return true
		}
	}

	return false
}

// cacheKeys records immutable column names once for the lifetime of the result set.
func (s *queryResult) cacheKeys(fields []pgconn.FieldDescription) {
	if s.keys != nil {
		return
	}

	// A pgx Rows value represents one result set, whose field descriptions do
	// not change between rows. Retain the names once instead of rebuilding the
	// same slice for every row.
	s.keys = make([]string, len(fields))
	for idx, field := range fields {
		s.keys[idx] = field.Name
	}
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

// decodeJSONValues replaces raw JSON and JSONB fields in the caller-owned row slice with decoded Go values.
func decodeJSONValues(values []any, fields []pgconn.FieldDescription) []any {
	// pgx Rows.Values returns a decoded value slice for the current row. The old
	// implementation made a shallow copy before replacing JSON scalars, but its
	// nested values were still shared. Updating this otherwise-unexposed slice
	// in place therefore preserves ownership while avoiding one allocation and
	// copy per row.
	for idx, field := range fields {
		switch field.DataTypeOID {
		case pgtype.JSONOID, pgtype.JSONBOID:
			if decoded, ok := decodeJSONValue(values[idx]); ok {
				values[idx] = decoded
			}
		}
	}

	return values
}

// decodeJSONValue decodes byte JSON and structured string JSON while preserving already-decoded scalar strings.
func decodeJSONValue(value any) (any, bool) {
	switch typedValue := value.(type) {
	case []byte:
		var decoded any
		if err := json.Unmarshal(typedValue, &decoded); err == nil {
			return decoded, true
		}

	case string:
		trimmedValue := strings.TrimSpace(typedValue)
		if len(trimmedValue) == 0 {
			return nil, false
		}

		switch trimmedValue[0] {
		case '{', '[', '"':
		default:
			return nil, false
		}

		var decoded any
		if err := json.Unmarshal([]byte(trimmedValue), &decoded); err == nil {
			return decoded, true
		}
	}

	return nil, false
}
