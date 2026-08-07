package pg

import (
	sqldriver "database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

// ownedComposite is the set of PostgreSQL composites that have a stable,
// driver-owned Go representation. Keeping this set closed makes it difficult
// to accidentally register an unrelated composite with a decoder whose field
// order does not match its PostgreSQL definition.
type ownedComposite interface {
	nodeComposite | edgeComposite | pathComposite
}

// ownedCompositeCodec retains pgx's encoding and explicit Scan behavior while
// replacing CompositeCodec.DecodeValue's map[string]any result. Rows.Values
// uses DecodeValue, so decoding directly into the concrete representation
// avoids a map and one interface value per field. The field scanners allocate
// their slices and JSON maps, which also makes the returned value independent
// of pgx's reusable wire buffer.
type ownedCompositeCodec[T ownedComposite] struct {
	compositeCodec *pgtype.CompositeCodec
}

// ownedCompositeArrayCodec decodes the common, non-null-element case directly
// into []T. PostgreSQL arrays may contain NULL composite elements, so a typed
// scan failure falls back to pgx's []any representation instead of discarding
// that information.
type ownedCompositeArrayCodec[T ownedComposite] struct {
	arrayCodec *pgtype.ArrayCodec
}

func (s *ownedCompositeCodec[T]) FormatSupported(format int16) bool {
	return s.compositeCodec.FormatSupported(format)
}

func (s *ownedCompositeCodec[T]) PreferredFormat() int16 {
	return s.compositeCodec.PreferredFormat()
}

func (s *ownedCompositeCodec[T]) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return s.compositeCodec.PlanEncode(m, oid, format, value)
}

func (s *ownedCompositeCodec[T]) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return s.compositeCodec.PlanScan(m, oid, format, target)
}

func (s *ownedCompositeCodec[T]) DecodeDatabaseSQLValue(
	m *pgtype.Map,
	oid uint32,
	format int16,
	src []byte,
) (sqldriver.Value, error) {
	return s.compositeCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

func (s *ownedCompositeCodec[T]) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var value T
	target, typeOK := any(&value).(pgtype.CompositeIndexScanner)
	if !typeOK {
		return nil, fmt.Errorf("owned composite target %T does not implement pgtype.CompositeIndexScanner", &value)
	}

	plan := s.compositeCodec.PlanScan(m, oid, format, target)
	if plan == nil {
		return nil, fmt.Errorf("unable to scan PostgreSQL composite OID %d in format %d into %T", oid, format, &value)
	}

	if err := plan.Scan(src, target); err != nil {
		// PostgreSQL permits NULL fields inside a non-NULL composite, while the
		// hot-path representation deliberately uses non-nullable scalar fields.
		// Preserve the old map representation for those uncommon values rather
		// than turning a valid row into a decode error.
		return s.compositeCodec.DecodeValue(m, oid, format, src)
	}

	return value, nil
}

func (s *ownedCompositeArrayCodec[T]) FormatSupported(format int16) bool {
	return s.arrayCodec.FormatSupported(format)
}

func (s *ownedCompositeArrayCodec[T]) PreferredFormat() int16 {
	return s.arrayCodec.PreferredFormat()
}

func (s *ownedCompositeArrayCodec[T]) PlanEncode(
	m *pgtype.Map,
	oid uint32,
	format int16,
	value any,
) pgtype.EncodePlan {
	return s.arrayCodec.PlanEncode(m, oid, format, value)
}

func (s *ownedCompositeArrayCodec[T]) PlanScan(
	m *pgtype.Map,
	oid uint32,
	format int16,
	target any,
) pgtype.ScanPlan {
	return s.arrayCodec.PlanScan(m, oid, format, target)
}

func (s *ownedCompositeArrayCodec[T]) DecodeDatabaseSQLValue(
	m *pgtype.Map,
	oid uint32,
	format int16,
	src []byte,
) (sqldriver.Value, error) {
	return s.arrayCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

func (s *ownedCompositeArrayCodec[T]) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}

	var values []T
	if plan := m.PlanScan(oid, format, &values); plan != nil {
		if err := plan.Scan(src, &values); err == nil {
			return values, nil
		}
	}

	// A []T cannot represent a NULL composite array element. Preserve pgx's
	// nullable []any behavior for that less common case.
	return s.arrayCodec.DecodeValue(m, oid, format, src)
}

func installOwnedCompositeCodec(dataType pgsql.DataType, definition *pgtype.Type) error {
	switch dataType {
	case pgsql.NodeCompositeArray:
		arrayCodec, typeOK := definition.Codec.(*pgtype.ArrayCodec)
		if !typeOK {
			return fmt.Errorf("expected PostgreSQL type %s to use *pgtype.ArrayCodec but received %T", dataType, definition.Codec)
		}

		definition.Codec = &ownedCompositeArrayCodec[nodeComposite]{arrayCodec: arrayCodec}
		return nil
	case pgsql.EdgeCompositeArray:
		arrayCodec, typeOK := definition.Codec.(*pgtype.ArrayCodec)
		if !typeOK {
			return fmt.Errorf("expected PostgreSQL type %s to use *pgtype.ArrayCodec but received %T", dataType, definition.Codec)
		}

		definition.Codec = &ownedCompositeArrayCodec[edgeComposite]{arrayCodec: arrayCodec}
		return nil
	}

	compositeCodec, typeOK := definition.Codec.(*pgtype.CompositeCodec)
	if !typeOK {
		return fmt.Errorf("expected PostgreSQL type %s to use *pgtype.CompositeCodec but received %T", dataType, definition.Codec)
	}

	switch dataType {
	case pgsql.NodeComposite:
		definition.Codec = &ownedCompositeCodec[nodeComposite]{compositeCodec: compositeCodec}
	case pgsql.EdgeComposite:
		definition.Codec = &ownedCompositeCodec[edgeComposite]{compositeCodec: compositeCodec}
	case pgsql.PathComposite:
		definition.Codec = &ownedCompositeCodec[pathComposite]{compositeCodec: compositeCodec}
	default:
		return fmt.Errorf("PostgreSQL type %s does not have an owned composite decoder", dataType)
	}

	return nil
}
