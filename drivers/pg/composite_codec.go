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
	// The closed type set limits optimized decoding to composites whose PostgreSQL field order is owned by this driver.
	nodeComposite | edgeComposite | pathComposite
}

// ownedCompositeCodec retains pgx's encoding and explicit Scan behavior while
// replacing CompositeCodec.DecodeValue's map[string]any result. Rows.Values
// uses DecodeValue, so decoding directly into the concrete representation
// avoids a map and one interface value per field. The field scanners allocate
// their slices and JSON maps, which also makes the returned value independent
// of pgx's reusable wire buffer.
type ownedCompositeCodec[T ownedComposite] struct {
	// compositeCodec retains pgx's standard encoding and scan-plan implementation.
	compositeCodec *pgtype.CompositeCodec
}

// ownedCompositeArrayCodec decodes the common, non-null-element case directly
// into []T. PostgreSQL arrays may contain NULL composite elements, so a typed
// scan failure falls back to pgx's []any representation instead of discarding
// that information.
type ownedCompositeArrayCodec[T ownedComposite] struct {
	// arrayCodec retains pgx's array metadata and fallback decoding behavior.
	arrayCodec *pgtype.ArrayCodec
}

// FormatSupported reports whether the wrapped composite codec accepts format.
func (s *ownedCompositeCodec[T]) FormatSupported(format int16) bool {
	return s.compositeCodec.FormatSupported(format)
}

// PreferredFormat returns the wire format preferred by the wrapped composite codec.
func (s *ownedCompositeCodec[T]) PreferredFormat() int16 {
	return s.compositeCodec.PreferredFormat()
}

// PlanEncode delegates composite encoding to pgx's registered composite codec.
func (s *ownedCompositeCodec[T]) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return s.compositeCodec.PlanEncode(m, oid, format, value)
}

// PlanScan preserves pgx's explicit-target composite scanning behavior.
func (s *ownedCompositeCodec[T]) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return s.compositeCodec.PlanScan(m, oid, format, target)
}

// DecodeDatabaseSQLValue delegates database/sql decoding to pgx's composite codec.
func (s *ownedCompositeCodec[T]) DecodeDatabaseSQLValue(
	m *pgtype.Map,
	oid uint32,
	format int16,
	src []byte,
) (sqldriver.Value, error) {
	return s.compositeCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

// DecodeValue decodes non-null composites into their owned Go representation and falls back for nullable fields.
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

// FormatSupported reports whether the wrapped array codec accepts format.
func (s *ownedCompositeArrayCodec[T]) FormatSupported(format int16) bool {
	return s.arrayCodec.FormatSupported(format)
}

// PreferredFormat returns the wire format preferred by the wrapped array codec.
func (s *ownedCompositeArrayCodec[T]) PreferredFormat() int16 {
	return s.arrayCodec.PreferredFormat()
}

// PlanEncode delegates composite-array encoding to pgx's registered array codec.
func (s *ownedCompositeArrayCodec[T]) PlanEncode(
	m *pgtype.Map,
	oid uint32,
	format int16,
	value any,
) pgtype.EncodePlan {
	return s.arrayCodec.PlanEncode(m, oid, format, value)
}

// PlanScan preserves pgx's explicit-target composite-array scanning behavior.
func (s *ownedCompositeArrayCodec[T]) PlanScan(
	m *pgtype.Map,
	oid uint32,
	format int16,
	target any,
) pgtype.ScanPlan {
	return s.arrayCodec.PlanScan(m, oid, format, target)
}

// DecodeDatabaseSQLValue delegates database/sql decoding to pgx's array codec.
func (s *ownedCompositeArrayCodec[T]) DecodeDatabaseSQLValue(
	m *pgtype.Map,
	oid uint32,
	format int16,
	src []byte,
) (sqldriver.Value, error) {
	return s.arrayCodec.DecodeDatabaseSQLValue(m, oid, format, src)
}

// DecodeValue decodes arrays without null elements into []T and otherwise preserves pgx's nullable representation.
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

// installOwnedCompositeCodec replaces a supported pgx codec with the matching driver-owned scalar or array decoder.
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
