package pg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/internal/jsonutil"
)

// installJSONCodecs must run before loading composites: their property fields
// retain pointers to the registered JSONB type. Array element types do too.
func installJSONCodecs(typeMap *pgtype.Map) {
	for _, definition := range []struct {
		name          string
		oid, arrayOID uint32
		codec         pgtype.Codec
	}{
		{"json", pgtype.JSONOID, pgtype.JSONArrayOID, &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: unmarshalJSONNumbers}},
		{"jsonb", pgtype.JSONBOID, pgtype.JSONBArrayOID, &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: unmarshalJSONNumbers}},
	} {
		dataType := &pgtype.Type{Name: definition.name, OID: definition.oid, Codec: definition.codec}
		typeMap.RegisterType(dataType)
		typeMap.RegisterType(&pgtype.Type{
			Name: "_" + definition.name, OID: definition.arrayOID,
			Codec: &pgtype.ArrayCodec{ElementType: dataType},
		})
	}
}

// unmarshalJSONNumbers preserves int64 integers in the untyped destinations
// used by Rows.Values and graph property composites. Explicitly typed pgx scan
// destinations keep encoding/json's normal conversion and unmarshaler behavior.
func unmarshalJSONNumbers(data []byte, target any) error {
	switch target.(type) {
	case *any, *map[string]any, *[]any:
	default:
		return json.Unmarshal(data, target)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected trailing JSON value")
	}

	switch destination := target.(type) {
	case *any:
		value, err := jsonutil.NormalizeNumbers(*destination)
		if err != nil {
			return err
		}
		*destination = value
	case *map[string]any:
		_, err := jsonutil.NormalizeNumbers(*destination)
		return err
	case *[]any:
		_, err := jsonutil.NormalizeNumbers(*destination)
		return err
	}
	return nil
}
