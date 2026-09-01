package retriever

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestNodeParquetSinkRoundTripsVariantProperties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.parquet")
	properties := representativeParquetProperties()

	sink, err := newNodeParquetSink(path)
	if err != nil {
		t.Fatalf("create node parquet sink: %v", err)
	}
	if err := sink.Write(FragmentNode{ID: "node-1", Kinds: []string{"Person", "Employee"}, Properties: properties}); err != nil {
		t.Fatalf("write first node: %v", err)
	}
	if err := sink.Write(FragmentNode{ID: "node-2", Kinds: []string{"Device"}, Properties: map[string]any{"name": "Laptop"}}); err != nil {
		t.Fatalf("write second node: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close node parquet sink: %v", err)
	}

	rows, err := parquet.ReadFile[parquetNodeRow](path)
	if err != nil {
		t.Fatalf("read node parquet file: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read %d node rows, want 2", len(rows))
	}
	if rows[0].ID != "node-1" || !reflect.DeepEqual(rows[0].Kinds, []string{"Person", "Employee"}) {
		t.Fatalf("unexpected first node identity: %+v", rows[0])
	}
	if rows[1].ID != "node-2" || !reflect.DeepEqual(rows[1].Kinds, []string{"Device"}) {
		t.Fatalf("unexpected second node identity: %+v", rows[1])
	}
	if !reflect.DeepEqual(rows[0].Properties, properties) {
		t.Fatalf("first node properties = %#v, want %#v", rows[0].Properties, properties)
	}
	if !reflect.DeepEqual(rows[1].Properties, map[string]any{"name": "Laptop"}) {
		t.Fatalf("second node properties = %#v, want %#v", rows[1].Properties, map[string]any{"name": "Laptop"})
	}
	assertNodeParquetFooterSchema(t, path)
}

func TestEdgeParquetSinkRoundTripsVariantProperties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.parquet")
	properties := representativeParquetProperties()

	sink, err := newEdgeParquetSink(path)
	if err != nil {
		t.Fatalf("create edge parquet sink: %v", err)
	}
	if err := sink.Write(FragmentEdge{StartID: "node-1", EndID: "node-2", Kind: "MemberOf", Properties: properties}); err != nil {
		t.Fatalf("write first edge: %v", err)
	}
	if err := sink.Write(FragmentEdge{StartID: "node-2", EndID: "node-3", Kind: "AdminTo", Properties: map[string]any{"enabled": false}}); err != nil {
		t.Fatalf("write second edge: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close edge parquet sink: %v", err)
	}

	rows, err := parquet.ReadFile[parquetEdgeRow](path)
	if err != nil {
		t.Fatalf("read edge parquet file: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read %d edge rows, want 2", len(rows))
	}
	if rows[0].StartID != "node-1" || rows[0].EndID != "node-2" || rows[0].Kind != "MemberOf" {
		t.Fatalf("unexpected first edge identity: %+v", rows[0])
	}
	if rows[1].StartID != "node-2" || rows[1].EndID != "node-3" || rows[1].Kind != "AdminTo" {
		t.Fatalf("unexpected second edge identity: %+v", rows[1])
	}
	if !reflect.DeepEqual(rows[0].Properties, properties) {
		t.Fatalf("first edge properties = %#v, want %#v", rows[0].Properties, properties)
	}
	if !reflect.DeepEqual(rows[1].Properties, map[string]any{"enabled": false}) {
		t.Fatalf("second edge properties = %#v, want %#v", rows[1].Properties, map[string]any{"enabled": false})
	}
	assertEdgeParquetFooterSchema(t, path)
}

func representativeParquetProperties() map[string]any {
	return map[string]any{
		"name":    "Ada",
		"enabled": true,
		"score":   float64(42.5),
		"nested":  map[string]any{"labels": []any{"a", float64(2)}},
	}
}

func assertNodeParquetFooterSchema(t *testing.T, path string) {
	t.Helper()

	schema := readParquetFooterSchema(t, path)
	assertRequiredParquetStringField(t, schema, "id")
	assertParquetVariantField(t, schema, "properties")

	kinds := parquetSchemaField(t, schema, "kinds")
	if !kinds.Required() {
		t.Fatalf("Parquet field %q is not required", kinds.Name())
	}
	if logicalType := kinds.Type().LogicalType(); logicalType == nil {
		t.Fatalf("Parquet field %q has no LIST logical annotation", kinds.Name())
	} else if _, ok := logicalType.Value.(*format.ListType); !ok {
		t.Fatalf("Parquet field %q logical type = %T, want *format.ListType", kinds.Name(), logicalType.Value)
	}
	listFields := kinds.Fields()
	if len(listFields) != 1 || listFields[0].Name() != "list" || !listFields[0].Repeated() {
		t.Fatalf("Parquet field %q LIST group = %#v, want one repeated list field", kinds.Name(), listFields)
	}
	elementFields := listFields[0].Fields()
	if len(elementFields) != 1 || elementFields[0].Name() != "element" || !elementFields[0].Required() {
		t.Fatalf("Parquet field %q element group = %#v, want one required element field", kinds.Name(), elementFields)
	}
	assertParquetStringNode(t, elementFields[0])
}

func assertEdgeParquetFooterSchema(t *testing.T, path string) {
	t.Helper()

	schema := readParquetFooterSchema(t, path)
	for _, name := range []string{"start_id", "end_id", "kind"} {
		assertRequiredParquetStringField(t, schema, name)
	}
	assertParquetVariantField(t, schema, "properties")
}

func readParquetFooterSchema(t *testing.T, path string) *parquet.Schema {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open Parquet file for footer schema: %v", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat Parquet file for footer schema: %v", err)
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		t.Fatalf("open Parquet footer: %v", err)
	}
	return parquetFile.Schema()
}

func parquetSchemaField(t *testing.T, schema *parquet.Schema, name string) parquet.Field {
	t.Helper()

	for _, field := range schema.Fields() {
		if field.Name() == name {
			return field
		}
	}
	t.Fatalf("Parquet footer schema does not contain field %q", name)
	return nil
}

func assertRequiredParquetStringField(t *testing.T, schema *parquet.Schema, name string) {
	t.Helper()

	field := parquetSchemaField(t, schema, name)
	if !field.Required() {
		t.Fatalf("Parquet field %q is not required", name)
	}
	assertParquetStringNode(t, field)
}

func assertParquetStringNode(t *testing.T, node parquet.Node) {
	t.Helper()

	if !node.Leaf() {
		t.Fatalf("Parquet string node is a group: %s", node)
	}
	if kind := node.Type().Kind(); kind != parquet.ByteArray {
		t.Fatalf("Parquet string physical kind = %s, want BYTE_ARRAY", kind)
	}
	logicalType := node.Type().LogicalType()
	if logicalType == nil {
		t.Fatal("Parquet string node has no STRING logical annotation")
	}
	if _, ok := logicalType.Value.(*format.StringType); !ok {
		t.Fatalf("Parquet string logical type = %T, want *format.StringType", logicalType.Value)
	}
}

func assertParquetVariantField(t *testing.T, schema *parquet.Schema, name string) {
	t.Helper()

	field := parquetSchemaField(t, schema, name)
	if !field.Required() {
		t.Fatalf("Parquet field %q is not required", name)
	}
	if logicalType := field.Type().LogicalType(); logicalType == nil {
		t.Fatalf("Parquet field %q has no VARIANT logical annotation", name)
	} else if _, ok := logicalType.Value.(*format.VariantType); !ok {
		t.Fatalf("Parquet field %q logical type = %T, want *format.VariantType", name, logicalType.Value)
	}
}
