package parquet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/specterops/dawgs/ret/entity"
)

func TestRelationshipSchemaContainsSourceIDAndUnshreddedVariantProperties(t *testing.T) {
	root := t.TempDir()
	temporary := filepath.Join(root, "relationships.tmp")
	artifact, err := WriteRelationships(temporary, "relationships.parquet", Config{Enabled: true}, []entity.Relationship{{
		SourceID: "99",
		StartID:  "1",
		EndID:    "2",
		Kind:     "MemberOf",
		Properties: map[string]any{
			"score": int64(7),
		},
	}})
	if err != nil {
		t.Fatalf("write relationships: %v", err)
	}

	contents, err := os.ReadFile(temporary)
	if err != nil {
		t.Fatalf("read Parquet artifact: %v", err)
	}
	file, err := parquetgo.OpenFile(bytes.NewReader(contents), int64(len(contents)))
	if err != nil {
		t.Fatalf("open Parquet artifact: %v", err)
	}
	schema := strings.ToLower(file.Schema().String())
	if !strings.Contains(schema, "source_id") {
		t.Fatalf("schema does not contain relationship source_id:\n%s", schema)
	}
	if !strings.Contains(schema, "required binary source_id (string)") {
		t.Fatalf("schema does not require relationship source_id:\n%s", schema)
	}
	if !strings.Contains(schema, "group properties (variant)") {
		t.Fatalf("schema does not identify properties as VARIANT:\n%s", schema)
	}
	if strings.Contains(schema, "typed_value") {
		t.Fatalf("schema shreds properties VARIANT unexpectedly:\n%s", schema)
	}
	if got, want := artifact.SchemaVersion, SchemaVersion; got != want {
		t.Fatalf("schema version = %q, want %q", got, want)
	}

	var propertiesVariant bool
	for _, element := range file.Metadata().Schema {
		if element.Name == "properties" && element.LogicalType.Valid && element.LogicalType.V.Variant != nil {
			propertiesVariant = true
		}
	}
	if !propertiesVariant {
		t.Fatal("properties field does not carry VARIANT logical type metadata")
	}
	for rowGroupIndex, rowGroup := range file.Metadata().RowGroups {
		for columnIndex, column := range rowGroup.Columns {
			if column.MetaData.Codec != format.Zstd {
				t.Fatalf("row group %d column %d codec = %s, want ZSTD", rowGroupIndex, columnIndex, column.MetaData.Codec)
			}
		}
	}
}

func TestNodeRoundTripPreservesKindOrderAndDuplicates(t *testing.T) {
	want := entity.Node{
		SourceID: "1",
		Kinds:    []string{"User", "Admin", "User"},
		Properties: map[string]any{
			"active": true,
		},
	}
	root := t.TempDir()
	artifact, err := WriteNodes(filepath.Join(root, "nodes.tmp"), "nodes.parquet", Config{Enabled: true}, []entity.Node{want})
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	if err := os.Rename(filepath.Join(root, "nodes.tmp"), filepath.Join(root, artifact.Path)); err != nil {
		t.Fatalf("install artifact: %v", err)
	}

	var got []entity.Node
	if err := ReadNodes(root, artifact, func(node entity.Node) error {
		got = append(got, node)
		return nil
	}); err != nil {
		t.Fatalf("read nodes: %v", err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("round trip nodes = %#v, want %#v", got, want)
	}
}

func TestWriteNodesRecordsStoredIntegrityMetadata(t *testing.T) {
	root := t.TempDir()
	temporary := filepath.Join(root, "nodes.tmp")
	artifact, err := WriteNodes(temporary, "nested/nodes.parquet", Config{Enabled: true}, []entity.Node{{SourceID: "1"}})
	if err != nil {
		t.Fatalf("write nodes: %v", err)
	}
	contents, err := os.ReadFile(temporary)
	if err != nil {
		t.Fatalf("read Parquet artifact: %v", err)
	}
	if got, want := artifact.Path, "nested/nodes.parquet"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
	if got, want := artifact.Count, int64(1); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
	if got, want := artifact.StoredBytes, int64(len(contents)); got != want {
		t.Fatalf("stored bytes = %d, want %d", got, want)
	}
	hash := sha256.Sum256(contents)
	if got, want := artifact.SHA256, hex.EncodeToString(hash[:]); got != want {
		t.Fatalf("SHA-256 = %q, want %q", got, want)
	}
}

func TestWritersEnforceFormatBoundaryAndSafePaths(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name  string
		write func() error
	}{
		{
			name: "disabled",
			write: func() error {
				_, err := WriteNodes(filepath.Join(root, "nodes.tmp"), "nodes.parquet", Config{}, []entity.Node{{SourceID: "1"}})
				return err
			},
		},
		{
			name: "relative temporary path",
			write: func() error {
				_, err := WriteNodes("nodes.tmp", "nodes.parquet", Config{Enabled: true}, []entity.Node{{SourceID: "1"}})
				return err
			},
		},
		{
			name: "escaping final path",
			write: func() error {
				_, err := WriteNodes(filepath.Join(root, "nodes.tmp"), "../nodes.parquet", Config{Enabled: true}, []entity.Node{{SourceID: "1"}})
				return err
			},
		},
		{
			name: "empty node source ID",
			write: func() error {
				_, err := WriteNodes(filepath.Join(root, "nodes.tmp"), "nodes.parquet", Config{Enabled: true}, []entity.Node{{}})
				return err
			},
		},
		{
			name: "empty relationship source ID",
			write: func() error {
				_, err := WriteRelationships(filepath.Join(root, "relationships.tmp"), "relationships.parquet", Config{Enabled: true}, []entity.Relationship{{
					StartID: "1",
					EndID:   "2",
					Kind:    "MemberOf",
				}})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.write(); err == nil {
				t.Fatal("write unexpectedly succeeded")
			}
		})
	}
}

func TestWriteNodesReturnsErrorForUnsupportedVariantValue(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("WriteNodes panicked instead of returning an error: %v", recovered)
		}
	}()

	_, err := WriteNodes(filepath.Join(t.TempDir(), "nodes.tmp"), "nodes.parquet", Config{Enabled: true}, []entity.Node{{
		SourceID: "1",
		Properties: map[string]any{
			"unsupported": make(chan int),
		},
	}})
	if err == nil {
		t.Fatal("WriteNodes unexpectedly accepted unsupported VARIANT value")
	}
}

func TestWriteNodesRejectsCyclicPropertiesBeforeOpeningTemporaryFile(t *testing.T) {
	runCyclicPropertiesWriterTest(t, "node")
}

func TestWriteRelationshipsRejectsCyclicPropertiesBeforeOpeningTemporaryFile(t *testing.T) {
	runCyclicPropertiesWriterTest(t, "relationship")
}

func TestWriteNodesAllowsRepeatedAcyclicPropertyReference(t *testing.T) {
	shared := map[string]any{"value": "shared"}
	_, err := WriteNodes(filepath.Join(t.TempDir(), "nodes.tmp"), "nodes.parquet", Config{Enabled: true}, []entity.Node{{
		SourceID: "node-1",
		Properties: map[string]any{
			"first":  shared,
			"second": shared,
		},
	}})
	if err != nil {
		t.Fatalf("write node with repeated acyclic property reference: %v", err)
	}
}

func TestWriteNodesAllowsAcyclicOverlappingSlices(t *testing.T) {
	values := make([]any, 1)
	values[0] = values[:0]
	_, err := WriteNodes(filepath.Join(t.TempDir(), "nodes.tmp"), "nodes.parquet", Config{Enabled: true}, []entity.Node{{
		SourceID:   "node-1",
		Properties: map[string]any{"values": values},
	}})
	if err != nil {
		t.Fatalf("write node with acyclic overlapping slices: %v", err)
	}
}

func TestConfigExposesOnlyEnabled(t *testing.T) {
	configType := reflect.TypeOf(Config{})
	if got, want := configType.NumField(), 1; got != want {
		t.Fatalf("Config field count = %d, want %d", got, want)
	}
	if got, want := configType.Field(0).Name, "Enabled"; got != want {
		t.Fatalf("Config field = %q, want %q", got, want)
	}
}

const cyclicPropertiesWriter = "DAWGS_PARQUET_CYCLIC_PROPERTIES_WRITER"

func runCyclicPropertiesWriterTest(t *testing.T, writerName string) {
	t.Helper()
	if os.Getenv(cyclicPropertiesWriter) == writerName {
		debug.SetMaxStack(1 << 20)
		assertCyclicPropertiesWriter(t, writerName)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), cyclicPropertiesWriter+"="+writerName)
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("%s writer subprocess did not terminate: %v\n%s", writerName, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("%s writer subprocess crashed instead of returning an error: %v\n%s", writerName, err, output)
	}
}

func assertCyclicPropertiesWriter(t *testing.T, writerName string) {
	t.Helper()
	mapCycle := map[string]any{}
	mapCycle["self"] = mapCycle
	sliceCycle := make([]any, 1)
	sliceCycle[0] = sliceCycle
	type pointerCycle struct {
		Self *pointerCycle
	}
	pointerValue := new(pointerCycle)
	pointerValue.Self = pointerValue

	cycles := []struct {
		name  string
		value any
	}{
		{name: "map", value: mapCycle},
		{name: "slice", value: sliceCycle},
		{name: "pointer", value: pointerValue},
	}
	for _, cycle := range cycles {
		for _, preexisting := range []bool{false, true} {
			root := t.TempDir()
			temporary := filepath.Join(root, "artifact.tmp")
			sentinel := []byte("keep existing temporary file")
			if preexisting {
				if err := os.WriteFile(temporary, sentinel, 0o600); err != nil {
					t.Fatalf("create existing temporary file: %v", err)
				}
			}
			properties := map[string]any{"cycle": cycle.value}

			var err error
			switch writerName {
			case "node":
				_, err = WriteNodes(temporary, "nodes.parquet", Config{Enabled: true}, []entity.Node{{
					SourceID:   "node-1",
					Properties: properties,
				}})
			case "relationship":
				_, err = WriteRelationships(temporary, "relationships.parquet", Config{Enabled: true}, []entity.Relationship{{
					SourceID:   "relationship-1",
					StartID:    "node-1",
					EndID:      "node-2",
					Kind:       "MemberOf",
					Properties: properties,
				}})
			default:
				t.Fatalf("unknown writer %q", writerName)
			}
			if err == nil {
				t.Fatalf("%s writer unexpectedly accepted cyclic %s properties", writerName, cycle.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), "cycle") {
				t.Fatalf("%s writer error = %q, want descriptive cycle error", writerName, err)
			}

			contents, readErr := os.ReadFile(temporary)
			if preexisting {
				if readErr != nil {
					t.Fatalf("read existing temporary file: %v", readErr)
				}
				if !bytes.Equal(contents, sentinel) {
					t.Fatalf("existing temporary file = %q, want unchanged %q", contents, sentinel)
				}
			} else if !os.IsNotExist(readErr) {
				t.Fatalf("temporary file was created or stat failed unexpectedly: %v", readErr)
			}
		}
	}
}
