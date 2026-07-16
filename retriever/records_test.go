package retriever

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func TestNormalizeNode(t *testing.T) {
	properties := map[string]any{
		"nested": map[string]any{"names": []string{"alice", "bob"}},
		"bytes":  []byte{1, 2, 3},
		"nil":    nil,
	}
	node := graph.NewNode(
		42,
		graph.AsProperties(properties),
		graph.StringKind("User"),
		graph.StringKind("Admin"),
	)

	normalized := normalizeNode(node)
	if normalized.ID != "42" {
		t.Fatalf("normalized ID = %q", normalized.ID)
	}
	if !reflect.DeepEqual(normalized.Kinds, []string{"Admin", "User"}) {
		t.Fatalf("normalized kinds = %v", normalized.Kinds)
	}
	if !reflect.DeepEqual(node.Kinds.Strings(), []string{"User", "Admin"}) {
		t.Fatalf("normalization modified source kinds: %v", node.Kinds.Strings())
	}
	if !reflect.DeepEqual(normalized.Properties, properties) {
		t.Fatalf("normalized properties = %#v, want %#v", normalized.Properties, properties)
	}

	properties["new"] = "source-only"
	if _, found := normalized.Properties["new"]; found {
		t.Fatalf("normalized properties alias source map: %#v", normalized.Properties)
	}

	normalized.Kinds[0] = "Changed"
	normalized.Properties["other"] = "normalized-only"
	if !reflect.DeepEqual(node.Kinds.Strings(), []string{"User", "Admin"}) {
		t.Fatalf("normalized kinds alias source kinds: %v", node.Kinds.Strings())
	}
	if _, found := properties["other"]; found {
		t.Fatalf("source properties changed through normalized map: %#v", properties)
	}
}

func TestNormalizeNodeWithoutPropertiesOwnsEmptyMap(t *testing.T) {
	normalized := normalizeNode(graph.NewNode(1, nil))
	if normalized.Properties == nil || len(normalized.Properties) != 0 {
		t.Fatalf("normalized properties = %#v", normalized.Properties)
	}
}

func TestNormalizeEdge(t *testing.T) {
	properties := map[string]any{
		"routes": []any{"north", map[string]any{"weight": int64(2)}},
	}
	relationship := graph.NewRelationship(
		99,
		10,
		20,
		graph.AsProperties(properties),
		graph.StringKind("AdminTo"),
	)

	normalized := normalizeEdge(relationship)
	expected := normalizedEdge{
		ID:         "99",
		StartID:    "10",
		EndID:      "20",
		Kind:       "AdminTo",
		Properties: map[string]any{"routes": []any{"north", map[string]any{"weight": int64(2)}}},
	}
	if !reflect.DeepEqual(normalized, expected) {
		t.Fatalf("normalized edge = %#v, want %#v", normalized, expected)
	}

	properties["routes"] = []any{"changed"}
	if !reflect.DeepEqual(normalized, expected) {
		t.Fatalf("normalized edge properties alias source map: %#v", normalized.Properties)
	}
}

func TestNormalizeEdgeWithoutKind(t *testing.T) {
	normalized := normalizeEdge(graph.NewRelationship(3, 1, 2, nil, nil))
	if normalized.Kind != "" {
		t.Fatalf("normalized missing kind = %q", normalized.Kind)
	}
	if normalized.Properties == nil || len(normalized.Properties) != 0 {
		t.Fatalf("normalized properties = %#v", normalized.Properties)
	}
}

func TestJSONLV1NormalizedRecordAdapters(t *testing.T) {
	node := normalizedNode{
		ID:         "1",
		Kinds:      []string{"Admin", "User"},
		Properties: map[string]any{"name": "alice"},
	}
	if actual := jsonlV1NodeFromNormalized(node); !reflect.DeepEqual(actual, FragmentNode{
		ID:         "1",
		Kinds:      []string{"Admin", "User"},
		Properties: map[string]any{"name": "alice"},
	}) {
		t.Fatalf("JSONL node adapter = %#v", actual)
	}

	edge := normalizedEdge{
		ID:         "source-edge-id",
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"route": "north"},
	}
	expectedEdge := FragmentEdge{
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"route": "north"},
	}
	if actual := jsonlV1EdgeFromNormalized(edge); !reflect.DeepEqual(actual, expectedEdge) {
		t.Fatalf("JSONL edge adapter = %#v, want %#v", actual, expectedEdge)
	}

	edge.ID = "different-source-edge-id"
	if actual := jsonlV1EdgeFromNormalized(edge); !reflect.DeepEqual(actual, expectedEdge) {
		t.Fatalf("source edge ID affected JSONL v1 adapter: %#v", actual)
	}
}
