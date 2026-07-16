package retriever

import (
	"reflect"
	"testing"
)

func TestGraphObserverCollectsSharedPipelineMetadata(t *testing.T) {
	observer := newGraphObserver("example", 2)
	if err := observer.ObserveNode(normalizedNode{ID: "1", Kinds: []string{"Admin", "User"}, Properties: map[string]any{}}, map[string]int{"preserve": 1}); err != nil {
		t.Fatalf("observe first node: %v", err)
	}
	if err := observer.ObserveNode(normalizedNode{ID: "2", Kinds: []string{"Group"}, Properties: map[string]any{}}, map[string]int{"redact": 1}); err != nil {
		t.Fatalf("observe second node: %v", err)
	}
	if err := observer.ObserveEdge(normalizedEdge{ID: "source-edge-id", StartID: "1", EndID: "2", Kind: "MemberOf", Properties: map[string]any{}}, map[string]int{"preserve": 1}); err != nil {
		t.Fatalf("observe edge: %v", err)
	}

	if observer.nodeCount != 2 || observer.edgeCount != 1 {
		t.Fatalf("observer totals: nodes=%d edges=%d", observer.nodeCount, observer.edgeCount)
	}
	if !reflect.DeepEqual(observer.nodeActionCounts, map[string]int{"preserve": 1, "redact": 1}) || !reflect.DeepEqual(observer.edgeActionCounts, map[string]int{"preserve": 1}) {
		t.Fatalf("observer actions: nodes=%v edges=%v", observer.nodeActionCounts, observer.edgeActionCounts)
	}
	if !reflect.DeepEqual(observer.Schema(), GraphSchemaMetadata{
		Name:      "example",
		NodeKinds: []string{"Admin", "Group", "User"},
		EdgeKinds: []string{"MemberOf"},
	}) {
		t.Fatalf("observer schema = %+v", observer.Schema())
	}

	metrics := observer.Metrics()
	if metrics.NodeCount != 2 || metrics.EdgeCount != 1 {
		t.Fatalf("observer metrics = %+v", metrics)
	}
}
