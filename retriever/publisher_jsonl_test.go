package retriever

import (
	"context"
	"reflect"
	"testing"
)

func TestNewJSONLFileManifestCombinesLogicalAndPhysicalMetadata(t *testing.T) {
	actionCounts := map[string]int{"redact": 2}
	summary := shardSummary{
		ID:           shardID{Graph: "example", Phase: PhaseNodes, Number: 3},
		Rows:         4,
		ActionCounts: actionCounts,
	}
	metadata := jsonlFragmentMetadata{
		Path:              "graphs/example/nodes-000003.jsonl.gz",
		Rows:              4,
		CompressedBytes:   100,
		UncompressedBytes: 200,
		SHA256:            "checksum",
	}

	actual := newJSONLFileManifest(summary, metadata)
	expected := FileManifest{
		Phase:             PhaseNodes,
		Path:              metadata.Path,
		Count:             4,
		CompressedBytes:   100,
		UncompressedBytes: 200,
		SHA256:            "checksum",
		ActionCounts:      map[string]int{"redact": 2},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("file manifest = %+v, want %+v", actual, expected)
	}

	actionCounts["redact"] = 9
	if actual.ActionCounts["redact"] != 2 {
		t.Fatalf("file manifest retained logical action-count map")
	}
}

func TestJSONLCollectionPublisherAggregatesGraphMetadata(t *testing.T) {
	workspace := newLocalCollectionWorkspace(t.TempDir(), false)
	options := DefaultDumpOptions(workspace.Root())
	publisher := newJSONLCollectionPublisher(workspace, "scripted", options, ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, 1)
	graphEntry := GraphManifest{
		Name:             "source",
		NodeActionCounts: map[string]int{"drop": 2},
		EdgeActionCounts: map[string]int{"redact": 1},
	}
	schemaEntry := GraphSchemaMetadata{Name: "source", NodeKinds: []string{}, EdgeKinds: []string{}}
	metricsEntry := newMetricsBuilder("source", 0).finalize()
	publisher.AddGraph(graphEntry, schemaEntry, metricsEntry)

	publication, err := publisher.Publish(context.Background())
	if err != nil {
		t.Fatalf("publish collection: %v", err)
	}
	stored, err := readManifest(workspace.Root())
	if err != nil {
		t.Fatalf("read published manifest: %v", err)
	}

	if !reflect.DeepEqual(publication.Manifest, stored) {
		t.Fatalf("published manifest differs from stored manifest")
	}
	if !reflect.DeepEqual(stored.Graphs, []GraphManifest{graphEntry}) {
		t.Fatalf("manifest graphs = %+v", stored.Graphs)
	}
	if !reflect.DeepEqual(stored.Schema.Graphs, []GraphSchemaMetadata{schemaEntry}) {
		t.Fatalf("manifest schema = %+v", stored.Schema.Graphs)
	}
	if stored.Metrics == nil || !reflect.DeepEqual(stored.Metrics.Graphs, []GraphMetrics{metricsEntry}) {
		t.Fatalf("manifest metrics = %+v", stored.Metrics)
	}
	if stored.Scrub.NodeActionCounts["drop"] != 2 || stored.Scrub.EdgeActionCounts["redact"] != 1 {
		t.Fatalf("manifest action counts = %+v", stored.Scrub)
	}
}
