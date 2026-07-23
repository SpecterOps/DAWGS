package retriever

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestParquetCollectionPublisherCombinesLogicalAndPhysicalMetadata(t *testing.T) {
	workspace := newLocalCollectionWorkspace(t.TempDir(), false)
	publisher := newParquetCollectionPublisher(workspace, 1)
	summary := shardSummary{
		ID:           shardID{Graph: "source", Phase: PhaseNodes, Number: 1},
		Rows:         2,
		ActionCounts: map[string]int{"redact": 1},
	}
	publisher.AddFragment(summary, parquetFragmentMetadata{
		Path:   "parquet/graphs/source/nodes-000001.parquet",
		Rows:   2,
		Bytes:  100,
		SHA256: "checksum",
	})
	publisher.AddGraph("source", 2, 0)

	manifestPath, err := publisher.PublishManifest(context.Background())
	if err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	successPath, err := publisher.PublishSuccess(context.Background())
	if err != nil {
		t.Fatalf("publish success: %v", err)
	}
	stored, err := readParquetManifest(workspace.Root())
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if manifestPath == "" || successPath == "" || len(stored.Graphs) != 1 || len(stored.Graphs[0].Files) != 1 {
		t.Fatalf("publication paths or manifest invalid: manifest=%q success=%q stored=%+v", manifestPath, successPath, stored)
	}
	file := stored.Graphs[0].Files[0]
	if file.Phase != PhaseNodes || file.Count != 2 || file.Bytes != 100 || file.ActionCounts["redact"] != 1 {
		t.Fatalf("Parquet file manifest = %+v", file)
	}
	summary.ActionCounts["redact"] = 9
	if file.ActionCounts["redact"] != 1 {
		t.Fatalf("Parquet manifest retained logical action-count map")
	}
}

type recordingCollectionPublisher struct {
	events *[]string
	err    error
}

func (s recordingCollectionPublisher) AddGraph(GraphManifest, GraphSchemaMetadata, GraphMetrics) {}

func (s recordingCollectionPublisher) Publish(context.Context) (collectionPublication, error) {
	*s.events = append(*s.events, "jsonl-manifest")
	return collectionPublication{Path: "manifest.json"}, s.err
}

type recordingParquetPublisher struct {
	events      *[]string
	manifestErr error
	successErr  error
}

func (s recordingParquetPublisher) AddFragment(shardSummary, parquetFragmentMetadata) {}

func (s recordingParquetPublisher) AddGraph(string, int64, int64) {}

func (s recordingParquetPublisher) PublishManifest(context.Context) (string, error) {
	*s.events = append(*s.events, "parquet-manifest")
	return parquetManifestFileName, s.manifestErr
}

func (s recordingParquetPublisher) PublishSuccess(context.Context) (string, error) {
	*s.events = append(*s.events, "parquet-success")
	return parquetSuccessFileName, s.successErr
}

func TestDumpOutputPublicationOrder(t *testing.T) {
	manifestFailure := errors.New("Parquet manifest failed")
	jsonlFailure := errors.New("JSONL manifest failed")
	successFailure := errors.New("Parquet success failed")
	tests := []struct {
		name        string
		jsonlErr    error
		manifestErr error
		successErr  error
		wantEvents  []string
		wantErr     error
	}{
		{name: "success", wantEvents: []string{"parquet-manifest", "jsonl-manifest", "parquet-success"}},
		{name: "Parquet manifest failure", manifestErr: manifestFailure, wantEvents: []string{"parquet-manifest"}, wantErr: manifestFailure},
		{name: "JSONL manifest failure", jsonlErr: jsonlFailure, wantEvents: []string{"parquet-manifest", "jsonl-manifest"}, wantErr: jsonlFailure},
		{name: "Parquet success failure", successErr: successFailure, wantEvents: []string{"parquet-manifest", "jsonl-manifest", "parquet-success"}, wantErr: successFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			jsonl := recordingCollectionPublisher{events: &events, err: test.jsonlErr}
			parquet := recordingParquetPublisher{events: &events, manifestErr: test.manifestErr, successErr: test.successErr}
			jsonlResult, parquetResult, err := publishDumpOutputs(context.Background(), jsonl, parquet)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("publish error = %v, want %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(events, test.wantEvents) {
				t.Fatalf("publication events = %v, want %v", events, test.wantEvents)
			}
			if test.wantErr == nil && (jsonlResult.Path == "" || parquetResult.ManifestPath == "" || parquetResult.SuccessPath == "") {
				t.Fatalf("successful publication results = JSONL %+v Parquet %+v", jsonlResult, parquetResult)
			}
		})
	}
}

func TestDumpOutputPublicationWithoutParquetUsesJSONLOnly(t *testing.T) {
	var events []string
	jsonl := recordingCollectionPublisher{events: &events}
	jsonlResult, parquetResult, err := publishDumpOutputs(context.Background(), jsonl, nil)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !reflect.DeepEqual(events, []string{"jsonl-manifest"}) || jsonlResult.Path == "" || parquetResult != (parquetPublication{}) {
		t.Fatalf("JSONL-only publication = events %v, JSONL %+v, Parquet %+v", events, jsonlResult, parquetResult)
	}
}
