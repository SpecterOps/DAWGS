package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/retriever"
)

func TestBenchSamplingHelpers(t *testing.T) {
	if got := benchPlannedCount(10, 3); got != 3 {
		t.Fatalf("planned sampled count = %d", got)
	}
	if got := benchPlannedCount(10, 0); got != 10 {
		t.Fatalf("planned full count = %d", got)
	}
	if got := benchPlannedCount(3, 10); got != 3 {
		t.Fatalf("planned capped count = %d", got)
	}
	if got := benchPlannedCount(0, 10); got != 0 {
		t.Fatalf("planned empty count = %d", got)
	}
	if got := retrieverBatchLimit(3, 10); got != 3 {
		t.Fatalf("batch limit for remainder = %d", got)
	}
	if got := retrieverBatchLimit(20, 10); got != 10 {
		t.Fatalf("batch limit for full batch = %d", got)
	}
	if got := retrieverInitialProgressAt(retrieverProgressEntityInterval); got != 0 {
		t.Fatalf("unexpected progress threshold for exact interval: %d", got)
	}
	if got := retrieverInitialProgressAt(retrieverProgressEntityInterval + 1); got != retrieverProgressEntityInterval {
		t.Fatalf("unexpected progress threshold: %d", got)
	}
}

func TestBenchFormattingHelpers(t *testing.T) {
	if got := perSecond(10, 2*time.Second); got != 5 {
		t.Fatalf("perSecond = %f", got)
	}
	if got := perSecond(10, 0); got != 0 {
		t.Fatalf("perSecond with zero duration = %f", got)
	}

	var buffer bytes.Buffer
	writeBenchReport(&buffer, benchReport{
		Graphs: []benchGraphReport{{
			Name: "default",
			Results: []benchResult{{
				Workers:           2,
				BatchSize:         100,
				SampleSize:        2,
				NodeCount:         3,
				EdgeCount:         4,
				NodeProcessed:     2,
				EdgeProcessed:     2,
				TotalWallMillis:   50,
				EntitiesPerSecond: 140,
				NodeDBReadMillis:  10,
				EdgeDBReadMillis:  20,
			}},
		}},
	})
	output := buffer.String()
	for _, expected := range []string{"graph: default", "workers=2", "sample_size=2", "nodes=2/3", "edges=2/4", "entities_per_sec=140.00", "db_read_ms=30"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("bench report missing %q in %q", expected, output)
		}
	}
}

func TestLogBenchPhaseProgressThresholds(t *testing.T) {
	planned := retrieverProgressEntityInterval * 3
	nextProgressAt := retrieverProgressEntityInterval
	startedAt := time.Now().Add(-time.Second)

	if got := logBenchPhaseProgress("default", retriever.PhaseNodes, 1, benchPhaseResult{
		Count: nextProgressAt - 1,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt {
		t.Fatalf("progress before threshold advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", retriever.PhaseNodes, 1, benchPhaseResult{
		Count: nextProgressAt,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt*2 {
		t.Fatalf("progress at threshold advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", retriever.PhaseNodes, 1, benchPhaseResult{
		Count: nextProgressAt*2 + 1,
	}, planned*2, startedAt, nextProgressAt); got != nextProgressAt*3 {
		t.Fatalf("progress after large jump advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", retriever.PhaseNodes, 1, benchPhaseResult{
		Count: planned,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt {
		t.Fatalf("completed progress advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", retriever.PhaseNodes, 1, benchPhaseResult{
		Count: nextProgressAt,
	}, planned, startedAt, 0); got != 0 {
		t.Fatalf("disabled progress advanced to %d", got)
	}
}

func TestBenchBatchProcessorAggregatesConcurrentResults(t *testing.T) {
	processor, _, err := newBenchBatchProcessor(context.Background(), 2, func(values []int) (benchPhaseResult, error) {
		return benchPhaseResult{
			Count:                int64(len(values)),
			EncodeCompressTime:   time.Duration(len(values)) * time.Millisecond,
			UncompressedByteSize: int64(len(values) * 10),
			CompressedByteSize:   int64(len(values) * 5),
		}, nil
	})
	if err != nil {
		t.Fatalf("create bench batch processor: %v", err)
	}

	processor.addDBReadElapsed(3 * time.Millisecond)
	if err := processor.handle([]int{1}); err != nil {
		t.Fatalf("handle first batch: %v", err)
	}
	if err := processor.handle([]int{2, 3}); err != nil {
		t.Fatalf("handle second batch: %v", err)
	}
	result, err := processor.closeAndWait()
	if err != nil {
		t.Fatalf("wait for bench batch processor: %v", err)
	}
	if result.Count != 3 {
		t.Fatalf("processed count = %d", result.Count)
	}
	if result.DBReadElapsed != 3*time.Millisecond {
		t.Fatalf("db read elapsed = %s", result.DBReadElapsed)
	}
	if result.EncodeCompressTime != 3*time.Millisecond {
		t.Fatalf("encode/compress elapsed = %s", result.EncodeCompressTime)
	}
	if result.UncompressedByteSize != 30 || result.CompressedByteSize != 15 {
		t.Fatalf("unexpected byte sizes: %+v", result)
	}
}

func TestBenchBatchCompression(t *testing.T) {
	options := benchOptions{
		Compression: retriever.CompressionGzip,
		ZstdLevel:   retriever.DefaultZstdLevel,
	}
	nodes := []*graph.Node{
		graph.NewNode(2, graph.AsProperties(map[string]any{"name": "alice"}), graph.StringKind("User"), graph.StringKind("Admin")),
		graph.NewNode(1, nil, graph.StringKind("Computer")),
	}

	nodeResult, err := benchNodeBatch(nodes, options)
	if err != nil {
		t.Fatalf("bench node batch: %v", err)
	}
	if nodeResult.Count != 2 || nodeResult.UncompressedByteSize <= 0 || nodeResult.CompressedByteSize <= 0 {
		t.Fatalf("unexpected node batch result: %+v", nodeResult)
	}

	noCompressionResult, err := benchNodeBatch(nodes, benchOptions{
		Compression: retriever.CompressionDisabled,
		ZstdLevel:   retriever.DefaultZstdLevel,
	})
	if err != nil {
		t.Fatalf("bench uncompressed node batch: %v", err)
	}
	if noCompressionResult.Count != 2 || noCompressionResult.UncompressedByteSize != 0 || noCompressionResult.CompressedByteSize != 0 {
		t.Fatalf("unexpected no-compression node result: %+v", noCompressionResult)
	}

	relationships := []*graph.Relationship{
		graph.NewRelationship(10, 1, 2, graph.AsProperties(map[string]any{"source": "test"}), graph.StringKind("AdminTo")),
		graph.NewRelationship(11, 2, 3, nil, nil),
	}
	relationshipResult, err := benchRelationshipBatch(relationships, options)
	if err != nil {
		t.Fatalf("bench relationship batch: %v", err)
	}
	if relationshipResult.Count != 2 || relationshipResult.UncompressedByteSize <= 0 || relationshipResult.CompressedByteSize <= 0 {
		t.Fatalf("unexpected relationship batch result: %+v", relationshipResult)
	}
}
