package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	cypherModel "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/jsonl"
	"github.com/specterops/dawgs/ret/parquet"
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
				Format:            "parquet",
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
	for _, expected := range []string{"graph: default", "format=parquet", "workers=2", "sample_size=2", "nodes=2/3", "edges=2/4", "entities_per_sec=140.00", "db_read_ms=30"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("bench report missing %q in %q", expected, output)
		}
	}
}

func TestLogBenchPhaseProgressThresholds(t *testing.T) {
	planned := retrieverProgressEntityInterval * 3
	nextProgressAt := retrieverProgressEntityInterval
	startedAt := time.Now().Add(-time.Second)

	if got := logBenchPhaseProgress("default", "nodes", 1, benchPhaseResult{
		Count: nextProgressAt - 1,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt {
		t.Fatalf("progress before threshold advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", "nodes", 1, benchPhaseResult{
		Count: nextProgressAt,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt*2 {
		t.Fatalf("progress at threshold advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", "nodes", 1, benchPhaseResult{
		Count: nextProgressAt*2 + 1,
	}, planned*2, startedAt, nextProgressAt); got != nextProgressAt*3 {
		t.Fatalf("progress after large jump advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", "nodes", 1, benchPhaseResult{
		Count: planned,
	}, planned, startedAt, nextProgressAt); got != nextProgressAt {
		t.Fatalf("completed progress advanced to %d", got)
	}
	if got := logBenchPhaseProgress("default", "nodes", 1, benchPhaseResult{
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

func TestBenchBatchProcessorReturnsParentCancellationWithQueuedJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	processor, _, err := newBenchBatchProcessor(ctx, 2, func(values []int) (benchPhaseResult, error) {
		started <- struct{}{}
		<-release
		return benchPhaseResult{Count: int64(len(values))}, nil
	})
	if err != nil {
		t.Fatalf("create bench batch processor: %v", err)
	}

	if err := processor.handle([]int{1}); err != nil {
		t.Fatalf("handle first active batch: %v", err)
	}
	if err := processor.handle([]int{2}); err != nil {
		t.Fatalf("handle second active batch: %v", err)
	}
	<-started
	<-started
	if err := processor.handle([]int{3}); err != nil {
		t.Fatalf("queue third batch: %v", err)
	}
	if err := processor.handle([]int{4}); err != nil {
		t.Fatalf("queue fourth batch: %v", err)
	}

	cancel()
	close(release)
	if _, err := processor.closeAndWait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want caller cancellation", err)
	}
}

func TestBenchBatchProcessorPrefersWriterErrorOverParentCancellation(t *testing.T) {
	writerFailure := errors.New("writer failed")
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	processor, _, err := newBenchBatchProcessor(ctx, 2, func([]int) (benchPhaseResult, error) {
		close(started)
		<-release
		return benchPhaseResult{}, writerFailure
	})
	if err != nil {
		t.Fatalf("create bench batch processor: %v", err)
	}
	if err := processor.handle([]int{1}); err != nil {
		t.Fatalf("handle active batch: %v", err)
	}
	<-started
	cancel()
	close(release)

	if _, err := processor.closeAndWait(); !errors.Is(err, writerFailure) {
		t.Fatalf("wait error = %v, want writer failure", err)
	}
}

func TestBenchSourcePhasesPreserveSamplePrefixAndAggregateWorkers(t *testing.T) {
	database := &benchSourceDatabase{
		nodes: []*graph.Node{
			graph.NewNode(1, graph.AsProperties(map[string]any{"name": "one"}), graph.StringKind("User"), graph.StringKind("Admin"), graph.StringKind("User")),
			graph.NewNode(2, graph.AsProperties(map[string]any{"name": "two"}), graph.StringKind("Computer")),
			graph.NewNode(3, graph.AsProperties(map[string]any{"name": "three"}), graph.StringKind("Group")),
			graph.NewNode(4, graph.AsProperties(map[string]any{"name": "four"}), graph.StringKind("Role")),
		},
		relationships: []*graph.Relationship{
			graph.NewRelationship(10, 1, 2, graph.AsProperties(map[string]any{"position": 1}), graph.StringKind("First")),
			graph.NewRelationship(11, 2, 3, graph.AsProperties(map[string]any{"position": 2}), graph.StringKind("Second")),
			graph.NewRelationship(12, 3, 4, graph.AsProperties(map[string]any{"position": 3}), graph.StringKind("Third")),
			graph.NewRelationship(13, 4, 1, graph.AsProperties(map[string]any{"position": 4}), graph.StringKind("Fourth")),
		},
	}
	options := benchOptions{BatchSize: 2, SampleSize: 3}

	var (
		mu                     sync.Mutex
		nodes                  []entity.Node
		nodeBatchSizes         []int
		relationships          []entity.Relationship
		relationshipBatchSizes []int
	)
	nodeResult, err := benchNodes(
		context.Background(), database, "example", "jsonl", int64(len(database.nodes)), 2, options,
		func(_ string, batch []entity.Node) (benchPhaseResult, error) {
			mu.Lock()
			nodes = append(nodes, batch...)
			nodeBatchSizes = append(nodeBatchSizes, len(batch))
			mu.Unlock()
			return benchPhaseResult{Count: int64(len(batch))}, nil
		},
	)
	if err != nil {
		t.Fatalf("bench nodes: %v", err)
	}
	relationshipResult, err := benchRelationships(
		context.Background(), database, "example", "parquet", int64(len(database.relationships)), 2, options,
		func(_ string, batch []entity.Relationship) (benchPhaseResult, error) {
			mu.Lock()
			relationships = append(relationships, batch...)
			relationshipBatchSizes = append(relationshipBatchSizes, len(batch))
			mu.Unlock()
			return benchPhaseResult{Count: int64(len(batch))}, nil
		},
	)
	if err != nil {
		t.Fatalf("bench relationships: %v", err)
	}

	if nodeResult.Count != 3 || relationshipResult.Count != 3 {
		t.Fatalf("phase counts = nodes %d relationships %d, want 3 each", nodeResult.Count, relationshipResult.Count)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].SourceID < nodes[j].SourceID })
	if got := []string{nodes[0].SourceID, nodes[1].SourceID, nodes[2].SourceID}; !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		t.Fatalf("node prefix = %v, want [1 2 3]", got)
	}
	if got := nodes[0].Kinds; !reflect.DeepEqual(got, []string{"User", "Admin", "User"}) {
		t.Fatalf("first node kinds = %v", got)
	}
	sort.Ints(nodeBatchSizes)
	if !reflect.DeepEqual(nodeBatchSizes, []int{1, 2}) {
		t.Fatalf("node batch sizes = %v, want final partial batch", nodeBatchSizes)
	}

	sort.Slice(relationships, func(i, j int) bool { return relationships[i].SourceID < relationships[j].SourceID })
	if got := []string{relationships[0].SourceID, relationships[1].SourceID, relationships[2].SourceID}; !reflect.DeepEqual(got, []string{"10", "11", "12"}) {
		t.Fatalf("relationship prefix/source IDs = %v, want [10 11 12]", got)
	}
	sort.Ints(relationshipBatchSizes)
	if !reflect.DeepEqual(relationshipBatchSizes, []int{1, 2}) {
		t.Fatalf("relationship batch sizes = %v, want final partial batch", relationshipBatchSizes)
	}
}

func TestBenchSourcePhaseErrorsAndCountMismatches(t *testing.T) {
	sourceFailure := errors.New("source failed")
	database := &benchSourceDatabase{
		nodes:        []*graph.Node{graph.NewNode(1, graph.NewProperties(), graph.StringKind("User"))},
		nodeFetchErr: sourceFailure,
	}
	options := benchOptions{BatchSize: 1, SampleSize: 1}
	if _, err := benchNodes(
		context.Background(), database, "example", "jsonl", 1, 1, options,
		func(_ string, batch []entity.Node) (benchPhaseResult, error) {
			return benchPhaseResult{Count: int64(len(batch))}, nil
		},
	); !errors.Is(err, sourceFailure) {
		t.Fatalf("node source error = %v, want source failure", err)
	}

	database.nodeFetchErr = nil
	if _, err := benchNodes(
		context.Background(), database, "example", "jsonl", 1, 1, options,
		func(string, []entity.Node) (benchPhaseResult, error) {
			return benchPhaseResult{}, nil
		},
	); err == nil || !strings.Contains(err.Error(), "wrote 0 of 1") {
		t.Fatalf("node count mismatch error = %v", err)
	}

	database.relationships = []*graph.Relationship{
		graph.NewRelationship(2, 1, 3, graph.NewProperties(), graph.StringKind("MemberOf")),
	}
	database.relationshipFetchErr = sourceFailure
	if _, err := benchRelationships(
		context.Background(), database, "example", "parquet", 1, 1, options,
		func(_ string, batch []entity.Relationship) (benchPhaseResult, error) {
			return benchPhaseResult{Count: int64(len(batch))}, nil
		},
	); !errors.Is(err, sourceFailure) {
		t.Fatalf("relationship source error = %v, want source failure", err)
	}

	database.relationshipFetchErr = nil
	if _, err := benchRelationships(
		context.Background(), database, "example", "parquet", 1, 1, options,
		func(string, []entity.Relationship) (benchPhaseResult, error) {
			return benchPhaseResult{}, nil
		},
	); err == nil || !strings.Contains(err.Error(), "wrote 0 of 1") {
		t.Fatalf("relationship count mismatch error = %v", err)
	}
}

func TestBenchPhaseReportsArtifactCleanupErrors(t *testing.T) {
	cleanupFailure := errors.New("cleanup failed")
	phaseFailure := errors.New("phase failed")
	filesystem := benchArtifactFilesystem{
		removeAll: func(path string) error {
			if err := os.RemoveAll(path); err != nil {
				t.Fatalf("remove benchmark test directory: %v", err)
			}
			return cleanupFailure
		},
	}
	options := benchOptions{BatchSize: 1}
	nodeDatabase := &benchSourceDatabase{
		nodes:        []*graph.Node{graph.NewNode(1, graph.NewProperties(), graph.StringKind("User"))},
		nodeFetchErr: phaseFailure,
	}

	if _, err := benchNodesWithFilesystem(
		context.Background(), nodeDatabase, "example", "jsonl", 1, 1, options,
		func(string, []entity.Node) (benchPhaseResult, error) { return benchPhaseResult{}, nil },
		filesystem,
	); !errors.Is(err, phaseFailure) || !errors.Is(err, cleanupFailure) {
		t.Fatalf("node cleanup error = %v, want joined phase and cleanup failures", err)
	}
	if _, err := benchRelationshipsWithFilesystem(
		context.Background(), emptyBenchDatabase{}, "example", "parquet", 0, 1, options,
		func(string, []entity.Relationship) (benchPhaseResult, error) { return benchPhaseResult{}, nil },
		filesystem,
	); !errors.Is(err, cleanupFailure) {
		t.Fatalf("relationship cleanup error = %v, want cleanup failure", err)
	}
}

func TestBenchConcreteFormatBatches(t *testing.T) {
	jsonlDirectory := t.TempDir()
	jsonlPath := filepath.Join(jsonlDirectory, "nodes.jsonl.gz")
	nodes := []entity.Node{
		{SourceID: "2", Kinds: []string{"User", "Admin", "User"}, Properties: map[string]any{"name": "alice"}},
		{SourceID: "1", Kinds: []string{"Computer"}},
	}
	jsonlResult, err := benchJSONLNodeBatch(
		jsonlPath,
		nodes,
		jsonl.Config{Codec: jsonl.CodecGzip},
	)
	if err != nil {
		t.Fatalf("bench JSONL node batch: %v", err)
	}
	if jsonlResult.Count != 2 || jsonlResult.UncompressedByteSize <= 0 || jsonlResult.CompressedByteSize <= 0 {
		t.Fatalf("unexpected JSONL node batch result: %+v", jsonlResult)
	}
	file, err := openGzipJSONL(jsonlPath)
	if err != nil {
		t.Fatalf("open benchmark JSONL: %v", err)
	}
	defer file.Close()
	var first struct {
		Kinds []string `json:"kinds"`
	}
	if err := json.NewDecoder(file).Decode(&first); err != nil {
		t.Fatalf("decode benchmark JSONL: %v", err)
	}
	if got := strings.Join(first.Kinds, ","); got != "User,Admin,User" {
		t.Fatalf("node kind order = %q", got)
	}
	parquetResult, err := benchParquetNodeBatch(
		filepath.Join(t.TempDir(), "nodes.parquet"),
		nodes,
		parquet.Config{},
	)
	if err != nil {
		t.Fatalf("bench Parquet node batch: %v", err)
	}
	if parquetResult.Count != 2 || parquetResult.UncompressedByteSize != 0 || parquetResult.CompressedByteSize <= 0 {
		t.Fatalf("unexpected Parquet node batch result: %+v", parquetResult)
	}

	relationships := []entity.Relationship{
		{SourceID: "10", StartID: "1", EndID: "2", Kind: "AdminTo", Properties: map[string]any{"source": "test"}},
		{SourceID: "11", StartID: "2", EndID: "3", Kind: "MemberOf"},
	}
	relationshipResult, err := benchParquetRelationshipBatch(
		filepath.Join(t.TempDir(), "relationships.parquet"),
		relationships,
		parquet.Config{},
	)
	if err != nil {
		t.Fatalf("bench Parquet relationship batch: %v", err)
	}
	if relationshipResult.Count != 2 || relationshipResult.CompressedByteSize <= 0 {
		t.Fatalf("unexpected relationship batch result: %+v", relationshipResult)
	}
}

func TestBenchReportsBothConcreteFormatsIndependently(t *testing.T) {
	report, err := Bench(context.Background(), emptyBenchDatabase{}, "pg", []string{"default"}, benchOptions{
		Workers:    []int{1},
		BatchSize:  10,
		SampleSize: 10,
		JSONL:      &jsonl.Config{Codec: jsonl.CodecZstd},
		Parquet:    &parquet.Config{},
	})
	if err != nil {
		t.Fatalf("bench: %v", err)
	}
	if len(report.Graphs) != 1 || len(report.Graphs[0].Results) != 2 {
		t.Fatalf("report = %+v", report)
	}
	if got := report.Graphs[0].Results[0].Format; got != "jsonl" {
		t.Fatalf("first format = %q", got)
	}
	if got := report.Graphs[0].Results[1].Format; got != "parquet" {
		t.Fatalf("second format = %q", got)
	}
}

func openGzipJSONL(path string) (*gzip.Reader, error) {
	stored, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return gzip.NewReader(bytes.NewReader(stored))
}

type emptyBenchDatabase struct {
	graph.Database
}

func (emptyBenchDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(emptyBenchTransaction{})
}

type emptyBenchTransaction struct {
	graph.Transaction
}

func (emptyBenchTransaction) WithGraph(graph.Graph) graph.Transaction {
	return emptyBenchTransaction{}
}

func (emptyBenchTransaction) Nodes() graph.NodeQuery {
	return emptyBenchNodeQuery{}
}

func (emptyBenchTransaction) Relationships() graph.RelationshipQuery {
	return emptyBenchRelationshipQuery{}
}

type emptyBenchNodeQuery struct {
	graph.NodeQuery
}

func (emptyBenchNodeQuery) Count() (int64, error) {
	return 0, nil
}

type emptyBenchRelationshipQuery struct {
	graph.RelationshipQuery
}

func (emptyBenchRelationshipQuery) Count() (int64, error) {
	return 0, nil
}

type benchTestCursor[T any] struct {
	values chan T
	err    error
}

func newBenchTestCursor[T any](values []T) *benchTestCursor[T] {
	channel := make(chan T, len(values))
	for _, value := range values {
		channel <- value
	}
	close(channel)
	return &benchTestCursor[T]{values: channel}
}

func (s *benchTestCursor[T]) Error() error { return s.err }
func (s *benchTestCursor[T]) Close()       {}
func (s *benchTestCursor[T]) Chan() chan T { return s.values }

type benchSourceDatabase struct {
	graph.Database
	nodes                []*graph.Node
	relationships        []*graph.Relationship
	nodeFetchErr         error
	relationshipFetchErr error
}

func (s *benchSourceDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&benchSourceTransaction{database: s})
}

type benchSourceTransaction struct {
	graph.Transaction
	database *benchSourceDatabase
}

func (s *benchSourceTransaction) WithGraph(graph.Graph) graph.Transaction { return s }
func (s *benchSourceTransaction) Nodes() graph.NodeQuery {
	return &benchSourceNodeQuery{database: s.database}
}
func (s *benchSourceTransaction) Relationships() graph.RelationshipQuery {
	return &benchSourceRelationshipQuery{database: s.database}
}

type benchSourceNodeQuery struct {
	graph.NodeQuery
	database *benchSourceDatabase
	afterID  graph.ID
	limit    int
}

func (s *benchSourceNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery { return s }
func (s *benchSourceNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.afterID = benchSourceAfterID(criteria)
	return s
}
func (s *benchSourceNodeQuery) Limit(limit int) graph.NodeQuery {
	s.limit = limit
	return s
}
func (s *benchSourceNodeQuery) Count() (int64, error) {
	return int64(len(s.database.nodes)), nil
}
func (s *benchSourceNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	if s.database.nodeFetchErr != nil {
		return s.database.nodeFetchErr
	}
	values := make([]*graph.Node, 0, s.limit)
	for _, node := range s.database.nodes {
		if node.ID > s.afterID && len(values) < s.limit {
			values = append(values, node)
		}
	}
	return delegate(newBenchTestCursor(values))
}

type benchSourceRelationshipQuery struct {
	graph.RelationshipQuery
	database *benchSourceDatabase
	afterID  graph.ID
	limit    int
}

func (s *benchSourceRelationshipQuery) OrderBy(...graph.Criteria) graph.RelationshipQuery {
	return s
}
func (s *benchSourceRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	s.afterID = benchSourceAfterID(criteria)
	return s
}
func (s *benchSourceRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	s.limit = limit
	return s
}
func (s *benchSourceRelationshipQuery) Count() (int64, error) {
	return int64(len(s.database.relationships)), nil
}
func (s *benchSourceRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	if s.database.relationshipFetchErr != nil {
		return s.database.relationshipFetchErr
	}
	values := make([]*graph.Relationship, 0, s.limit)
	for _, relationship := range s.database.relationships {
		if relationship.ID > s.afterID && len(values) < s.limit {
			values = append(values, relationship)
		}
	}
	return delegate(newBenchTestCursor(values))
}

func benchSourceAfterID(criteria graph.Criteria) graph.ID {
	comparison := criteria.(*cypherModel.Comparison)
	return comparison.Partials[0].Right.(*cypherModel.Parameter).Value.(graph.ID)
}
