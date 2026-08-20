package pg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"iter"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const (
	postgresIngestBenchmarkNodesEnv          = "DAWGS_INGEST_BENCH_NODES"
	postgresIngestBenchmarkEdgesEnv          = "DAWGS_INGEST_BENCH_EDGES"
	postgresIngestBenchmarkChangePercentEnv  = "DAWGS_INGEST_BENCH_CHANGE_PERCENT"
	postgresIngestBenchmarkBucketsEnv        = "DAWGS_INGEST_BENCH_BUCKETS"
	postgresIngestBenchmarkClusterEnv        = "DAWGS_INGEST_BENCH_CLUSTER"
	postgresIngestBenchmarkDefaultNodeCount  = 100_000
	postgresIngestBenchmarkDefaultEdgeCount  = 200_000
	postgresIngestBenchmarkDefaultChangeRate = 1
	postgresIngestBenchmarkBatchSize         = 2_000
	postgresIngestBenchmarkMaxEntityCount    = 1<<31 - 1
)

var (
	postgresIngestBenchmarkDefaultBuckets = []int{256, 4_096, 65_536}
	postgresIngestBenchmarkDefaultCluster = []bool{false, true}
	postgresIngestBenchmarkScenarios      = []postgresIngestBenchmarkScenario{
		ingestBenchmarkFreshInsert,
		ingestBenchmarkDenseFullReplay,
		ingestBenchmarkDenseChange,
		ingestBenchmarkPartialMergeNoop,
		ingestBenchmarkSparseChange,
	}
)

type postgresIngestBenchmarkPath string

const (
	postgresIngestBenchmarkBatchPath  postgresIngestBenchmarkPath = "batch_operation"
	postgresIngestBenchmarkIngestPath postgresIngestBenchmarkPath = "driver_ingest"
)

type postgresIngestBenchmarkScenario string

const (
	ingestBenchmarkFreshInsert      postgresIngestBenchmarkScenario = "fresh_insert"
	ingestBenchmarkDenseFullReplay  postgresIngestBenchmarkScenario = "dense_full_replay"
	ingestBenchmarkDenseChange      postgresIngestBenchmarkScenario = "dense_one_percent_change"
	ingestBenchmarkPartialMergeNoop postgresIngestBenchmarkScenario = "partial_merge_noop"
	ingestBenchmarkSparseChange     postgresIngestBenchmarkScenario = "sparse_change"
)

type postgresIngestBenchmarkConfig struct {
	NodeCount     int
	EdgeCount     int
	ChangePercent int
	BucketCounts  []int
	ClusterModes  []bool
}

type postgresIngestBenchmarkDataset struct {
	nodeCount     int
	edgeCount     int
	changedNodes  int
	changedEdges  int
	benchmarkNode graph.Kind
	benchmarkEven graph.Kind
	benchmarkOdd  graph.Kind
	benchmarkEdge graph.Kind
}

type postgresIngestBenchmarkBucketMetrics struct {
	PopulatedBuckets int64
	IdentityRowsRead int64
}

type postgresIngestBenchmarkLogicalRecordType byte

const (
	postgresIngestBenchmarkLogicalNode postgresIngestBenchmarkLogicalRecordType = 1
	postgresIngestBenchmarkLogicalEdge postgresIngestBenchmarkLogicalRecordType = 2
)

type postgresIngestBenchmarkLogicalRecord struct {
	recordType    postgresIngestBenchmarkLogicalRecordType
	objectID      string
	kinds         []string
	startObjectID string
	edgeKind      string
	endObjectID   string
	properties    *graph.Properties
}

type postgresIngestBenchmarkLogicalValidation struct {
	Checksum [sha256.Size]byte
	Nodes    int64
	Edges    int64
}

func evictPostgresIngestBenchmarkGraph(manager *SchemaManager, name string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	delete(manager.graphs, name)
}

func assertPostgresIngestBenchmarkKinds(
	ctx context.Context,
	asserter ingestKindAsserter,
	dataset postgresIngestBenchmarkDataset,
) error {
	_, err := asserter.AssertKinds(ctx, graph.Kinds{
		dataset.benchmarkNode,
		dataset.benchmarkEven,
		dataset.benchmarkOdd,
		dataset.benchmarkEdge,
	})
	if err != nil {
		return fmt.Errorf("assert PostgreSQL ingest benchmark kinds: %w", err)
	}

	return nil
}

type recordingPostgresIngestBenchmarkKindAsserter struct {
	calls int
	kinds graph.Kinds
}

func (s *recordingPostgresIngestBenchmarkKindAsserter) AssertKinds(
	_ context.Context,
	kinds graph.Kinds,
) ([]int16, error) {
	s.calls++
	s.kinds = append(graph.Kinds(nil), kinds...)

	return []int16{1, 2, 3, 4}, nil
}

func loadPostgresIngestBenchmarkConfig(
	lookup func(string) (string, bool),
) (postgresIngestBenchmarkConfig, error) {
	if lookup == nil {
		return postgresIngestBenchmarkConfig{}, fmt.Errorf("PostgreSQL ingest benchmark environment lookup is nil")
	}

	nodeCount, err := parsePostgresIngestBenchmarkPositiveInt(
		lookup,
		postgresIngestBenchmarkNodesEnv,
		postgresIngestBenchmarkDefaultNodeCount,
	)
	if err != nil {
		return postgresIngestBenchmarkConfig{}, err
	}
	edgeCount, err := parsePostgresIngestBenchmarkPositiveInt(
		lookup,
		postgresIngestBenchmarkEdgesEnv,
		postgresIngestBenchmarkDefaultEdgeCount,
	)
	if err != nil {
		return postgresIngestBenchmarkConfig{}, err
	}
	if uint64(edgeCount) > uint64(nodeCount)*uint64(nodeCount) {
		return postgresIngestBenchmarkConfig{}, fmt.Errorf(
			"%s=%q cannot describe unique directed edges for %s=%d; require edges <= nodes^2",
			postgresIngestBenchmarkEdgesEnv,
			strconv.Itoa(edgeCount),
			postgresIngestBenchmarkNodesEnv,
			nodeCount,
		)
	}

	changePercent, err := parsePostgresIngestBenchmarkChangePercent(lookup)
	if err != nil {
		return postgresIngestBenchmarkConfig{}, err
	}
	bucketCounts, err := parsePostgresIngestBenchmarkBucketCounts(lookup)
	if err != nil {
		return postgresIngestBenchmarkConfig{}, err
	}
	clusterModes, err := parsePostgresIngestBenchmarkClusterModes(lookup)
	if err != nil {
		return postgresIngestBenchmarkConfig{}, err
	}

	return postgresIngestBenchmarkConfig{
		NodeCount:     nodeCount,
		EdgeCount:     edgeCount,
		ChangePercent: changePercent,
		BucketCounts:  bucketCounts,
		ClusterModes:  clusterModes,
	}, nil
}

func parsePostgresIngestBenchmarkPositiveInt(
	lookup func(string) (string, bool),
	name string,
	defaultValue int,
) (int, error) {
	raw, found := lookup(name)
	if !found {
		return defaultValue, nil
	}

	trimmed := strings.TrimSpace(raw)
	value, err := strconv.ParseUint(trimmed, 10, 31)
	if err != nil || value == 0 || value > postgresIngestBenchmarkMaxEntityCount {
		return 0, fmt.Errorf(
			"%s=%q must be a positive base-10 integer no greater than %d",
			name,
			raw,
			postgresIngestBenchmarkMaxEntityCount,
		)
	}

	return int(value), nil
}

func parsePostgresIngestBenchmarkChangePercent(
	lookup func(string) (string, bool),
) (int, error) {
	raw, found := lookup(postgresIngestBenchmarkChangePercentEnv)
	if !found {
		return postgresIngestBenchmarkDefaultChangeRate, nil
	}

	trimmed := strings.TrimSpace(raw)
	value, err := strconv.ParseUint(trimmed, 10, 7)
	if err != nil || value < 1 || value > 100 {
		return 0, fmt.Errorf(
			"%s=%q must be an integer from 1 through 100",
			postgresIngestBenchmarkChangePercentEnv,
			raw,
		)
	}

	return int(value), nil
}

func parsePostgresIngestBenchmarkBucketCounts(
	lookup func(string) (string, bool),
) ([]int, error) {
	raw, found := lookup(postgresIngestBenchmarkBucketsEnv)
	if !found {
		return append([]int(nil), postgresIngestBenchmarkDefaultBuckets...), nil
	}

	entries := strings.Split(raw, ",")
	if len(entries) == 0 {
		return nil, fmt.Errorf("%s=%q must contain at least one bucket count", postgresIngestBenchmarkBucketsEnv, raw)
	}

	maxInt := uint64(^uint(0) >> 1)
	seen := make(map[int]struct{}, len(entries))
	result := make([]int, 0, len(entries))
	for _, entry := range entries {
		trimmed := strings.TrimSpace(entry)
		value, err := strconv.ParseUint(trimmed, 10, 64)
		if err != nil || value == 0 || value > ingestHashSpaceCount || value > maxInt {
			return nil, fmt.Errorf(
				"%s=%q contains invalid bucket count %q; each count must be a power of two from 1 through 2^32",
				postgresIngestBenchmarkBucketsEnv,
				raw,
				entry,
			)
		}
		bucketCount := int(value)
		if _, err := newIngestBucketSet(value); err != nil {
			return nil, fmt.Errorf(
				"%s=%q contains invalid bucket count %q: %w",
				postgresIngestBenchmarkBucketsEnv,
				raw,
				entry,
				err,
			)
		}
		if _, duplicate := seen[bucketCount]; duplicate {
			return nil, fmt.Errorf(
				"%s=%q repeats bucket count %d",
				postgresIngestBenchmarkBucketsEnv,
				raw,
				bucketCount,
			)
		}
		seen[bucketCount] = struct{}{}
		result = append(result, bucketCount)
	}

	return result, nil
}

func parsePostgresIngestBenchmarkClusterModes(
	lookup func(string) (string, bool),
) ([]bool, error) {
	raw, found := lookup(postgresIngestBenchmarkClusterEnv)
	if !found {
		return append([]bool(nil), postgresIngestBenchmarkDefaultCluster...), nil
	}

	entries := strings.Split(raw, ",")
	seen := make(map[bool]struct{}, len(entries))
	result := make([]bool, 0, len(entries))
	for _, entry := range entries {
		trimmed := strings.TrimSpace(entry)
		value, err := strconv.ParseBool(trimmed)
		if err != nil {
			return nil, fmt.Errorf(
				"%s=%q contains invalid boolean %q",
				postgresIngestBenchmarkClusterEnv,
				raw,
				entry,
			)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf(
				"%s=%q repeats cluster mode %t",
				postgresIngestBenchmarkClusterEnv,
				raw,
				value,
			)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf(
			"%s=%q must contain at least one boolean",
			postgresIngestBenchmarkClusterEnv,
			raw,
		)
	}

	return result, nil
}

func newPostgresIngestBenchmarkDataset(
	nodeCount int,
	edgeCount int,
	changePercent int,
) (postgresIngestBenchmarkDataset, error) {
	if nodeCount < 1 {
		return postgresIngestBenchmarkDataset{}, fmt.Errorf("PostgreSQL ingest benchmark node count must be positive")
	}
	if edgeCount < 1 {
		return postgresIngestBenchmarkDataset{}, fmt.Errorf("PostgreSQL ingest benchmark edge count must be positive")
	}
	if uint64(edgeCount) > uint64(nodeCount)*uint64(nodeCount) {
		return postgresIngestBenchmarkDataset{}, fmt.Errorf("PostgreSQL ingest benchmark edge count must not exceed nodes^2")
	}
	if changePercent < 1 || changePercent > 100 {
		return postgresIngestBenchmarkDataset{}, fmt.Errorf("PostgreSQL ingest benchmark change percent must be from 1 through 100")
	}

	return postgresIngestBenchmarkDataset{
		nodeCount:     nodeCount,
		edgeCount:     edgeCount,
		changedNodes:  postgresIngestBenchmarkChangedCount(nodeCount, changePercent),
		changedEdges:  postgresIngestBenchmarkChangedCount(edgeCount, changePercent),
		benchmarkNode: graph.StringKind("DAWGSIngestBenchmarkNode"),
		benchmarkEven: graph.StringKind("DAWGSIngestBenchmarkEvenNode"),
		benchmarkOdd:  graph.StringKind("DAWGSIngestBenchmarkOddNode"),
		benchmarkEdge: graph.StringKind("DAWGSIngestBenchmarkEdge"),
	}, nil
}

func postgresIngestBenchmarkChangedCount(total int, percentage int) int {
	count := int(uint64(total) * uint64(percentage) / 100)
	if count == 0 && total > 0 && percentage > 0 {
		return 1
	}

	return count
}

func postgresIngestBenchmarkSelected(index int, total int, selected int) bool {
	if selected >= total {
		return true
	}

	before := int64(index) * int64(selected) / int64(total)
	after := int64(index+1) * int64(selected) / int64(total)
	return after > before
}

func (s postgresIngestBenchmarkDataset) input(scenario postgresIngestBenchmarkScenario) IngestInput {
	return IngestInput{
		Nodes: s.nodeSequence(scenario),
		Edges: s.edgeSequence(scenario),
	}
}

func (s postgresIngestBenchmarkDataset) nodeSequence(
	scenario postgresIngestBenchmarkScenario,
) iter.Seq2[*IngestNode, error] {
	return func(yield func(*IngestNode, error) bool) {
		if !scenario.valid() {
			yield(nil, fmt.Errorf("unknown PostgreSQL ingest benchmark scenario %q", scenario))
			return
		}
		for index := range s.nodeCount {
			if scenario == ingestBenchmarkSparseChange &&
				!postgresIngestBenchmarkSelected(index, s.nodeCount, s.changedNodes) {
				continue
			}
			if !yield(s.node(index, scenario), nil) {
				return
			}
		}
	}
}

func (s postgresIngestBenchmarkDataset) edgeSequence(
	scenario postgresIngestBenchmarkScenario,
) iter.Seq2[*IngestEdge, error] {
	return func(yield func(*IngestEdge, error) bool) {
		if !scenario.valid() {
			yield(nil, fmt.Errorf("unknown PostgreSQL ingest benchmark scenario %q", scenario))
			return
		}
		for index := range s.edgeCount {
			if scenario == ingestBenchmarkSparseChange &&
				!postgresIngestBenchmarkSelected(index, s.edgeCount, s.changedEdges) {
				continue
			}
			if !yield(s.edge(index, scenario), nil) {
				return
			}
		}
	}
}

func (s postgresIngestBenchmarkDataset) node(
	index int,
	scenario postgresIngestBenchmarkScenario,
) *IngestNode {
	objectID := s.nodeObjectID(index)
	stable := fmt.Sprintf("node-stable-%03d", index%97)
	if scenario == ingestBenchmarkPartialMergeNoop {
		return &IngestNode{
			ObjectID: objectID,
			Kinds:    graph.Kinds{s.benchmarkNode},
			Properties: graph.AsProperties(map[string]any{
				"objectid": objectID,
				"stable":   stable,
			}),
		}
	}

	revision := int64(1)
	if (scenario == ingestBenchmarkDenseChange || scenario == ingestBenchmarkSparseChange) &&
		postgresIngestBenchmarkSelected(index, s.nodeCount, s.changedNodes) {
		revision = 2
	}
	secondaryKind := s.benchmarkOdd
	if index%2 == 0 {
		secondaryKind = s.benchmarkEven
	}

	return &IngestNode{
		ObjectID: objectID,
		Kinds:    graph.Kinds{s.benchmarkNode, secondaryKind},
		Properties: graph.AsProperties(map[string]any{
			"objectid": objectID,
			"ordinal":  int64(index),
			"stable":   stable,
			"revision": revision,
			"payload": map[string]any{
				"active": index%2 == 0,
				"group":  int64(index % 128),
			},
		}),
	}
}

func (s postgresIngestBenchmarkDataset) edge(
	index int,
	scenario postgresIngestBenchmarkScenario,
) *IngestEdge {
	startObjectID, endObjectID, lane := s.edgeIdentity(index)
	stable := fmt.Sprintf("edge-stable-%03d", index%89)
	properties := map[string]any{"stable": stable}
	if scenario != ingestBenchmarkPartialMergeNoop {
		revision := int64(1)
		if (scenario == ingestBenchmarkDenseChange || scenario == ingestBenchmarkSparseChange) &&
			postgresIngestBenchmarkSelected(index, s.edgeCount, s.changedEdges) {
			revision = 2
		}
		properties = map[string]any{
			"ordinal":  int64(index),
			"stable":   stable,
			"revision": revision,
			"payload": map[string]any{
				"lane":   int64(lane),
				"weight": int64(index % 31),
			},
		}
	}

	return &IngestEdge{
		StartObjectID: startObjectID,
		EndObjectID:   endObjectID,
		Kind:          s.benchmarkEdge,
		Properties:    graph.AsProperties(properties),
	}
}

func (s postgresIngestBenchmarkDataset) edgeIdentity(index int) (string, string, int) {
	lane := index / s.nodeCount
	startIndex := index % s.nodeCount
	endIndex := (startIndex + lane + 1) % s.nodeCount

	return s.nodeObjectID(startIndex), s.nodeObjectID(endIndex), lane
}

func (s postgresIngestBenchmarkDataset) nodeObjectID(index int) string {
	return fmt.Sprintf("dawgs-ingest-benchmark-node-%09d", index)
}

func (s postgresIngestBenchmarkDataset) inputCounts(
	scenario postgresIngestBenchmarkScenario,
) (int, int) {
	if scenario == ingestBenchmarkSparseChange {
		return s.changedNodes, s.changedEdges
	}

	return s.nodeCount, s.edgeCount
}

func (s postgresIngestBenchmarkDataset) expectedChangedCounts(
	scenario postgresIngestBenchmarkScenario,
) (int, int) {
	if scenario == ingestBenchmarkDenseChange || scenario == ingestBenchmarkSparseChange {
		return s.changedNodes, s.changedEdges
	}

	return 0, 0
}

func (s postgresIngestBenchmarkDataset) expectedLogicalRecords(
	scenario postgresIngestBenchmarkScenario,
) iter.Seq2[postgresIngestBenchmarkLogicalRecord, error] {
	return func(yield func(postgresIngestBenchmarkLogicalRecord, error) bool) {
		finalScenario := ingestBenchmarkFreshInsert
		switch scenario {
		case ingestBenchmarkFreshInsert,
			ingestBenchmarkDenseFullReplay,
			ingestBenchmarkPartialMergeNoop:
		case ingestBenchmarkDenseChange,
			ingestBenchmarkSparseChange:
			finalScenario = ingestBenchmarkDenseChange
		default:
			yield(postgresIngestBenchmarkLogicalRecord{}, fmt.Errorf(
				"unknown PostgreSQL ingest benchmark scenario %q",
				scenario,
			))
			return
		}

		for index := range s.nodeCount {
			node := s.node(index, finalScenario)
			if !yield(postgresIngestBenchmarkLogicalRecord{
				recordType: postgresIngestBenchmarkLogicalNode,
				objectID:   node.ObjectID,
				kinds:      node.Kinds.Strings(),
				properties: node.Properties,
			}, nil) {
				return
			}
		}
		for index := range s.edgeCount {
			edge := s.edge(index, finalScenario)
			if !yield(postgresIngestBenchmarkLogicalRecord{
				recordType:    postgresIngestBenchmarkLogicalEdge,
				startObjectID: edge.StartObjectID,
				edgeKind:      edge.Kind.String(),
				endObjectID:   edge.EndObjectID,
				properties:    edge.Properties,
			}, nil) {
				return
			}
		}
	}
}

func (s postgresIngestBenchmarkDataset) expectedBucketMetrics(
	scenario postgresIngestBenchmarkScenario,
	bucketCount int,
) (postgresIngestBenchmarkBucketMetrics, postgresIngestBenchmarkBucketMetrics, error) {
	if !scenario.valid() {
		return postgresIngestBenchmarkBucketMetrics{}, postgresIngestBenchmarkBucketMetrics{}, fmt.Errorf(
			"unknown PostgreSQL ingest benchmark scenario %q",
			scenario,
		)
	}
	buckets, err := newIngestBucketSet(uint64(bucketCount))
	if err != nil {
		return postgresIngestBenchmarkBucketMetrics{}, postgresIngestBenchmarkBucketMetrics{}, err
	}

	nodes := postgresIngestBenchmarkExpectedBucketMetrics(
		buckets,
		scenario,
		s.nodeCount,
		s.changedNodes,
		func(index int) uint32 {
			return hashIngestNodeIdentity(s.nodeObjectID(index))
		},
	)
	edges := postgresIngestBenchmarkExpectedBucketMetrics(
		buckets,
		scenario,
		s.edgeCount,
		s.changedEdges,
		func(index int) uint32 {
			startObjectID, endObjectID, _ := s.edgeIdentity(index)
			return hashIngestEdgeIdentity(startObjectID, s.benchmarkEdge.String(), endObjectID)
		},
	)

	return nodes, edges, nil
}

func postgresIngestBenchmarkExpectedBucketMetrics(
	buckets ingestBucketSet,
	scenario postgresIngestBenchmarkScenario,
	total int,
	changed int,
	identityHash func(int) uint32,
) postgresIngestBenchmarkBucketMetrics {
	populated := make(map[uint64]struct{})
	for index := range total {
		if scenario == ingestBenchmarkSparseChange &&
			!postgresIngestBenchmarkSelected(index, total, changed) {
			continue
		}
		populated[buckets.Bucket(identityHash(index))] = struct{}{}
	}

	metrics := postgresIngestBenchmarkBucketMetrics{PopulatedBuckets: int64(len(populated))}
	if scenario.requiresSeed() {
		for index := range total {
			if _, found := populated[buckets.Bucket(identityHash(index))]; found {
				metrics.IdentityRowsRead++
			}
		}
	}

	return metrics
}

func (s postgresIngestBenchmarkScenario) valid() bool {
	switch s {
	case ingestBenchmarkFreshInsert,
		ingestBenchmarkDenseFullReplay,
		ingestBenchmarkDenseChange,
		ingestBenchmarkPartialMergeNoop,
		ingestBenchmarkSparseChange:
		return true
	default:
		return false
	}
}

func (s postgresIngestBenchmarkScenario) requiresSeed() bool {
	return s != ingestBenchmarkFreshInsert
}

func runPostgresIngestBenchmarkSeed(
	path postgresIngestBenchmarkPath,
	runBatch func(postgresIngestBenchmarkScenario) error,
	runIngest func(postgresIngestBenchmarkScenario) (IngestStats, error),
) (IngestStats, error) {
	switch path {
	case postgresIngestBenchmarkBatchPath:
		if runBatch == nil {
			return IngestStats{}, fmt.Errorf("PostgreSQL ingest benchmark BatchOperation seed writer is nil")
		}
		if err := runBatch(ingestBenchmarkFreshInsert); err != nil {
			return IngestStats{}, err
		}
		return IngestStats{}, nil

	case postgresIngestBenchmarkIngestPath:
		if runIngest == nil {
			return IngestStats{}, fmt.Errorf("PostgreSQL ingest benchmark Driver.Ingest seed writer is nil")
		}
		return runIngest(ingestBenchmarkFreshInsert)

	default:
		return IngestStats{}, fmt.Errorf("unknown PostgreSQL ingest benchmark path %q", path)
	}
}

func vacuumAnalyzePostgresIngestBenchmarkPartitions(
	ctx context.Context,
	exec func(context.Context, string) error,
	nodePartition string,
	edgePartition string,
) error {
	if exec == nil {
		return fmt.Errorf("PostgreSQL ingest benchmark maintenance executor is nil")
	}

	for _, partition := range []struct {
		label string
		name  string
	}{
		{label: "node", name: nodePartition},
		{label: "edge", name: edgePartition},
	} {
		statement := "VACUUM (ANALYZE) " + pgx.Identifier{partition.name}.Sanitize() + ";"
		if err := exec(ctx, statement); err != nil {
			return fmt.Errorf(
				"VACUUM ANALYZE PostgreSQL ingest benchmark %s partition %q: %w",
				partition.label,
				partition.name,
				err,
			)
		}
	}

	return nil
}

func validatePostgresIngestBenchmarkLogicalRecords(
	expected iter.Seq2[postgresIngestBenchmarkLogicalRecord, error],
	actual iter.Seq2[postgresIngestBenchmarkLogicalRecord, error],
) (postgresIngestBenchmarkLogicalValidation, error) {
	if expected == nil || actual == nil {
		return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
			"PostgreSQL ingest benchmark logical record streams must be non-nil",
		)
	}

	nextExpected, stopExpected := iter.Pull2(expected)
	defer stopExpected()
	nextActual, stopActual := iter.Pull2(actual)
	defer stopActual()

	digest := sha256.New()
	if err := writeIngestBytes(digest, []byte("dawgs:pg-ingest:benchmark-logical-state:v1")); err != nil {
		return postgresIngestBenchmarkLogicalValidation{}, err
	}

	result := postgresIngestBenchmarkLogicalValidation{}
	for index := int64(0); ; index++ {
		expectedRecord, expectedErr, expectedOK := nextExpected()
		actualRecord, actualErr, actualOK := nextActual()
		if expectedErr != nil {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"build expected PostgreSQL ingest benchmark logical record %d: %w",
				index,
				expectedErr,
			)
		}
		if actualErr != nil {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"read actual PostgreSQL ingest benchmark logical record %d: %w",
				index,
				actualErr,
			)
		}
		if expectedOK != actualOK {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"PostgreSQL ingest benchmark logical record count mismatch at record %d: expected_present=%t actual_present=%t",
				index,
				expectedOK,
				actualOK,
			)
		}
		if !expectedOK {
			break
		}

		expectedBytes, err := encodePostgresIngestBenchmarkLogicalRecord(expectedRecord)
		if err != nil {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"encode expected PostgreSQL ingest benchmark logical record %d: %w",
				index,
				err,
			)
		}
		actualBytes, err := encodePostgresIngestBenchmarkLogicalRecord(actualRecord)
		if err != nil {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"encode actual PostgreSQL ingest benchmark logical record %d: %w",
				index,
				err,
			)
		}
		if !bytes.Equal(expectedBytes, actualBytes) {
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"PostgreSQL ingest benchmark logical record %d mismatch (expected_type=%d actual_type=%d)",
				index,
				expectedRecord.recordType,
				actualRecord.recordType,
			)
		}
		if err := writeLengthFramedIngestBytes(digest, expectedBytes); err != nil {
			return postgresIngestBenchmarkLogicalValidation{}, err
		}

		switch expectedRecord.recordType {
		case postgresIngestBenchmarkLogicalNode:
			result.Nodes++
		case postgresIngestBenchmarkLogicalEdge:
			result.Edges++
		default:
			return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
				"PostgreSQL ingest benchmark logical record %d has unknown type %d",
				index,
				expectedRecord.recordType,
			)
		}
	}

	if result.Nodes == 0 || result.Edges == 0 {
		return postgresIngestBenchmarkLogicalValidation{}, fmt.Errorf(
			"PostgreSQL ingest benchmark logical state must be non-vacuous for nodes and edges: nodes=%d edges=%d",
			result.Nodes,
			result.Edges,
		)
	}
	if err := writeIngestUint64(digest, uint64(result.Nodes)); err != nil {
		return postgresIngestBenchmarkLogicalValidation{}, err
	}
	if err := writeIngestUint64(digest, uint64(result.Edges)); err != nil {
		return postgresIngestBenchmarkLogicalValidation{}, err
	}
	copy(result.Checksum[:], digest.Sum(nil))

	return result, nil
}

func encodePostgresIngestBenchmarkLogicalRecord(
	record postgresIngestBenchmarkLogicalRecord,
) ([]byte, error) {
	var encoded bytes.Buffer
	switch record.recordType {
	case postgresIngestBenchmarkLogicalNode:
		if err := validateIngestString(record.objectID); err != nil {
			return nil, fmt.Errorf("logical node object ID: %w", err)
		}
		if err := writeIngestByte(&encoded, byte(record.recordType)); err != nil {
			return nil, err
		}
		if err := writeLengthFramedIngestBytes(&encoded, []byte(record.objectID)); err != nil {
			return nil, err
		}

		kindSet := make(map[string]struct{}, len(record.kinds))
		for _, kind := range record.kinds {
			if err := validateIngestString(kind); err != nil {
				return nil, fmt.Errorf("logical node kind: %w", err)
			}
			kindSet[kind] = struct{}{}
		}
		kindNames := make([]string, 0, len(kindSet))
		for kind := range kindSet {
			kindNames = append(kindNames, kind)
		}
		sort.Strings(kindNames)
		if err := writeIngestUint64(&encoded, uint64(len(kindNames))); err != nil {
			return nil, err
		}
		for _, kind := range kindNames {
			if err := writeLengthFramedIngestBytes(&encoded, []byte(kind)); err != nil {
				return nil, err
			}
		}

	case postgresIngestBenchmarkLogicalEdge:
		for label, value := range map[string]string{
			"start object ID": record.startObjectID,
			"edge kind":       record.edgeKind,
			"end object ID":   record.endObjectID,
		} {
			if err := validateIngestString(value); err != nil {
				return nil, fmt.Errorf("logical edge %s: %w", label, err)
			}
		}
		if err := writeIngestByte(&encoded, byte(record.recordType)); err != nil {
			return nil, err
		}
		for _, value := range []string{record.startObjectID, record.edgeKind, record.endObjectID} {
			if err := writeLengthFramedIngestBytes(&encoded, []byte(value)); err != nil {
				return nil, err
			}
		}

	default:
		return nil, fmt.Errorf("unknown logical record type %d", record.recordType)
	}

	properties, err := normalizeIngestProperties(record.properties)
	if err != nil {
		return nil, fmt.Errorf("logical record properties: %w", err)
	}
	if err := writeCanonicalIngestValue(&encoded, properties); err != nil {
		return nil, fmt.Errorf("logical record properties: %w", err)
	}

	return encoded.Bytes(), nil
}

func formatPostgresIngestBenchmarkLogicalNodes(graphTarget model.Graph) string {
	return strings.Join([]string{
		"select n.properties->>'objectid', ",
		"array(select k.name::text from unnest(n.kind_ids) as requested_kind(id) ",
		"join kind as k on k.id = requested_kind.id ",
		"order by convert_to(k.name::text, 'UTF8')), ",
		"n.properties::text from ",
		pgx.Identifier{graphTarget.Partitions.Node.Name}.Sanitize(),
		" as n ",
		"order by (n.properties->>'ordinal')::bigint, convert_to(n.properties->>'objectid', 'UTF8');",
	}, "")
}

func formatPostgresIngestBenchmarkLogicalEdges(graphTarget model.Graph) string {
	nodePartition := pgx.Identifier{graphTarget.Partitions.Node.Name}.Sanitize()
	return strings.Join([]string{
		"select start_node.properties->>'objectid', edge_kind.name::text, ",
		"end_node.properties->>'objectid', e.properties::text ",
		"from ",
		pgx.Identifier{graphTarget.Partitions.Edge.Name}.Sanitize(),
		" as e ",
		"join ", nodePartition, " as start_node on start_node.id = e.start_id ",
		"join ", nodePartition, " as end_node on end_node.id = e.end_id ",
		"join kind as edge_kind on edge_kind.id = e.kind_id ",
		"order by (e.properties->>'ordinal')::bigint, ",
		"convert_to(start_node.properties->>'objectid', 'UTF8'), ",
		"convert_to(edge_kind.name::text, 'UTF8'), ",
		"convert_to(end_node.properties->>'objectid', 'UTF8');",
	}, "")
}

func TestPostgresIngestBenchmarkConfigDefaults(t *testing.T) {
	config, err := loadPostgresIngestBenchmarkConfig(func(string) (string, bool) {
		return "", false
	})

	require.NoError(t, err)
	require.Equal(t, 100_000, config.NodeCount)
	require.Equal(t, 200_000, config.EdgeCount)
	require.Equal(t, 1, config.ChangePercent)
	require.Equal(t, []int{256, 4_096, 65_536}, config.BucketCounts)
	require.Equal(t, []bool{false, true}, config.ClusterModes)
}

func TestPostgresIngestBenchmarkConfigParsesExplicitValues(t *testing.T) {
	environment := map[string]string{
		"DAWGS_INGEST_BENCH_NODES":          " 8 ",
		"DAWGS_INGEST_BENCH_EDGES":          "12",
		"DAWGS_INGEST_BENCH_CHANGE_PERCENT": "25",
		"DAWGS_INGEST_BENCH_BUCKETS":        "1, 4,256",
		"DAWGS_INGEST_BENCH_CLUSTER":        "true, false",
	}

	config, err := loadPostgresIngestBenchmarkConfig(func(name string) (string, bool) {
		value, found := environment[name]
		return value, found
	})

	require.NoError(t, err)
	require.Equal(t, postgresIngestBenchmarkConfig{
		NodeCount:     8,
		EdgeCount:     12,
		ChangePercent: 25,
		BucketCounts:  []int{1, 4, 256},
		ClusterModes:  []bool{true, false},
	}, config)
}

func TestPostgresIngestBenchmarkConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name          string
		environment   map[string]string
		errorContains string
	}{
		{
			name:          "blank nodes",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_NODES": ""},
			errorContains: "DAWGS_INGEST_BENCH_NODES",
		},
		{
			name:          "zero nodes",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_NODES": "0"},
			errorContains: "DAWGS_INGEST_BENCH_NODES",
		},
		{
			name:          "nonnumeric edges",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_EDGES": "many"},
			errorContains: "DAWGS_INGEST_BENCH_EDGES",
		},
		{
			name: "too many unique edges",
			environment: map[string]string{
				"DAWGS_INGEST_BENCH_NODES": "2",
				"DAWGS_INGEST_BENCH_EDGES": "5",
			},
			errorContains: "DAWGS_INGEST_BENCH_EDGES",
		},
		{
			name:          "zero change percent",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_CHANGE_PERCENT": "0"},
			errorContains: "DAWGS_INGEST_BENCH_CHANGE_PERCENT",
		},
		{
			name:          "change percent over one hundred",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_CHANGE_PERCENT": "101"},
			errorContains: "DAWGS_INGEST_BENCH_CHANGE_PERCENT",
		},
		{
			name:          "non-power-of-two bucket",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_BUCKETS": "16,24"},
			errorContains: "DAWGS_INGEST_BENCH_BUCKETS",
		},
		{
			name:          "empty bucket entry",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_BUCKETS": "16,,256"},
			errorContains: "DAWGS_INGEST_BENCH_BUCKETS",
		},
		{
			name:          "duplicate bucket",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_BUCKETS": "16,16"},
			errorContains: "DAWGS_INGEST_BENCH_BUCKETS",
		},
		{
			name:          "invalid cluster mode",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_CLUSTER": "false,occasionally"},
			errorContains: "DAWGS_INGEST_BENCH_CLUSTER",
		},
		{
			name:          "duplicate cluster mode",
			environment:   map[string]string{"DAWGS_INGEST_BENCH_CLUSTER": "true,true"},
			errorContains: "DAWGS_INGEST_BENCH_CLUSTER",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadPostgresIngestBenchmarkConfig(func(name string) (string, bool) {
				value, found := test.environment[name]
				return value, found
			})

			require.ErrorContains(t, err, test.errorContains)
		})
	}
}

func TestPostgresIngestBenchmarkDatasetHasExactIdentities(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(4, 6, 25)
	require.NoError(t, err)

	nodes, edges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkFreshInsert))
	require.Len(t, nodes, 4)
	require.Len(t, edges, 6)
	require.Equal(t, []string{
		"dawgs-ingest-benchmark-node-000000000",
		"dawgs-ingest-benchmark-node-000000001",
		"dawgs-ingest-benchmark-node-000000002",
		"dawgs-ingest-benchmark-node-000000003",
	}, postgresIngestBenchmarkNodeIDs(nodes))
	require.Equal(t, []string{
		"dawgs-ingest-benchmark-node-000000000|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000001",
		"dawgs-ingest-benchmark-node-000000001|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000002",
		"dawgs-ingest-benchmark-node-000000002|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000003",
		"dawgs-ingest-benchmark-node-000000003|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000000",
		"dawgs-ingest-benchmark-node-000000000|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000002",
		"dawgs-ingest-benchmark-node-000000001|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000003",
	}, postgresIngestBenchmarkEdgeIDs(edges))
	require.EqualValues(t, 0, nodes[0].Properties.Map["ordinal"])
	require.EqualValues(t, 5, edges[5].Properties.Map["ordinal"])
	require.Equal(t, []string{"DAWGSIngestBenchmarkNode", "DAWGSIngestBenchmarkEvenNode"}, nodes[0].Kinds.Strings())
}

func TestPostgresIngestBenchmarkDenseChangeUsesExactPercentage(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(200, 400, 1)
	require.NoError(t, err)

	nodes, edges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkDenseChange))
	require.Len(t, nodes, 200)
	require.Len(t, edges, 400)
	require.Equal(t, 2, countPostgresIngestBenchmarkRevision(nodes, 2))
	require.Equal(t, 4, countPostgresIngestBenchmarkEdgeRevision(edges, 2))
}

func TestPostgresIngestBenchmarkDenseFullReplayIsExact(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)

	freshNodes, freshEdges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkFreshInsert))
	replayNodes, replayEdges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkDenseFullReplay))

	require.Equal(t, freshNodes, replayNodes)
	require.Equal(t, freshEdges, replayEdges)
}

func TestPostgresIngestBenchmarkPartialMergeIsLogicalNoop(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)

	fullNodes, fullEdges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkFreshInsert))
	partialNodes, partialEdges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkPartialMergeNoop))
	require.Len(t, partialNodes, len(fullNodes))
	require.Len(t, partialEdges, len(fullEdges))

	for index, partial := range partialNodes {
		full := fullNodes[index]
		require.Less(t, len(partial.Kinds), len(full.Kinds), full.ObjectID)
		require.Less(t, len(partial.Properties.Map), len(full.Properties.Map), full.ObjectID)

		merged := full.Properties.Clone()
		merged.Merge(partial.Properties)
		require.Equal(t, full.Properties.Map, merged.Map, full.ObjectID)
		require.Equal(t, full.Kinds, full.Kinds.Add(partial.Kinds...), full.ObjectID)
	}
	for index, partial := range partialEdges {
		full := fullEdges[index]
		require.Less(t, len(partial.Properties.Map), len(full.Properties.Map), postgresIngestBenchmarkEdgeID(full))

		merged := full.Properties.Clone()
		merged.Merge(partial.Properties)
		require.Equal(t, full.Properties.Map, merged.Map, postgresIngestBenchmarkEdgeID(full))
	}
}

func TestPostgresIngestBenchmarkSparseSelectionIsDeterministic(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)

	nodes, edges := collectPostgresIngestBenchmarkInput(t, dataset.input(ingestBenchmarkSparseChange))
	require.Equal(t, []string{
		"dawgs-ingest-benchmark-node-000000003",
		"dawgs-ingest-benchmark-node-000000007",
	}, postgresIngestBenchmarkNodeIDs(nodes))
	require.Equal(t, []string{
		"dawgs-ingest-benchmark-node-000000003|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000004",
		"dawgs-ingest-benchmark-node-000000007|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000000",
		"dawgs-ingest-benchmark-node-000000003|DAWGSIngestBenchmarkEdge|dawgs-ingest-benchmark-node-000000005",
	}, postgresIngestBenchmarkEdgeIDs(edges))
	require.Equal(t, len(nodes), countPostgresIngestBenchmarkRevision(nodes, 2))
	require.Equal(t, len(edges), countPostgresIngestBenchmarkEdgeRevision(edges, 2))
}

func TestPostgresIngestBenchmarkGraphEvictionRemovesOnlyRequestedCacheEntry(t *testing.T) {
	manager := NewSchemaManager(nil, 0)
	target := manager.defaultGraph
	target.ID = 41
	unrelated := manager.defaultGraph
	unrelated.ID = 42
	manager.graphs["target"] = target
	manager.graphs["unrelated"] = unrelated

	evictPostgresIngestBenchmarkGraph(manager, "target")

	require.NotContains(t, manager.graphs, "target")
	require.Equal(t, unrelated, manager.graphs["unrelated"])
	require.Len(t, manager.graphs, 1)
}

func TestPostgresIngestBenchmarkKindPrewarmAssertsAllKindsOnce(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)
	asserter := &recordingPostgresIngestBenchmarkKindAsserter{}

	err = assertPostgresIngestBenchmarkKinds(context.Background(), asserter, dataset)

	require.NoError(t, err)
	require.Equal(t, 1, asserter.calls)
	require.Equal(t, []string{
		"DAWGSIngestBenchmarkNode",
		"DAWGSIngestBenchmarkEvenNode",
		"DAWGSIngestBenchmarkOddNode",
		"DAWGSIngestBenchmarkEdge",
	}, asserter.kinds.Strings())
}

func TestPostgresIngestBenchmarkSeedUsesOnlyTheMeasuredWriterPath(t *testing.T) {
	wantStats := IngestStats{Nodes: IngestPhaseStats{InputRecords: 8}}
	for _, test := range []struct {
		name      string
		path      postgresIngestBenchmarkPath
		wantCalls []string
		wantStats IngestStats
	}{
		{
			name:      "BatchOperation path",
			path:      postgresIngestBenchmarkBatchPath,
			wantCalls: []string{"batch:fresh_insert"},
		},
		{
			name:      "Driver.Ingest path",
			path:      postgresIngestBenchmarkIngestPath,
			wantCalls: []string{"ingest:fresh_insert"},
			wantStats: wantStats,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls []string
			stats, err := runPostgresIngestBenchmarkSeed(
				test.path,
				func(scenario postgresIngestBenchmarkScenario) error {
					calls = append(calls, "batch:"+string(scenario))
					return nil
				},
				func(scenario postgresIngestBenchmarkScenario) (IngestStats, error) {
					calls = append(calls, "ingest:"+string(scenario))
					return wantStats, nil
				},
			)

			require.NoError(t, err)
			require.Equal(t, test.wantCalls, calls)
			require.Equal(t, test.wantStats, stats)
		})
	}
}

func TestPostgresIngestBenchmarkSeedRejectsUnknownPathWithoutCallingWriters(t *testing.T) {
	called := false
	_, err := runPostgresIngestBenchmarkSeed(
		postgresIngestBenchmarkPath("unknown"),
		func(postgresIngestBenchmarkScenario) error {
			called = true
			return nil
		},
		func(postgresIngestBenchmarkScenario) (IngestStats, error) {
			called = true
			return IngestStats{}, nil
		},
	)

	require.ErrorContains(t, err, "unknown PostgreSQL ingest benchmark path")
	require.False(t, called)
}

func TestPostgresIngestBenchmarkMaintenanceVacuumsExactQuotedPartitionsInOrder(t *testing.T) {
	var statements []string
	err := vacuumAnalyzePostgresIngestBenchmarkPartitions(
		context.Background(),
		func(_ context.Context, statement string) error {
			statements = append(statements, statement)
			return nil
		},
		`node"; select danger`,
		`edge"; select danger`,
	)

	require.NoError(t, err)
	require.Equal(t, []string{
		`VACUUM (ANALYZE) "node""; select danger";`,
		`VACUUM (ANALYZE) "edge""; select danger";`,
	}, statements)
}

func TestPostgresIngestBenchmarkMaintenanceStopsOnPartitionError(t *testing.T) {
	for _, test := range []struct {
		name          string
		failureCall   int
		wantCalls     int
		errorContains string
	}{
		{name: "node partition", failureCall: 1, wantCalls: 1, errorContains: "node"},
		{name: "edge partition", failureCall: 2, wantCalls: 2, errorContains: "edge"},
	} {
		t.Run(test.name, func(t *testing.T) {
			maintenanceErr := fmt.Errorf("maintenance failed")
			calls := 0
			err := vacuumAnalyzePostgresIngestBenchmarkPartitions(
				context.Background(),
				func(context.Context, string) error {
					calls++
					if calls == test.failureCall {
						return maintenanceErr
					}
					return nil
				},
				"node_target",
				"edge_target",
			)

			require.ErrorIs(t, err, maintenanceErr)
			require.ErrorContains(t, err, test.errorContains)
			require.Equal(t, test.wantCalls, calls)
		})
	}
}

func TestPostgresIngestBenchmarkExpectedBucketMetricsAreExact(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)

	tests := []struct {
		name        string
		scenario    postgresIngestBenchmarkScenario
		bucketCount int
		nodes       postgresIngestBenchmarkBucketMetrics
		edges       postgresIngestBenchmarkBucketMetrics
	}{
		{
			name:        "fresh four buckets",
			scenario:    ingestBenchmarkFreshInsert,
			bucketCount: 4,
			nodes:       postgresIngestBenchmarkBucketMetrics{PopulatedBuckets: 3},
			edges:       postgresIngestBenchmarkBucketMetrics{PopulatedBuckets: 4},
		},
		{
			name:        "dense replay four buckets",
			scenario:    ingestBenchmarkDenseFullReplay,
			bucketCount: 4,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 3,
				IdentityRowsRead: 8,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 4,
				IdentityRowsRead: 12,
			},
		},
		{
			name:        "dense changes four buckets",
			scenario:    ingestBenchmarkDenseChange,
			bucketCount: 4,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 3,
				IdentityRowsRead: 8,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 4,
				IdentityRowsRead: 12,
			},
		},
		{
			name:        "partial merge four buckets",
			scenario:    ingestBenchmarkPartialMergeNoop,
			bucketCount: 4,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 3,
				IdentityRowsRead: 8,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 4,
				IdentityRowsRead: 12,
			},
		},
		{
			name:        "sparse single bucket reads all stored identities",
			scenario:    ingestBenchmarkSparseChange,
			bucketCount: 1,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 1,
				IdentityRowsRead: 8,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 1,
				IdentityRowsRead: 12,
			},
		},
		{
			name:        "sparse four buckets includes range collisions",
			scenario:    ingestBenchmarkSparseChange,
			bucketCount: 4,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 2,
				IdentityRowsRead: 5,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 2,
				IdentityRowsRead: 7,
			},
		},
		{
			name:        "sparse 256 buckets isolates selected identities",
			scenario:    ingestBenchmarkSparseChange,
			bucketCount: 256,
			nodes: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 2,
				IdentityRowsRead: 2,
			},
			edges: postgresIngestBenchmarkBucketMetrics{
				PopulatedBuckets: 3,
				IdentityRowsRead: 3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodes, edges, err := dataset.expectedBucketMetrics(test.scenario, test.bucketCount)

			require.NoError(t, err)
			require.Equal(t, test.nodes, nodes)
			require.Equal(t, test.edges, edges)
		})
	}
}

func TestPostgresIngestBenchmarkExpectedBucketMetricsCoalesceHashAndRangeCollisions(t *testing.T) {
	buckets, err := newIngestBucketSet(4)
	require.NoError(t, err)
	hashes := []uint32{
		0x80000000,
		0x80000000,
		0x90000000,
		0x00000000,
		0x40000000,
	}

	metrics := postgresIngestBenchmarkExpectedBucketMetrics(
		buckets,
		ingestBenchmarkDenseFullReplay,
		len(hashes),
		1,
		func(index int) uint32 { return hashes[index] },
	)

	require.Equal(t, postgresIngestBenchmarkBucketMetrics{
		PopulatedBuckets: 3,
		IdentityRowsRead: 5,
	}, metrics)
}

func TestPostgresIngestBenchmarkLogicalRecordValidatorChecksExactCompleteState(t *testing.T) {
	expected := testPostgresIngestBenchmarkLogicalRecords()
	actual := testPostgresIngestBenchmarkLogicalRecords()

	result, err := validatePostgresIngestBenchmarkLogicalRecords(
		postgresIngestBenchmarkLogicalRecordSequence(expected),
		postgresIngestBenchmarkLogicalRecordSequence(actual),
	)

	require.NoError(t, err)
	require.Equal(t, int64(2), result.Nodes)
	require.Equal(t, int64(1), result.Edges)
	require.NotEqual(t, [32]byte{}, result.Checksum)
}

func TestPostgresIngestBenchmarkLogicalRecordValidatorRejectsAnyMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord
	}{
		{
			name: "wrong node identity",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[0].objectID = "node-wrong"
				return records
			},
		},
		{
			name: "wrong node kind set",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[0].kinds = []string{"BenchmarkNode", "WrongKind"}
				return records
			},
		},
		{
			name: "wrong directed edge endpoint",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[2].endObjectID = "node-a"
				return records
			},
		},
		{
			name: "wrong edge kind",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[2].edgeKind = "WrongEdge"
				return records
			},
		},
		{
			name: "wrong nested property",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[0].properties.Map["payload"].(map[string]any)["rank"] = int64(999)
				return records
			},
		},
		{
			name: "wrong nested array order",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[0].properties.Map["payload"].(map[string]any)["roles"] = []any{"reader", "writer"}
				return records
			},
		},
		{
			name: "wrong record order",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				records[0], records[1] = records[1], records[0]
				return records
			},
		},
		{
			name: "missing record",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				return records[:len(records)-1]
			},
		},
		{
			name: "extra record",
			mutate: func(records []postgresIngestBenchmarkLogicalRecord) []postgresIngestBenchmarkLogicalRecord {
				return append(records, postgresIngestBenchmarkLogicalRecord{
					recordType:    postgresIngestBenchmarkLogicalEdge,
					startObjectID: "node-b",
					edgeKind:      "BenchmarkEdge",
					endObjectID:   "node-a",
					properties:    graph.AsProperties(map[string]any{"weight": int64(2)}),
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := testPostgresIngestBenchmarkLogicalRecords()
			actual := test.mutate(testPostgresIngestBenchmarkLogicalRecords())

			_, err := validatePostgresIngestBenchmarkLogicalRecords(
				postgresIngestBenchmarkLogicalRecordSequence(expected),
				postgresIngestBenchmarkLogicalRecordSequence(actual),
			)

			require.Error(t, err)
		})
	}
}

func TestPostgresIngestBenchmarkLogicalRecordValidatorRejectsVacuousState(t *testing.T) {
	_, err := validatePostgresIngestBenchmarkLogicalRecords(
		postgresIngestBenchmarkLogicalRecordSequence(nil),
		postgresIngestBenchmarkLogicalRecordSequence(nil),
	)

	require.ErrorContains(t, err, "non-vacuous")
}

func TestPostgresIngestBenchmarkExpectedLogicalRecordsDescribeCompleteMergedState(t *testing.T) {
	dataset, err := newPostgresIngestBenchmarkDataset(8, 12, 25)
	require.NoError(t, err)

	for _, scenarios := range [][]postgresIngestBenchmarkScenario{
		{ingestBenchmarkFreshInsert, ingestBenchmarkDenseFullReplay, ingestBenchmarkPartialMergeNoop},
		{ingestBenchmarkDenseChange, ingestBenchmarkSparseChange},
	} {
		baseline := scenarios[0]
		for _, scenario := range scenarios[1:] {
			result, err := validatePostgresIngestBenchmarkLogicalRecords(
				dataset.expectedLogicalRecords(baseline),
				dataset.expectedLogicalRecords(scenario),
			)
			require.NoError(t, err, "%s versus %s", baseline, scenario)
			require.Equal(t, int64(8), result.Nodes)
			require.Equal(t, int64(12), result.Edges)
		}
	}

	_, err = validatePostgresIngestBenchmarkLogicalRecords(
		dataset.expectedLogicalRecords(ingestBenchmarkFreshInsert),
		dataset.expectedLogicalRecords(ingestBenchmarkDenseChange),
	)
	require.Error(t, err, "changed complete state must differ from the fresh seed")
}

func TestPostgresIngestBenchmarkLogicalQueriesStreamOnlyExactLogicalData(t *testing.T) {
	target := model.Graph{Partitions: model.GraphPartitions{
		Node: model.NewGraphPartition(`node"; danger`),
		Edge: model.NewGraphPartition(`edge"; danger`),
	}}

	require.Equal(t, strings.Join([]string{
		"select n.properties->>'objectid', ",
		"array(select k.name::text from unnest(n.kind_ids) as requested_kind(id) ",
		"join kind as k on k.id = requested_kind.id ",
		"order by convert_to(k.name::text, 'UTF8')), ",
		"n.properties::text from \"node\"\"; danger\" as n ",
		"order by (n.properties->>'ordinal')::bigint, convert_to(n.properties->>'objectid', 'UTF8');",
	}, ""), formatPostgresIngestBenchmarkLogicalNodes(target))
	require.Equal(t, strings.Join([]string{
		"select start_node.properties->>'objectid', edge_kind.name::text, ",
		"end_node.properties->>'objectid', e.properties::text ",
		"from \"edge\"\"; danger\" as e ",
		"join \"node\"\"; danger\" as start_node on start_node.id = e.start_id ",
		"join \"node\"\"; danger\" as end_node on end_node.id = e.end_id ",
		"join kind as edge_kind on edge_kind.id = e.kind_id ",
		"order by (e.properties->>'ordinal')::bigint, ",
		"convert_to(start_node.properties->>'objectid', 'UTF8'), ",
		"convert_to(edge_kind.name::text, 'UTF8'), ",
		"convert_to(end_node.properties->>'objectid', 'UTF8');",
	}, ""), formatPostgresIngestBenchmarkLogicalEdges(target))

	for _, statement := range []string{
		formatPostgresIngestBenchmarkLogicalNodes(target),
		formatPostgresIngestBenchmarkLogicalEdges(target),
	} {
		require.NotContains(t, statement, "content_hash")
		require.NotContains(t, statement, "id_hash")
		require.NotContains(t, statement, "start_object_id")
		require.NotContains(t, statement, "end_object_id")
	}
}

func testPostgresIngestBenchmarkLogicalRecords() []postgresIngestBenchmarkLogicalRecord {
	return []postgresIngestBenchmarkLogicalRecord{
		{
			recordType: postgresIngestBenchmarkLogicalNode,
			objectID:   "node-a",
			kinds:      []string{"BenchmarkNode", "BenchmarkEven"},
			properties: graph.AsProperties(map[string]any{
				"objectid": "node-a",
				"payload": map[string]any{
					"rank":  int64(1),
					"roles": []any{"writer", "reader"},
				},
			}),
		},
		{
			recordType: postgresIngestBenchmarkLogicalNode,
			objectID:   "node-b",
			kinds:      []string{"BenchmarkNode", "BenchmarkOdd"},
			properties: graph.AsProperties(map[string]any{
				"objectid": "node-b",
				"payload":  map[string]any{"rank": int64(2)},
			}),
		},
		{
			recordType:    postgresIngestBenchmarkLogicalEdge,
			startObjectID: "node-a",
			edgeKind:      "BenchmarkEdge",
			endObjectID:   "node-b",
			properties: graph.AsProperties(map[string]any{
				"weight":  int64(1),
				"payload": map[string]any{"active": true},
			}),
		},
	}
}

func postgresIngestBenchmarkLogicalRecordSequence(
	records []postgresIngestBenchmarkLogicalRecord,
) iter.Seq2[postgresIngestBenchmarkLogicalRecord, error] {
	return func(yield func(postgresIngestBenchmarkLogicalRecord, error) bool) {
		for _, record := range records {
			if !yield(record, nil) {
				return
			}
		}
	}
}

func collectPostgresIngestBenchmarkInput(
	t *testing.T,
	input IngestInput,
) ([]*IngestNode, []*IngestEdge) {
	t.Helper()

	var nodes []*IngestNode
	for node, err := range input.Nodes {
		require.NoError(t, err)
		nodes = append(nodes, node)
	}
	var edges []*IngestEdge
	for edge, err := range input.Edges {
		require.NoError(t, err)
		edges = append(edges, edge)
	}

	return nodes, edges
}

func postgresIngestBenchmarkNodeIDs(nodes []*IngestNode) []string {
	identities := make([]string, len(nodes))
	for index, node := range nodes {
		identities[index] = node.ObjectID
	}
	return identities
}

func postgresIngestBenchmarkEdgeIDs(edges []*IngestEdge) []string {
	identities := make([]string, len(edges))
	for index, edge := range edges {
		identities[index] = postgresIngestBenchmarkEdgeID(edge)
	}
	return identities
}

func postgresIngestBenchmarkEdgeID(edge *IngestEdge) string {
	return fmt.Sprintf("%s|%s|%s", edge.StartObjectID, edge.Kind.String(), edge.EndObjectID)
}

func countPostgresIngestBenchmarkRevision(nodes []*IngestNode, revision int64) int {
	count := 0
	for _, node := range nodes {
		if node.Properties.Map["revision"] == revision {
			count++
		}
	}
	return count
}

func countPostgresIngestBenchmarkEdgeRevision(edges []*IngestEdge, revision int64) int {
	count := 0
	for _, edge := range edges {
		if edge.Properties.Map["revision"] == revision {
			count++
		}
	}
	return count
}

var _ graph.Kind = graph.StringKind("")
