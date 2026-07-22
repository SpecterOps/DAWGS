package retriever

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

type loadNodeBatchTestDatabase struct {
	graph.Database
	batchSizes []int
	graphs     []string
	createdIDs [][]graph.ID
	failAt     int
}

func (s *loadNodeBatchTestDatabase) BatchOperation(ctx context.Context, delegate graph.BatchDelegate, _ ...graph.BatchOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return delegate(&loadNodeBatchTestBatch{database: s})
}

type loadNodeBatchTestBatch struct {
	graph.Batch
	database *loadNodeBatchTestDatabase
	graph    string
}

func (s *loadNodeBatchTestBatch) WithGraph(target graph.Graph) graph.Batch {
	s.graph = target.Name
	return s
}

func (s *loadNodeBatchTestBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	call := len(s.database.batchSizes) + 1
	s.database.batchSizes = append(s.database.batchSizes, len(nodes))
	s.database.graphs = append(s.database.graphs, s.graph)
	if s.database.failAt == call {
		return nil, errors.New("injected bulk node failure")
	}
	ids := append([]graph.ID(nil), s.database.createdIDs[call-1]...)
	return ids, nil
}

func TestRequireEmptyLoadTargets(t *testing.T) {
	ctx := context.Background()
	graphEntries := []GraphManifest{
		{Name: "empty"},
		{Name: "busy"},
	}

	if err := requireEmptyLoadTargetsWithCounter(ctx, nil, graphEntries[:1], func(context.Context, graph.Database, graph.Graph) (graphEntitySnapshot, error) {
		return graphEntitySnapshot{}, nil
	}); err != nil {
		t.Fatalf("require empty load targets: %v", err)
	}

	err := requireEmptyLoadTargetsWithCounter(ctx, nil, graphEntries, func(_ context.Context, _ graph.Database, target graph.Graph) (graphEntitySnapshot, error) {
		if target.Name == "busy" {
			return graphEntitySnapshot{
				NodeCount: 1,
				EdgeCount: 2,
			}, nil
		}

		return graphEntitySnapshot{}, nil
	})

	if err == nil || !strings.Contains(err.Error(), `target graph "busy" is not empty`) {
		t.Fatalf("expected non-empty graph error, got %v", err)
	}

	var nonEmptyErr NonEmptyTargetGraphError
	if !errors.As(err, &nonEmptyErr) {
		t.Fatalf("expected NonEmptyTargetGraphError, got %T: %v", err, err)
	}

	if nonEmptyErr.GraphName != "busy" || nonEmptyErr.NodeCount != 1 || nonEmptyErr.EdgeCount != 2 {
		t.Fatalf("unexpected non-empty graph error: %+v", nonEmptyErr)
	}

	expectedErr := errors.New("count failed")
	err = requireEmptyLoadTargetsWithCounter(ctx, nil, graphEntries[:1], func(context.Context, graph.Database, graph.Graph) (graphEntitySnapshot, error) {
		return graphEntitySnapshot{}, expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected counter error, got %v", err)
	}
}

func TestSchemaAssertionsFromManifest(t *testing.T) {
	value := newValidTestManifest(2)
	value.Graphs = []GraphManifest{
		{
			Name: "a",
		},
		{
			Name: "b",
		},
	}
	value.Schema.Graphs = []GraphSchemaMetadata{
		{
			Name:      "a",
			NodeKinds: []string{"User"},
			EdgeKinds: []string{"AdminTo"},
		},
		{
			Name:      "b",
			NodeKinds: []string{"Computer"},
			EdgeKinds: []string{"MemberOf"},
		},
	}

	assertions, err := schemaAssertionsFromManifest(value)
	if err != nil {
		t.Fatalf("schema assertions: %v", err)
	}

	if len(assertions) != 2 {
		t.Fatalf("assertion count = %d", len(assertions))
	}

	if assertions[0].Schema.DefaultGraph.Name != "a" || assertions[0].Schema.Graphs[0].Nodes[0].String() != "User" {
		t.Fatalf("unexpected first schema assertion: %+v", assertions[0])
	}

	value.Schema.Graphs = value.Schema.Graphs[:1]

	if _, err := schemaAssertionsFromManifest(value); err == nil {
		t.Fatalf("expected missing schema error")
	}
}

func TestResolveFragmentEdge(t *testing.T) {
	nodeMap := map[string]graph.ID{
		"1": 101,
		"2": 202,
	}
	item := FragmentEdge{
		StartID:    "1",
		EndID:      "2",
		Kind:       "AdminTo",
		Properties: map[string]any{"source": "test"},
	}

	resolved, err := resolveFragmentEdge(item, nodeMap)
	if err != nil {
		t.Fatalf("resolve edge: %v", err)
	}

	if resolved.StartID != 101 || resolved.EndID != 202 || resolved.Kind.String() != "AdminTo" {
		t.Fatalf("unexpected resolved edge: %+v", resolved)
	}

	if resolved.Properties.Get("source").Any() != "test" {
		t.Fatalf("unexpected resolved properties: %+v", resolved.Properties.Map)
	}

	item.StartID = "missing"

	if _, err := resolveFragmentEdge(item, nodeMap); err == nil {
		t.Fatalf("expected missing start node error")
	}

	item.StartID = "1"
	item.EndID = "missing"

	if _, err := resolveFragmentEdge(item, nodeMap); err == nil {
		t.Fatalf("expected missing end node error")
	}
}

func TestNodeIDResolverUsesNumericFastPathAndArbitraryFallback(t *testing.T) {
	resolver := newNodeIDResolver(4)
	for sourceID, destinationID := range map[string]graph.ID{
		"42":      100,
		"00042":   101,
		"node-a":  102,
		"-custom": 103,
	} {
		if !resolver.put(sourceID, destinationID) {
			t.Fatalf("put source ID %q", sourceID)
		}
	}

	if len(resolver.numeric) != 1 || len(resolver.fallback) != 3 {
		t.Fatalf("resolver storage numeric=%d fallback=%d", len(resolver.numeric), len(resolver.fallback))
	}
	for sourceID, expected := range map[string]graph.ID{"42": 100, "00042": 101, "node-a": 102, "-custom": 103} {
		if actual, found := resolver.resolve(sourceID); !found || actual != expected {
			t.Fatalf("resolve %q = %d, %t; want %d", sourceID, actual, found, expected)
		}
		if resolver.put(sourceID, 999) {
			t.Fatalf("duplicate source ID %q was accepted", sourceID)
		}
	}
	if _, found := resolver.resolve("missing"); found {
		t.Fatal("missing source ID resolved")
	}
}

func TestLoadGraphNodesUsesBoundedCorrelatedBatches(t *testing.T) {
	dir := t.TempDir()
	first := writeTestNodeFragment(t, dir, "nodes-1.gz", []FragmentNode{
		{ID: "1", Kinds: []string{"Node"}},
		{ID: "arbitrary", Kinds: []string{"Node"}},
		{ID: "3", Kinds: []string{"Node"}},
	})
	second := writeTestNodeFragment(t, dir, "nodes-2.gz", []FragmentNode{
		{ID: "4", Kinds: []string{"Node"}},
		{ID: "5", Kinds: []string{"Node"}},
	})
	graphEntry := GraphManifest{
		Name:      "selected",
		NodeCount: 5,
		Files:     []FileManifest{first, second},
	}
	database := &loadNodeBatchTestDatabase{createdIDs: [][]graph.ID{
		{300, 100},
		{900, 200},
		{700},
	}}

	resolver, count, err := loadGraphNodes(context.Background(), database, dir, CompressionGzip, graphEntry, 2, nil, 0)
	if err != nil {
		t.Fatalf("load graph nodes: %v", err)
	}
	if count != 5 || !reflect.DeepEqual(database.batchSizes, []int{2, 2, 1}) {
		t.Fatalf("count=%d batch sizes=%v", count, database.batchSizes)
	}
	if !reflect.DeepEqual(database.graphs, []string{"selected", "selected", "selected"}) {
		t.Fatalf("batch graph targets = %v", database.graphs)
	}
	for sourceID, expectedID := range map[string]graph.ID{
		"1": 300, "arbitrary": 100, "3": 900, "4": 200, "5": 700,
	} {
		if actual, found := resolver.resolve(sourceID); !found || actual != expectedID {
			t.Fatalf("resolver[%q] = %d, %t; want %d", sourceID, actual, found, expectedID)
		}
	}
}

func TestLoadGraphNodesStopsAtFailedAtomicBatch(t *testing.T) {
	dir := t.TempDir()
	file := writeTestNodeFragment(t, dir, "nodes.gz", []FragmentNode{{ID: "1"}, {ID: "2"}, {ID: "3"}})
	database := &loadNodeBatchTestDatabase{
		createdIDs: [][]graph.ID{{10, 20}, {30}},
		failAt:     2,
	}

	resolver, loaded, err := loadGraphNodes(context.Background(), database, dir, CompressionGzip, GraphManifest{
		Name: "default", NodeCount: 3, Files: []FileManifest{file},
	}, 2, nil, 0)
	if err == nil || !strings.Contains(err.Error(), "injected bulk node failure") {
		t.Fatalf("expected failed node batch, got resolver=%v loaded=%d err=%v", resolver, loaded, err)
	}
	if loaded != 2 || !reflect.DeepEqual(database.batchSizes, []int{2, 1}) {
		t.Fatalf("loaded=%d batch sizes=%v", loaded, database.batchSizes)
	}
}

func TestLoadGraphNodesPropagatesCancellationBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	file := writeTestNodeFragment(t, dir, "nodes.gz", []FragmentNode{{ID: "1"}})
	database := &loadNodeBatchTestDatabase{createdIDs: [][]graph.ID{{10}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resolver, loaded, err := loadGraphNodes(ctx, database, dir, CompressionGzip, GraphManifest{
		Name: "default", NodeCount: 1, Files: []FileManifest{file},
	}, 1, nil, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got resolver=%v loaded=%d err=%v", resolver, loaded, err)
	}
	if len(database.batchSizes) != 0 {
		t.Fatalf("canceled load reached node creator: %v", database.batchSizes)
	}
}

func TestVerifyCollectionFragmentsRejectsCrossFragmentDuplicateAndMissingEndpoint(t *testing.T) {
	t.Run("duplicate node", func(t *testing.T) {
		dir := t.TempDir()
		first := writeTestNodeFragment(t, dir, "nodes-1.gz", []FragmentNode{{ID: "1"}})
		second := writeTestNodeFragment(t, dir, "nodes-2.gz", []FragmentNode{{ID: "1"}})
		manifest := newValidTestManifest(1)
		manifest.Graphs = []GraphManifest{{Name: "default", NodeCount: 2, Files: []FileManifest{first, second}}}

		err := verifyCollectionFragments(dir, manifest)
		if err == nil || !strings.Contains(err.Error(), "duplicate source node ID") {
			t.Fatalf("expected duplicate source ID error, got %v", err)
		}
	})

	t.Run("missing endpoint", func(t *testing.T) {
		dir := t.TempDir()
		nodes := writeTestNodeFragment(t, dir, "nodes.gz", []FragmentNode{{ID: "1"}})
		edges := writeTestEdgeFragment(t, dir, "edges.gz", []FragmentEdge{{StartID: "1", EndID: "missing", Kind: "Edge"}})
		manifest := newValidTestManifest(1)
		manifest.Graphs = []GraphManifest{{Name: "default", NodeCount: 1, EdgeCount: 1, Files: []FileManifest{nodes, edges}}}

		err := verifyCollectionFragments(dir, manifest)
		if err == nil || !strings.Contains(err.Error(), `missing end node "missing"`) {
			t.Fatalf("expected missing endpoint error, got %v", err)
		}
	})
}

func writeTestNodeFragment(t *testing.T, dir, name string, nodes []FragmentNode) FileManifest {
	t.Helper()
	entry, err := writeCompressedJSONLines(filepath.Join(dir, name), CompressionGzip, DefaultZstdLevel, nodes)
	if err != nil {
		t.Fatalf("write node fragment: %v", err)
	}
	entry.Phase = PhaseNodes
	entry.Path = name
	return entry
}

func writeTestEdgeFragment(t *testing.T, dir, name string, edges []FragmentEdge) FileManifest {
	t.Helper()
	entry, err := writeCompressedJSONLines(filepath.Join(dir, name), CompressionGzip, DefaultZstdLevel, edges)
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}
	entry.Phase = PhaseEdges
	entry.Path = name
	return entry
}

func TestReadLoadManifestRejectsNeo4jMultiGraph(t *testing.T) {
	dir := t.TempDir()
	value := newValidTestManifest(2)
	value.Graphs = []GraphManifest{
		{Name: "a"},
		{Name: "b"},
	}
	if err := writeManifest(dir, value); err != nil {
		t.Fatalf("write Manifest: %v", err)
	}

	if _, err := readLoadManifest(dir, pg.DriverName); err != nil {
		t.Fatalf("read postgres load Manifest: %v", err)
	}

	if _, err := readLoadManifest(dir, neo4j.DriverName); err == nil {
		t.Fatalf("expected neo4j multi-graph load rejection")
	} else {
		var incompatibleErr IncompatibleDriverError

		if !errors.As(err, &incompatibleErr) {
			t.Fatalf("expected IncompatibleDriverError, got %T: %v", err, err)
		}

		if incompatibleErr.Driver != neo4j.DriverName || incompatibleErr.Operation != OperationLoad {
			t.Fatalf("unexpected incompatible driver error: %+v", incompatibleErr)
		}
	}
}

func TestVerifyManifestFilesRejectsBadChecksum(t *testing.T) {
	dir := t.TempDir()
	entry, err := writeCompressedJSONLines(filepath.Join(dir, "fragment.gz"), CompressionGzip, DefaultZstdLevel, []FragmentNode{{
		ID: "1",
	}})
	if err != nil {
		t.Fatalf("write fragment: %v", err)
	}

	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 1,
		Files: []FileManifest{{
			Phase:           PhaseNodes,
			Path:            "fragment.gz",
			Count:           1,
			CompressedBytes: entry.CompressedBytes,
			SHA256:          "bad",
		}},
	}}

	if err := verifyManifestFiles(dir, value); err == nil {
		t.Fatalf("expected checksum verification failure")
	} else {
		var checksumErr ChecksumMismatchError

		if !errors.As(err, &checksumErr) {
			t.Fatalf("expected ChecksumMismatchError, got %T: %v", err, err)
		}
	}

	value.Graphs[0].Files[0].SHA256 = entry.SHA256

	if err := verifyManifestFiles(dir, value); err != nil {
		t.Fatalf("verify Manifest files: %v", err)
	}
}

func TestReadFragmentFilesValidatePhaseAndCount(t *testing.T) {
	dir := t.TempDir()
	nodeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "nodes.gz"), CompressionGzip, DefaultZstdLevel, []FragmentNode{{
		ID: "1",
	}})
	if err != nil {
		t.Fatalf("write node fragment: %v", err)
	}

	nodeFile := FileManifest{
		Phase:           PhaseNodes,
		Path:            "nodes.gz",
		Count:           1,
		CompressedBytes: nodeEntry.CompressedBytes,
		SHA256:          nodeEntry.SHA256,
	}

	var nodes []FragmentNode
	if count, err := readNodeFragmentFile(dir, CompressionGzip, nodeFile, func(item FragmentNode) error {
		nodes = append(nodes, item)

		return nil
	}); err != nil {
		t.Fatalf("read node fragment: %v", err)
	} else if count != 1 || len(nodes) != 1 || nodes[0].ID != "1" {
		t.Fatalf("unexpected node records: %+v", nodes)
	}

	nodeFile.Count = 2

	if _, err := readNodeFragmentFile(dir, CompressionGzip, nodeFile, nil); err == nil {
		t.Fatalf("expected node count mismatch")
	}

	wrongPhaseEntry, err := writeCompressedJSONLines(filepath.Join(dir, "wrong-phase.gz"), CompressionGzip, DefaultZstdLevel, []FragmentEdge{{
		StartID: "1",
		EndID:   "2",
	}})
	if err != nil {
		t.Fatalf("write wrong Phase fragment: %v", err)
	}

	if _, err := readNodeFragmentFile(dir, CompressionGzip, FileManifest{
		Phase:           PhaseEdges,
		Path:            "wrong-phase.gz",
		Count:           1,
		CompressedBytes: wrongPhaseEntry.CompressedBytes,
		SHA256:          wrongPhaseEntry.SHA256,
	}, nil); err == nil {
		t.Fatalf("expected node Phase mismatch")
	} else if !strings.Contains(err.Error(), "phase") {
		t.Fatalf("expected node Phase mismatch, got %v", err)
	}

	edgeEntry, err := writeCompressedJSONLines(filepath.Join(dir, "edges.gz"), CompressionGzip, DefaultZstdLevel, []FragmentEdge{{
		StartID: "1",
		EndID:   "2",
		Kind:    "AdminTo",
	}})
	if err != nil {
		t.Fatalf("write edge fragment: %v", err)
	}

	edgeFile := FileManifest{
		Phase:           PhaseEdges,
		Path:            "edges.gz",
		Count:           1,
		CompressedBytes: edgeEntry.CompressedBytes,
		SHA256:          edgeEntry.SHA256,
	}
	var edges []FragmentEdge
	if count, err := readEdgeFragmentFile(dir, CompressionGzip, edgeFile, func(item FragmentEdge) error {
		edges = append(edges, item)

		return nil
	}); err != nil {
		t.Fatalf("read edge fragment: %v", err)
	} else if count != 1 || len(edges) != 1 || edges[0].Kind != "AdminTo" {
		t.Fatalf("unexpected edge records: %+v", edges)
	}

	edgeFile.Count = 2
	if _, err := readEdgeFragmentFile(dir, CompressionGzip, edgeFile, nil); err == nil {
		t.Fatalf("expected edge count mismatch")
	}
}

func TestVerifyLoadFragmentsRejectsMalformedJSONLines(t *testing.T) {
	dir := t.TempDir()
	fragmentPath := filepath.Join(dir, "nodes.jsonl.gz")
	writeCompressedPayload(t, fragmentPath, CompressionGzip, "{\"id\":\"1\",\"kinds\":[]}\n\n")

	contents, err := os.ReadFile(fragmentPath)
	if err != nil {
		t.Fatalf("read fragment: %v", err)
	}
	checksum := sha256.Sum256(contents)

	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 2,
		Files: []FileManifest{{
			Phase:           PhaseNodes,
			Path:            "nodes.jsonl.gz",
			Count:           2,
			CompressedBytes: int64(len(contents)),
			SHA256:          hex.EncodeToString(checksum[:]),
		}},
	}}

	if err := verifyLoadFragments(dir, value); err == nil || !strings.Contains(err.Error(), "blank line") {
		t.Fatalf("expected malformed JSONL preflight error, got %v", err)
	}
}

func TestVerifyLoadFragmentsPrioritizesIntegrityFailure(t *testing.T) {
	dir := t.TempDir()
	fragmentPath := filepath.Join(dir, "nodes.jsonl.gz")
	writeCompressedPayload(t, fragmentPath, CompressionGzip, "{\"id\":\"1\",\"kinds\":[]}\n\n")

	contents, err := os.ReadFile(fragmentPath)
	if err != nil {
		t.Fatalf("read fragment: %v", err)
	}

	value := newValidTestManifest(1)
	value.Graphs = []GraphManifest{{
		Name:      "default",
		NodeCount: 2,
		Files: []FileManifest{{
			Phase:           PhaseNodes,
			Path:            "nodes.jsonl.gz",
			Count:           2,
			CompressedBytes: int64(len(contents)),
			SHA256:          "bad",
		}},
	}}

	err = verifyLoadFragments(dir, value)
	var checksumErr ChecksumMismatchError
	if !errors.As(err, &checksumErr) {
		t.Fatalf("expected checksum error before malformed JSONL error, got %v", err)
	}
}

func TestReadFragmentFilesRejectEmptySourceIDs(t *testing.T) {
	dir := t.TempDir()

	if _, err := writeCompressedJSONLines(filepath.Join(dir, "node.jsonl.gz"), CompressionGzip, DefaultZstdLevel, []FragmentNode{{}}); err != nil {
		t.Fatalf("write node fragment: %v", err)
	}
	if _, err := readNodeFragmentFile(dir, CompressionGzip, FileManifest{
		Phase: PhaseNodes,
		Path:  "node.jsonl.gz",
		Count: 1,
	}, nil); err == nil || !strings.Contains(err.Error(), "empty source ID") {
		t.Fatalf("expected empty node source ID error, got %v", err)
	}

	for name, edge := range map[string]FragmentEdge{
		"start": {EndID: "2"},
		"end":   {StartID: "1"},
	} {
		t.Run(name, func(t *testing.T) {
			path := name + ".jsonl.gz"
			if _, err := writeCompressedJSONLines(filepath.Join(dir, path), CompressionGzip, DefaultZstdLevel, []FragmentEdge{edge}); err != nil {
				t.Fatalf("write edge fragment: %v", err)
			}

			if _, err := readEdgeFragmentFile(dir, CompressionGzip, FileManifest{
				Phase: PhaseEdges,
				Path:  path,
				Count: 1,
			}, nil); err == nil || !strings.Contains(err.Error(), "empty "+name+" source ID") {
				t.Fatalf("expected empty %s source ID error, got %v", name, err)
			}
		})
	}
}
