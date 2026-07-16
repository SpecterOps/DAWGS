package retriever

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	cypherModel "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func TestDumpCharacterizationJSONLContract(t *testing.T) {
	database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
		"graph/name": {
			nodes: []*graph.Node{
				graph.NewNode(3, graph.AsProperties(map[string]any{"note": "third"}), graph.StringKind("User")),
				graph.NewNode(1, graph.AsProperties(map[string]any{"enabled": true, "name": "alice"}), graph.StringKind("User"), graph.StringKind("Admin")),
				graph.NewNode(2, nil, graph.StringKind("Computer")),
			},
			relationships: []*graph.Relationship{
				graph.NewRelationship(11, 2, 3, nil, nil),
				graph.NewRelationship(10, 1, 2, graph.AsProperties(map[string]any{"route": "north"}), graph.StringKind("AdminTo")),
			},
		},
		"empty": {},
	})

	outputDir := t.TempDir()
	var progressMessages []string
	result, err := Dump(context.Background(), database, "scripted", []GraphTarget{
		{Name: "graph/name"},
		{Name: "empty"},
	}, DumpOptions{
		OutputDir:   outputDir,
		Scrub:       ScrubNone,
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   2,
		BatchSize:   3,
		Progress: func(event ProgressEvent) {
			progressMessages = append(progressMessages, event.Message)
		},
	})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	if result.NodeCount != 3 || result.EdgeCount != 2 {
		t.Fatalf("dump counts: nodes=%d edges=%d", result.NodeCount, result.EdgeCount)
	}

	manifest := result.Manifest
	if _, offset := manifest.GeneratedAt.Zone(); offset != 0 {
		t.Fatalf("manifest generated_at is not UTC: %s", manifest.GeneratedAt)
	}
	if manifest.Format != manifestFormat || manifest.Driver != "scripted" || manifest.Source.GraphCount != 2 || manifest.IDStrategy != idStrategy {
		t.Fatalf("manifest identity changed: %+v", manifest)
	}
	if manifest.Compression != CompressionGzip || manifest.CompressionLevel != DefaultZstdLevel {
		t.Fatalf("manifest compression changed: %+v", manifest)
	}
	if !reflect.DeepEqual(manifest.Scrub, ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}) {
		t.Fatalf("manifest scrub metadata changed: %+v", manifest.Scrub)
	}

	expectedGraphs := []GraphManifest{
		{
			Name:             "graph/name",
			NodeCount:        3,
			EdgeCount:        2,
			NodeActionCounts: map[string]int{},
			EdgeActionCounts: map[string]int{},
			Files: []FileManifest{
				{Phase: PhaseNodes, Path: "graphs/graph%2Fname/nodes-000001.jsonl.gz", Count: 2, CompressedBytes: 115, UncompressedBytes: 113, SHA256: "6d123f3d464e1737cff426cbe890972d1ec8abaecbd15a2739c203ffa43b97a4", ActionCounts: map[string]int{}},
				{Phase: PhaseNodes, Path: "graphs/graph%2Fname/nodes-000002.jsonl.gz", Count: 1, CompressedBytes: 81, UncompressedBytes: 58, SHA256: "a5976778349131421837eb8a69aab6e1efd75d607d50bc5f233ac4107809f739", ActionCounts: map[string]int{}},
				{Phase: PhaseEdges, Path: "graphs/graph%2Fname/edges-000001.jsonl.gz", Count: 2, CompressedBytes: 107, UncompressedBytes: 118, SHA256: "748baddea892c36540aadf75f6412b57bd1e7e4dfe961953f8dcdacf2d0f685d", ActionCounts: map[string]int{}},
			},
		},
		{
			Name:             "empty",
			NodeCount:        0,
			EdgeCount:        0,
			NodeActionCounts: map[string]int{},
			EdgeActionCounts: map[string]int{},
		},
	}
	if !reflect.DeepEqual(manifest.Graphs, expectedGraphs) {
		t.Fatalf("manifest graph contract changed:\nactual:   %#v\nexpected: %#v", manifest.Graphs, expectedGraphs)
	}

	expectedSchema := SchemaMetadata{Graphs: []GraphSchemaMetadata{
		{Name: "graph/name", NodeKinds: []string{"Admin", "Computer", "User"}, EdgeKinds: []string{"AdminTo"}},
		{Name: "empty", NodeKinds: []string{}, EdgeKinds: []string{}},
	}}
	if !reflect.DeepEqual(manifest.Schema, expectedSchema) {
		t.Fatalf("manifest schema contract changed: actual=%+v expected=%+v", manifest.Schema, expectedSchema)
	}

	if manifest.Metrics == nil || len(manifest.Metrics.Graphs) != 2 {
		t.Fatalf("manifest metrics contract changed: %+v", manifest.Metrics)
	}
	if manifest.Metrics.Version != metricsVersion {
		t.Fatalf("metrics version = %q", manifest.Metrics.Version)
	}
	expectedMetrics := []GraphMetrics{
		{
			Name:                 "graph/name",
			NodeCount:            3,
			EdgeCount:            2,
			NodeKindHistogram:    map[string]int64{"4:User": 1, "5:Admin+4:User": 1, "8:Computer": 1},
			EdgeKindHistogram:    map[string]int64{"7:AdminTo": 1, metricsNoneKind: 1},
			InDegreeHistogram:    map[string]int64{"0": 1, "1": 2},
			OutDegreeHistogram:   map[string]int64{"0": 1, "1": 2},
			TotalDegreeHistogram: map[string]int64{"1": 2, "2": 1},
			EndpointKindHistogram: map[string]int64{
				"10:8:Computer|2:0:|6:4:User":                 1,
				"14:5:Admin+4:User|9:7:AdminTo|10:8:Computer": 1,
			},
			Fingerprint: "sha256:4a4a992c9c605b220356b38e042929dd69a671e7b5cd6da17f91ee355e6a1dbc",
		},
		{
			Name:                  "empty",
			NodeKindHistogram:     map[string]int64{},
			EdgeKindHistogram:     map[string]int64{},
			InDegreeHistogram:     map[string]int64{},
			OutDegreeHistogram:    map[string]int64{},
			TotalDegreeHistogram:  map[string]int64{},
			EndpointKindHistogram: map[string]int64{},
			Fingerprint:           "sha256:3fffdda8af847c800a59c98edd6655fc37a8dd70cb1b4b74731cbd283c1af3eb",
		},
	}
	if !reflect.DeepEqual(manifest.Metrics.Graphs, expectedMetrics) {
		t.Fatalf("metrics contract changed:\nactual:   %#v\nexpected: %#v", manifest.Metrics.Graphs, expectedMetrics)
	}

	assertFragmentNodes(t, outputDir, manifest.Graphs[0].Files[0], []FragmentNode{
		{ID: "1", Kinds: []string{"Admin", "User"}, Properties: map[string]any{"enabled": true, "name": "alice"}},
		{ID: "2", Kinds: []string{"Computer"}},
	})
	assertFragmentNodes(t, outputDir, manifest.Graphs[0].Files[1], []FragmentNode{
		{ID: "3", Kinds: []string{"User"}, Properties: map[string]any{"note": "third"}},
	})
	assertFragmentEdges(t, outputDir, manifest.Graphs[0].Files[2], []FragmentEdge{
		{StartID: "1", EndID: "2", Kind: "AdminTo", Properties: map[string]any{"route": "north"}},
		{StartID: "2", EndID: "3", Kind: ""},
	})

	writtenManifest, err := readManifest(outputDir)
	if err != nil {
		t.Fatalf("read written manifest: %v", err)
	}
	if !reflect.DeepEqual(writtenManifest, result.Manifest) {
		t.Fatalf("returned and written manifests differ")
	}

	expectedProgressMessages := []string{
		"retriever dump started",
		"retriever dump output directory ready",
		"retriever dump graph started",
		"retriever dump graph counts ready",
		"retriever dump node phase started",
		"retriever dump node phase completed",
		"retriever dump edge phase started",
		"retriever dump edge phase completed",
		"retriever dump graph completed",
		"retriever dump graph started",
		"retriever dump graph counts ready",
		"retriever dump node phase started",
		"retriever dump node phase completed",
		"retriever dump edge phase started",
		"retriever dump edge phase completed",
		"retriever dump graph completed",
		"retriever dump completed",
	}
	if !reflect.DeepEqual(progressMessages, expectedProgressMessages) {
		t.Fatalf("progress event order changed: actual=%v expected=%v", progressMessages, expectedProgressMessages)
	}
}

func TestDumpCharacterizationScrubRegistryGraphOrdering(t *testing.T) {
	const sourceSID = "S-1-5-21-111111111-222222222-333333333-1001"
	database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
		"first": {
			nodes: []*graph.Node{
				graph.NewNode(1, graph.AsProperties(map[string]any{
					"description": "private description",
					"owner_sid":   sourceSID,
				}), graph.StringKind("User")),
			},
		},
		"second": {
			nodes: []*graph.Node{
				graph.NewNode(2, graph.AsProperties(map[string]any{
					"objectid": sourceSID,
				}), graph.StringKind("User")),
			},
		},
	})

	outputDir := t.TempDir()
	result, err := Dump(context.Background(), database, "scripted", []GraphTarget{
		{Name: "first"},
		{Name: "second"},
	}, DumpOptions{
		OutputDir:   outputDir,
		Scrub:       ScrubFull,
		Salt:        "characterization-salt",
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   1,
		BatchSize:   1,
	})
	if err != nil {
		t.Fatalf("dump: %v", err)
	}

	expectedOperations := []string{
		"first:nodes:count",
		"first:edges:count",
		"first:nodes:fetch",
		"first:nodes:fetch",
		"second:nodes:count",
		"second:edges:count",
		"second:nodes:fetch",
		"second:nodes:fetch",
	}
	if !reflect.DeepEqual(database.operations, expectedOperations) {
		t.Fatalf("scrub graph operation order changed: actual=%v expected=%v", database.operations, expectedOperations)
	}

	if !reflect.DeepEqual(result.Manifest.Scrub.NodeActionCounts, map[string]int{
		string(actionPseudonymize): 2,
		string(actionRedact):       1,
	}) {
		t.Fatalf("collection scrub counts changed: %+v", result.Manifest.Scrub.NodeActionCounts)
	}
	if !reflect.DeepEqual(result.Manifest.Graphs[0].NodeActionCounts, map[string]int{
		string(actionPseudonymize): 1,
		string(actionRedact):       1,
	}) {
		t.Fatalf("first graph scrub counts changed: %+v", result.Manifest.Graphs[0].NodeActionCounts)
	}
	if !reflect.DeepEqual(result.Manifest.Graphs[0].Files[0].ActionCounts, result.Manifest.Graphs[0].NodeActionCounts) {
		t.Fatalf("first shard scrub counts changed: %+v", result.Manifest.Graphs[0].Files[0].ActionCounts)
	}

	var firstRows []FragmentNode
	readFragmentNodes(t, outputDir, result.Manifest.Graphs[0].Files[0], &firstRows)
	if len(firstRows) != 1 {
		t.Fatalf("first graph row count = %d", len(firstRows))
	}
	if firstRows[0].Properties["description"] != "[REDACTED]" {
		t.Fatalf("description scrub output changed: %#v", firstRows[0].Properties["description"])
	}
	if firstRows[0].Properties["owner_sid"] == sourceSID {
		t.Fatalf("owner_sid was not scrubbed: %#v", firstRows[0].Properties["owner_sid"])
	}

	var secondRows []FragmentNode
	readFragmentNodes(t, outputDir, result.Manifest.Graphs[1].Files[0], &secondRows)
	if len(secondRows) != 1 {
		t.Fatalf("second graph row count = %d", len(secondRows))
	}
	if secondRows[0].Properties["objectid"] != firstRows[0].Properties["owner_sid"] {
		t.Fatalf("shared scrub registry identity changed: first=%#v second=%#v", firstRows[0].Properties["owner_sid"], secondRows[0].Properties["objectid"])
	}
}

func TestDumpCharacterizationCountDrift(t *testing.T) {
	t.Run("short scan commits fragment then reports mismatch", func(t *testing.T) {
		database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
			"default": {
				nodeCount: 3,
				nodes: []*graph.Node{
					graph.NewNode(1, nil, graph.StringKind("User")),
					graph.NewNode(2, nil, graph.StringKind("User")),
				},
			},
		})
		outputDir := t.TempDir()

		_, err := Dump(context.Background(), database, "scripted", []GraphTarget{{Name: "default"}}, characterizationDumpOptions(outputDir))
		var mismatch EntityCountMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("expected entity count mismatch, got %v", err)
		}
		if mismatch.Phase != PhaseNodes || mismatch.Expected != 3 || mismatch.Actual != 2 {
			t.Fatalf("count mismatch contract changed: %+v", mismatch)
		}
		assertPathExists(t, filepath.Join(outputDir, "graphs", "default", "nodes-000001.jsonl.gz"))
		assertPathDoesNotExist(t, filepath.Join(outputDir, manifestFileName))
		assertNoTemporaryArtifacts(t, outputDir)
	})

	t.Run("growth beyond inventory is capped and succeeds", func(t *testing.T) {
		database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
			"default": {
				nodeCount:       2,
				ignoreNodeLimit: true,
				nodes: []*graph.Node{
					graph.NewNode(1, nil, graph.StringKind("User")),
					graph.NewNode(2, nil, graph.StringKind("User")),
					graph.NewNode(3, nil, graph.StringKind("User")),
				},
			},
		})
		outputDir := t.TempDir()

		result, err := Dump(context.Background(), database, "scripted", []GraphTarget{{Name: "default"}}, characterizationDumpOptions(outputDir))
		if err != nil {
			t.Fatalf("dump: %v", err)
		}
		if result.NodeCount != 2 || result.Manifest.Graphs[0].NodeCount != 2 {
			t.Fatalf("growth cap contract changed: result=%+v", result)
		}
		assertFragmentNodes(t, outputDir, result.Manifest.Graphs[0].Files[0], []FragmentNode{
			{ID: "1", Kinds: []string{"User"}},
			{ID: "2", Kinds: []string{"User"}},
		})
	})
}

func TestDumpCharacterizationFailurePublication(t *testing.T) {
	t.Run("source failure retains completed fragments", func(t *testing.T) {
		sourceErr := errors.New("scripted relationship read failure")
		database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
			"default": {
				nodes: []*graph.Node{
					graph.NewNode(1, nil, graph.StringKind("User")),
					graph.NewNode(2, nil, graph.StringKind("User")),
				},
				relationships: []*graph.Relationship{
					graph.NewRelationship(10, 1, 2, nil, graph.StringKind("AdminTo")),
				},
				relationshipFetchErr: sourceErr,
			},
		})
		outputDir := t.TempDir()

		_, err := Dump(context.Background(), database, "scripted", []GraphTarget{{Name: "default"}}, characterizationDumpOptions(outputDir))
		if !errors.Is(err, sourceErr) {
			t.Fatalf("expected relationship read error, got %v", err)
		}

		assertPathExists(t, filepath.Join(outputDir, "graphs", "default", "nodes-000001.jsonl.gz"))
		assertPathDoesNotExist(t, filepath.Join(outputDir, "graphs", "default", "edges-000001.jsonl.gz"))
		assertPathDoesNotExist(t, filepath.Join(outputDir, manifestFileName))
		assertNoTemporaryArtifacts(t, outputDir)
	})

	t.Run("encoding failure aborts active fragment", func(t *testing.T) {
		database := newScriptedDumpDatabase(map[string]*scriptedDumpGraph{
			"default": {
				nodes: []*graph.Node{
					graph.NewNode(1, graph.AsProperties(map[string]any{
						"unsupported": func() {},
					}), graph.StringKind("User")),
				},
			},
		})
		outputDir := t.TempDir()

		_, err := Dump(context.Background(), database, "scripted", []GraphTarget{{Name: "default"}}, characterizationDumpOptions(outputDir))
		if err == nil || !strings.Contains(err.Error(), "encode JSONL record 1") {
			t.Fatalf("expected JSONL encoding error, got %v", err)
		}

		assertPathDoesNotExist(t, filepath.Join(outputDir, "graphs", "default", "nodes-000001.jsonl.gz"))
		assertPathDoesNotExist(t, filepath.Join(outputDir, manifestFileName))
		assertNoTemporaryArtifacts(t, outputDir)
	})
}

func characterizationDumpOptions(outputDir string) DumpOptions {
	return DumpOptions{
		OutputDir:   outputDir,
		Scrub:       ScrubNone,
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   10,
		BatchSize:   10,
	}
}

func assertFragmentNodes(t *testing.T, outputDir string, fileEntry FileManifest, expected []FragmentNode) {
	t.Helper()
	var actual []FragmentNode
	readFragmentNodes(t, outputDir, fileEntry, &actual)
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("node fragment %q changed:\nactual:   %#v\nexpected: %#v", fileEntry.Path, actual, expected)
	}
}

func readFragmentNodes(t *testing.T, outputDir string, fileEntry FileManifest, target *[]FragmentNode) {
	t.Helper()
	count, err := readCompressedJSONLines(filepath.Join(outputDir, filepath.FromSlash(fileEntry.Path)), CompressionGzip, func(row FragmentNode) error {
		*target = append(*target, row)
		return nil
	})
	if err != nil {
		t.Fatalf("read node fragment %q: %v", fileEntry.Path, err)
	}
	if count != fileEntry.Count {
		t.Fatalf("node fragment %q count=%d manifest=%d", fileEntry.Path, count, fileEntry.Count)
	}
}

func assertFragmentEdges(t *testing.T, outputDir string, fileEntry FileManifest, expected []FragmentEdge) {
	t.Helper()
	var actual []FragmentEdge
	count, err := readCompressedJSONLines(filepath.Join(outputDir, filepath.FromSlash(fileEntry.Path)), CompressionGzip, func(row FragmentEdge) error {
		actual = append(actual, row)
		return nil
	})
	if err != nil {
		t.Fatalf("read edge fragment %q: %v", fileEntry.Path, err)
	}
	if count != fileEntry.Count {
		t.Fatalf("edge fragment %q count=%d manifest=%d", fileEntry.Path, count, fileEntry.Count)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("edge fragment %q changed:\nactual:   %#v\nexpected: %#v", fileEntry.Path, actual, expected)
	}
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected path %q: %v", path, err)
	}
}

func assertPathDoesNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected path %q to be absent, got %v", path, err)
	}
}

func assertNoTemporaryArtifacts(t *testing.T, root string) {
	t.Helper()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tmp") {
			return fmt.Errorf("temporary artifact remains at %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type scriptedDumpGraph struct {
	nodeCount               int64
	relationshipCount       int64
	nodes                   []*graph.Node
	relationships           []*graph.Relationship
	ignoreNodeLimit         bool
	ignoreRelationshipLimit bool
	nodeFetchErr            error
	relationshipFetchErr    error
}

type scriptedDumpDatabase struct {
	graph.Database
	graphs     map[string]*scriptedDumpGraph
	operations []string
}

func newScriptedDumpDatabase(graphs map[string]*scriptedDumpGraph) *scriptedDumpDatabase {
	for _, graphFixture := range graphs {
		if graphFixture.nodeCount == 0 {
			graphFixture.nodeCount = int64(len(graphFixture.nodes))
		}
		if graphFixture.relationshipCount == 0 {
			graphFixture.relationshipCount = int64(len(graphFixture.relationships))
		}
	}
	return &scriptedDumpDatabase{graphs: graphs}
}

func (s *scriptedDumpDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&scriptedDumpTransaction{database: s})
}

type scriptedDumpTransaction struct {
	graph.Transaction
	database  *scriptedDumpDatabase
	graphName string
}

func (s *scriptedDumpTransaction) WithGraph(target graph.Graph) graph.Transaction {
	return &scriptedDumpTransaction{database: s.database, graphName: target.Name}
}

func (s *scriptedDumpTransaction) Nodes() graph.NodeQuery {
	return &scriptedDumpNodeQuery{database: s.database, graphName: s.graphName}
}

func (s *scriptedDumpTransaction) Relationships() graph.RelationshipQuery {
	return &scriptedDumpRelationshipQuery{database: s.database, graphName: s.graphName}
}

type scriptedDumpNodeQuery struct {
	graph.NodeQuery
	database  *scriptedDumpDatabase
	graphName string
	limit     int
	afterID   graph.ID
	hasAfter  bool
}

func (s *scriptedDumpNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery {
	return s
}

func (s *scriptedDumpNodeQuery) Limit(limit int) graph.NodeQuery {
	s.limit = limit
	return s
}

func (s *scriptedDumpNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.afterID = scriptedDumpAfterID(criteria)
	s.hasAfter = true
	return s
}

func (s *scriptedDumpNodeQuery) Count() (int64, error) {
	fixture := s.database.graphFixture(s.graphName)
	s.database.recordOperation(s.graphName, PhaseNodes, "count")
	return fixture.nodeCount, nil
}

func (s *scriptedDumpNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	fixture := s.database.graphFixture(s.graphName)
	s.database.recordOperation(s.graphName, PhaseNodes, "fetch")
	if fixture.nodeFetchErr != nil {
		return fixture.nodeFetchErr
	}

	rows := append([]*graph.Node(nil), fixture.nodes...)
	sort.Slice(rows, func(left, right int) bool { return rows[left].ID < rows[right].ID })
	rows = filterScriptedDumpRows(rows, s.afterID, s.hasAfter, func(node *graph.Node) graph.ID { return node.ID })
	if !fixture.ignoreNodeLimit && s.limit >= 0 && len(rows) > s.limit {
		rows = rows[:s.limit]
	}
	return delegate(newScriptedDumpCursor(rows, nil))
}

type scriptedDumpRelationshipQuery struct {
	graph.RelationshipQuery
	database  *scriptedDumpDatabase
	graphName string
	limit     int
	afterID   graph.ID
	hasAfter  bool
}

func (s *scriptedDumpRelationshipQuery) OrderBy(...graph.Criteria) graph.RelationshipQuery {
	return s
}

func (s *scriptedDumpRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	s.limit = limit
	return s
}

func (s *scriptedDumpRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	s.afterID = scriptedDumpAfterID(criteria)
	s.hasAfter = true
	return s
}

func (s *scriptedDumpRelationshipQuery) Count() (int64, error) {
	fixture := s.database.graphFixture(s.graphName)
	s.database.recordOperation(s.graphName, PhaseEdges, "count")
	return fixture.relationshipCount, nil
}

func (s *scriptedDumpRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	fixture := s.database.graphFixture(s.graphName)
	s.database.recordOperation(s.graphName, PhaseEdges, "fetch")
	if fixture.relationshipFetchErr != nil {
		return fixture.relationshipFetchErr
	}

	rows := append([]*graph.Relationship(nil), fixture.relationships...)
	sort.Slice(rows, func(left, right int) bool { return rows[left].ID < rows[right].ID })
	rows = filterScriptedDumpRows(rows, s.afterID, s.hasAfter, func(relationship *graph.Relationship) graph.ID { return relationship.ID })
	if !fixture.ignoreRelationshipLimit && s.limit >= 0 && len(rows) > s.limit {
		rows = rows[:s.limit]
	}
	return delegate(newScriptedDumpCursor(rows, nil))
}

func (s *scriptedDumpDatabase) graphFixture(name string) *scriptedDumpGraph {
	fixture, found := s.graphs[name]
	if !found {
		panic(fmt.Sprintf("missing scripted graph %q", name))
	}
	return fixture
}

func (s *scriptedDumpDatabase) recordOperation(graphName string, phase Phase, operation string) {
	s.operations = append(s.operations, fmt.Sprintf("%s:%s:%s", graphName, phase, operation))
}

func scriptedDumpAfterID(criteria graph.Criteria) graph.ID {
	comparison, ok := criteria.(*cypherModel.Comparison)
	if !ok || len(comparison.Partials) != 1 {
		panic(fmt.Sprintf("unexpected scripted keyset criteria %T", criteria))
	}
	parameter, ok := comparison.Partials[0].Right.(*cypherModel.Parameter)
	if !ok {
		panic(fmt.Sprintf("unexpected scripted keyset value %T", comparison.Partials[0].Right))
	}
	value, ok := parameter.Value.(graph.ID)
	if !ok {
		panic(fmt.Sprintf("unexpected scripted keyset ID %T", parameter.Value))
	}
	return value
}

func filterScriptedDumpRows[T any](rows []T, afterID graph.ID, hasAfter bool, id func(T) graph.ID) []T {
	if !hasAfter {
		return rows
	}
	for index, row := range rows {
		if id(row) > afterID {
			return rows[index:]
		}
	}
	return nil
}

type scriptedDumpCursor[T any] struct {
	values chan T
	err    error
}

func newScriptedDumpCursor[T any](values []T, err error) *scriptedDumpCursor[T] {
	valueChannel := make(chan T, len(values))
	for _, value := range values {
		valueChannel <- value
	}
	close(valueChannel)
	return &scriptedDumpCursor[T]{values: valueChannel, err: err}
}

func (s *scriptedDumpCursor[T]) Error() error {
	return s.err
}

func (s *scriptedDumpCursor[T]) Close() {}

func (s *scriptedDumpCursor[T]) Chan() chan T {
	return s.values
}
