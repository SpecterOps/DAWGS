package retriever

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	cypherModel "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

type checkpointTestCursor[T any] struct {
	values chan T
}

func newCheckpointTestCursor[T any](values []T) *checkpointTestCursor[T] {
	valueC := make(chan T, len(values))
	for _, value := range values {
		valueC <- value
	}
	close(valueC)
	return &checkpointTestCursor[T]{values: valueC}
}

func (s *checkpointTestCursor[T]) Error() error { return nil }
func (s *checkpointTestCursor[T]) Close()       {}
func (s *checkpointTestCursor[T]) Chan() chan T { return s.values }

type checkpointTestDatabase struct {
	graph.Database
	nodes             []*graph.Node
	relationships     []*graph.Relationship
	nodeFetches       int
	failNodeFetch     int
	failNodeFetchOnce bool
}

func (s *checkpointTestDatabase) ReadTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&checkpointTestTransaction{database: s})
}

type checkpointTestTransaction struct {
	graph.Transaction
	database *checkpointTestDatabase
}

func (s *checkpointTestTransaction) WithGraph(graph.Graph) graph.Transaction { return s }
func (s *checkpointTestTransaction) Nodes() graph.NodeQuery {
	return &checkpointTestNodeQuery{database: s.database}
}
func (s *checkpointTestTransaction) Relationships() graph.RelationshipQuery {
	return &checkpointTestRelationshipQuery{database: s.database}
}

type checkpointTestNodeQuery struct {
	graph.NodeQuery
	database *checkpointTestDatabase
	after    graph.ID
	limit    int
}

func (s *checkpointTestNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.after = checkpointTestAfterID(criteria)
	return s
}
func (s *checkpointTestNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery { return s }
func (s *checkpointTestNodeQuery) Limit(limit int) graph.NodeQuery {
	s.limit = limit
	return s
}
func (s *checkpointTestNodeQuery) Count() (int64, error) {
	return int64(len(s.database.nodes)), nil
}
func (s *checkpointTestNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	s.database.nodeFetches++
	if s.database.failNodeFetchOnce && s.database.nodeFetches == s.database.failNodeFetch {
		s.database.failNodeFetchOnce = false
		return errors.New("injected node cursor failure")
	}
	values := make([]*graph.Node, 0, s.limit)
	for _, node := range s.database.nodes {
		if node.ID > s.after && len(values) < s.limit {
			values = append(values, node)
		}
	}
	return delegate(newCheckpointTestCursor(values))
}

type checkpointTestRelationshipQuery struct {
	graph.RelationshipQuery
	database *checkpointTestDatabase
	after    graph.ID
	limit    int
}

func (s *checkpointTestRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	s.after = checkpointTestAfterID(criteria)
	return s
}
func (s *checkpointTestRelationshipQuery) OrderBy(...graph.Criteria) graph.RelationshipQuery {
	return s
}
func (s *checkpointTestRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	s.limit = limit
	return s
}
func (s *checkpointTestRelationshipQuery) Count() (int64, error) {
	return int64(len(s.database.relationships)), nil
}
func (s *checkpointTestRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	values := make([]*graph.Relationship, 0, s.limit)
	for _, relationship := range s.database.relationships {
		if relationship.ID > s.after && len(values) < s.limit {
			values = append(values, relationship)
		}
	}
	return delegate(newCheckpointTestCursor(values))
}

func checkpointTestAfterID(criteria graph.Criteria) graph.ID {
	comparison := criteria.(*cypherModel.Comparison)
	parameter := comparison.Partials[0].Right.(*cypherModel.Parameter)
	return parameter.Value.(graph.ID)
}

func TestDumpResumesFromLastCommittedFragment(t *testing.T) {
	nodeKind := graph.StringKind("Node")
	edgeKind := graph.StringKind("Edge")
	database := &checkpointTestDatabase{
		nodes: []*graph.Node{
			graph.NewNode(1, graph.AsProperties(map[string]any{"name": "one"}), nodeKind),
			graph.NewNode(2, graph.AsProperties(map[string]any{"name": "two"}), nodeKind),
			graph.NewNode(3, graph.AsProperties(map[string]any{"name": "three"}), nodeKind),
			graph.NewNode(4, graph.AsProperties(map[string]any{"name": "four"}), nodeKind),
			graph.NewNode(5, graph.AsProperties(map[string]any{"name": "five"}), nodeKind),
		},
		relationships: []*graph.Relationship{
			graph.NewRelationship(10, 1, 2, graph.AsProperties(map[string]any{"index": 1}), edgeKind),
			graph.NewRelationship(11, 2, 3, graph.AsProperties(map[string]any{"index": 2}), edgeKind),
			graph.NewRelationship(12, 3, 4, graph.AsProperties(map[string]any{"index": 3}), edgeKind),
		},
		failNodeFetch:     2,
		failNodeFetchOnce: true,
	}

	outputDir := t.TempDir()
	options := DefaultDumpOptions(outputDir)
	options.Compression = CompressionNone
	options.BatchSize = 2
	options.ShardSize = 2
	options.Scrub = ScrubFull
	options.Salt = "resume-test-salt"

	_, err := Dump(context.Background(), database, "test", []GraphTarget{{Name: "default"}}, options)
	if err == nil {
		t.Fatal("expected injected dump failure")
	}
	if _, err := os.Stat(filepath.Join(outputDir, manifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("incomplete dump published a manifest: %v", err)
	}

	checkpoint, err := readDumpCheckpoint(outputDir)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if checkpoint.Current == nil || checkpoint.Current.LastCommittedID != 2 || fileTotal(checkpoint.Current.Files) != 2 {
		t.Fatalf("unexpected interrupted checkpoint: %+v", checkpoint.Current)
	}

	options.Resume = true
	database.nodes = append(database.nodes, graph.NewNode(6, graph.AsProperties(map[string]any{"name": "six"}), nodeKind))
	if _, err := Dump(context.Background(), database, "test", []GraphTarget{{Name: "default"}}, options); err == nil || !strings.Contains(err.Error(), "source counts") {
		t.Fatalf("expected stale source checkpoint rejection, got %v", err)
	}
	database.nodes = database.nodes[:5]

	result, err := Dump(context.Background(), database, "test", []GraphTarget{{Name: "default"}}, options)
	if err != nil {
		t.Fatalf("resume dump: %v", err)
	}
	if result.NodeCount != 5 || result.EdgeCount != 3 {
		t.Fatalf("unexpected resumed counts: %+v", result)
	}
	if result.Manifest.Scrub.NodeActionCounts[string(actionPseudonymize)] != 5 {
		t.Fatalf("resumed scrub action counts = %v", result.Manifest.Scrub.NodeActionCounts)
	}
	if _, err := os.Stat(filepath.Join(outputDir, dumpCheckpointFileName)); !os.IsNotExist(err) {
		t.Fatalf("completed dump retained checkpoint: %v", err)
	}

	var nodeIDs []string
	for _, fileEntry := range result.Manifest.Graphs[0].Files {
		if fileEntry.Phase != PhaseNodes {
			continue
		}
		_, err := readNodeFragmentFile(outputDir, result.Manifest.Compression, fileEntry, func(item FragmentNode) error {
			nodeIDs = append(nodeIDs, item.ID)
			return nil
		})
		if err != nil {
			t.Fatalf("read resumed node fragment: %v", err)
		}
	}
	if !reflect.DeepEqual(nodeIDs, []string{"1", "2", "3", "4", "5"}) {
		t.Fatalf("resumed node IDs = %v", nodeIDs)
	}
}

func TestDumpResumeRejectsIncompatibleIdentityAndUnexpectedFiles(t *testing.T) {
	outputDir := t.TempDir()
	options := DefaultDumpOptions(outputDir)
	identity, err := newDumpCheckpointIdentity("test", []GraphTarget{{Name: "default"}}, options, nil)
	if err != nil {
		t.Fatalf("checkpoint identity: %v", err)
	}
	manifest := newManifest("test", options.Compression, options.ZstdLevel, ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}, 1)
	metrics := newMetricsManifest(1)
	manifest.Metrics = &metrics
	checkpoint := dumpCheckpoint{Version: dumpCheckpointVersion, Identity: identity, Manifest: manifest}
	if err := writeDumpCheckpoint(outputDir, checkpoint); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	incompatible := identity
	incompatible.BatchSize++
	if _, err := loadCompatibleDumpCheckpoint(outputDir, incompatible, 1); err == nil {
		t.Fatal("expected incompatible checkpoint rejection")
	}

	if err := os.WriteFile(filepath.Join(outputDir, "orphan.jsonl"), []byte("orphan"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if _, err := loadCompatibleDumpCheckpoint(outputDir, identity, 1); err == nil {
		t.Fatal("expected unexpected checkpoint file rejection")
	}
}

func TestDumpRejectsInvalidGraphTargetsBeforeWritingOutput(t *testing.T) {
	for name, targets := range map[string][]GraphTarget{
		"empty":     {{Name: ""}},
		"duplicate": {{Name: "same"}, {Name: "same"}},
	} {
		t.Run(name, func(t *testing.T) {
			outputDir := filepath.Join(t.TempDir(), "output")
			_, err := Dump(context.Background(), nil, "test", targets, DefaultDumpOptions(outputDir))
			if err == nil {
				t.Fatal("expected invalid graph target error")
			}
			if _, statErr := os.Stat(outputDir); !os.IsNotExist(statErr) {
				t.Fatalf("invalid targets created output: %v", statErr)
			}
		})
	}
}
