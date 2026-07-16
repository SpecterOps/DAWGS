package retriever

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func TestIdentityTransformSession(t *testing.T) {
	session := identityTransformSession{}
	if session.NeedsPreparation() {
		t.Fatalf("identity transform requires preparation")
	}
	if metadata := session.Metadata(); !reflect.DeepEqual(metadata, ScrubMetadata{
		Mode:             ScrubNone,
		NodeActionCounts: map[string]int{},
		EdgeActionCounts: map[string]int{},
	}) {
		t.Fatalf("identity metadata = %+v", metadata)
	}

	nodes := []*graph.Node{
		graph.NewNode(2, graph.AsProperties(map[string]any{"name": "alice"}), graph.StringKind("User"), graph.StringKind("Admin")),
		graph.NewNode(3, nil),
	}
	if actual := session.TransformNodes(nodes); !reflect.DeepEqual(actual, transformedBatch[normalizedNode]{
		Records: []normalizedNode{
			{ID: "2", Kinds: []string{"Admin", "User"}, Properties: map[string]any{"name": "alice"}},
			{ID: "3", Kinds: []string{}, Properties: map[string]any{}},
		},
		ActionCounts: make([]map[string]int, 2),
	}) {
		t.Fatalf("identity node batch = %#v", actual)
	}

	relationships := []*graph.Relationship{
		graph.NewRelationship(9, 2, 3, graph.AsProperties(map[string]any{"enabled": true}), graph.StringKind("MemberOf")),
	}
	if actual := session.TransformEdges(relationships); !reflect.DeepEqual(actual, transformedBatch[normalizedEdge]{
		Records: []normalizedEdge{
			{ID: "9", StartID: "2", EndID: "3", Kind: "MemberOf", Properties: map[string]any{"enabled": true}},
		},
		ActionCounts: make([]map[string]int, 1),
	}) {
		t.Fatalf("identity edge batch = %#v", actual)
	}
}

func TestFullScrubTransformSessionPreparationAndActions(t *testing.T) {
	const sourceSID = "S-1-5-21-111111111-222222222-333333333-1001"

	transform, err := newTransformSession(DumpOptions{Scrub: ScrubFull, Salt: "transform-test"})
	if err != nil {
		t.Fatalf("new transform: %v", err)
	}
	if !transform.NeedsPreparation() || transform.Metadata().Mode != ScrubFull {
		t.Fatalf("full scrub transform metadata = %+v", transform.Metadata())
	}

	transform.PrepareNode(graph.NewNode(1, graph.AsProperties(map[string]any{"objectid": sourceSID})))
	nodeBatch := transform.TransformNodes([]*graph.Node{
		graph.NewNode(1, graph.AsProperties(map[string]any{
			"description": "private text",
			"objectid":    sourceSID,
		})),
	})
	if len(nodeBatch.Records) != 1 {
		t.Fatalf("node records = %d", len(nodeBatch.Records))
	}
	if !reflect.DeepEqual(nodeBatch.ActionCounts[0], map[string]int{
		string(actionPseudonymize): 1,
		string(actionRedact):       1,
	}) {
		t.Fatalf("node actions = %+v", nodeBatch.ActionCounts[0])
	}
	if nodeBatch.Records[0].Properties["objectid"] == sourceSID || nodeBatch.Records[0].Properties["description"] != "[REDACTED]" {
		t.Fatalf("scrubbed node = %+v", nodeBatch.Records[0])
	}

	edgeBatch := transform.TransformEdges([]*graph.Relationship{
		graph.NewRelationship(4, 1, 2, graph.AsProperties(map[string]any{"owner_sid": sourceSID}), graph.StringKind("MemberOf")),
	})
	if len(edgeBatch.Records) != 1 || !reflect.DeepEqual(edgeBatch.ActionCounts[0], map[string]int{
		string(actionPseudonymize): 1,
	}) {
		t.Fatalf("edge batch = %#v", edgeBatch)
	}
	if edgeBatch.Records[0].Properties["owner_sid"] != nodeBatch.Records[0].Properties["objectid"] {
		t.Fatalf("prepared registry did not preserve reference equality")
	}
}

func TestDumpGraphTransformsAndObservesEachRecordOnce(t *testing.T) {
	source := &scriptedGraphSource{
		snapshot: graphEntitySnapshot{NodeCount: 2, EdgeCount: 1},
		nodeBatches: [][]*graph.Node{{
			graph.NewNode(1, nil, graph.StringKind("User")),
			graph.NewNode(2, nil, graph.StringKind("Group")),
		}},
		edgeBatches: [][]*graph.Relationship{{
			graph.NewRelationship(3, 1, 2, nil, graph.StringKind("MemberOf")),
		}},
	}
	transform := &countingTransformSession{
		identity:  identityTransformSession{},
		nodeCalls: map[string]int{},
		edgeCalls: map[string]int{},
	}

	options := DumpOptions{
		OutputDir:   t.TempDir(),
		Scrub:       ScrubNone,
		Compression: CompressionGzip,
		ZstdLevel:   DefaultZstdLevel,
		ShardSize:   10,
		BatchSize:   10,
	}
	workspace := newLocalCollectionWorkspace(options.OutputDir, false)
	graphEntry, schema, metrics, err := dumpGraph(
		context.Background(),
		source,
		GraphTarget{Name: "source"},
		options,
		transform,
		newShardSinkSet(newJSONLShardSink(newJSONLNodeSinkInWorkspace(options, workspace))),
		newShardSinkSet(newJSONLShardSink(newJSONLEdgeSinkInWorkspace(options, workspace))),
	)
	if err != nil {
		t.Fatalf("dump graph: %v", err)
	}

	if !reflect.DeepEqual(transform.nodeCalls, map[string]int{"1": 1, "2": 1}) || !reflect.DeepEqual(transform.edgeCalls, map[string]int{"3": 1}) {
		t.Fatalf("transform calls: nodes=%v edges=%v", transform.nodeCalls, transform.edgeCalls)
	}
	if graphEntry.NodeCount != 2 || graphEntry.EdgeCount != 1 || metrics.NodeCount != 2 || metrics.EdgeCount != 1 {
		t.Fatalf("observed totals: graph=%+v metrics=%+v", graphEntry, metrics)
	}
	if !reflect.DeepEqual(schema, GraphSchemaMetadata{Name: "source", NodeKinds: []string{"Group", "User"}, EdgeKinds: []string{"MemberOf"}}) {
		t.Fatalf("observed schema = %+v", schema)
	}
}

type countingTransformSession struct {
	identity  identityTransformSession
	nodeCalls map[string]int
	edgeCalls map[string]int
}

func (s *countingTransformSession) Metadata() ScrubMetadata {
	return s.identity.Metadata()
}

func (*countingTransformSession) NeedsPreparation() bool {
	return false
}

func (*countingTransformSession) PrepareNode(*graph.Node) {}

func (s *countingTransformSession) TransformNodes(nodes []*graph.Node) transformedBatch[normalizedNode] {
	for _, node := range nodes {
		s.nodeCalls[node.ID.String()]++
	}
	return s.identity.TransformNodes(nodes)
}

func (s *countingTransformSession) TransformEdges(relationships []*graph.Relationship) transformedBatch[normalizedEdge] {
	for _, relationship := range relationships {
		s.edgeCalls[relationship.ID.String()]++
	}
	return s.identity.TransformEdges(relationships)
}
