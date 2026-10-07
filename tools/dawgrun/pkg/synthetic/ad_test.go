package synthetic

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func TestBuildIsDeterministicAndValid(t *testing.T) {
	options := Options{
		Nodes: 100,
		Edges: 400,
		Scale: 1,
		Seed:  1234,
	}

	first, firstStats, err := Build(options)
	require.NoError(t, err)
	second, secondStats, err := Build(options)
	require.NoError(t, err)

	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)

	require.Equal(t, firstJSON, secondJSON)
	require.Equal(t, firstStats, secondStats)
	require.Len(t, first.Nodes, 100)
	require.Len(t, first.Edges, 400)
	require.Equal(t, int64(100), firstStats.Nodes)
	require.Equal(t, int64(400), firstStats.Edges)
	require.NoError(t, opengraph.Validate(opengraph.Document{Graph: first}))
}

func TestDefaultOptionsUseReferenceScale(t *testing.T) {
	options, err := DefaultOptions().Normalize()
	require.NoError(t, err)
	require.Equal(t, ReferenceNodeCount, options.Nodes)
	require.Equal(t, ReferenceEdgeCount, options.Edges)
	require.Equal(t, DefaultBatchSize, options.BatchSize)
}

func TestBuildStressModes(t *testing.T) {
	document, stats, err := Build(Options{
		Nodes:        50,
		Edges:        200,
		Seed:         55,
		IndexStress:  true,
		IngestStress: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(40), stats.DuplicateEdges)
	require.Contains(t, stats.EdgeKinds, "MemberOf")

	names := make(map[any]int)
	for _, node := range document.Nodes {
		names[node.Properties["name"]]++
	}
	var repeatedNames int
	for _, count := range names {
		if count > 1 {
			repeatedNames++
		}
	}
	require.Greater(t, repeatedNames, 0)
	require.Contains(t, document.Nodes[0].Properties, "description")
}

func TestLoadUsesBulkIDsForEdges(t *testing.T) {
	database := &fakeDatabase{}
	stats, err := Load(context.Background(), database, Options{
		Nodes:     25,
		Edges:     100,
		BatchSize: 7,
		Seed:      9,
	})
	require.NoError(t, err)
	require.Equal(t, int64(25), stats.Nodes)
	require.Equal(t, int64(100), stats.Edges)
	require.Len(t, database.nodes, 25)
	require.Len(t, database.edges, 100)

	for _, edge := range database.edges {
		require.GreaterOrEqual(t, edge.start, graph.ID(1))
		require.LessOrEqual(t, edge.start, graph.ID(25))
		require.GreaterOrEqual(t, edge.end, graph.ID(1))
		require.LessOrEqual(t, edge.end, graph.ID(25))
	}
}

type fakeDatabase struct {
	nodes  []*graph.Node
	edges  []fakeEdge
	nextID graph.ID
}

type fakeEdge struct {
	start graph.ID
	end   graph.ID
}

func (s *fakeDatabase) SetWriteFlushSize(int) {}

func (s *fakeDatabase) SetBatchWriteSize(int) {}

func (s *fakeDatabase) ReadTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}

func (s *fakeDatabase) WriteTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}

func (s *fakeDatabase) BatchOperation(ctx context.Context, delegate graph.BatchDelegate, options ...graph.BatchOption) error {
	return delegate(&fakeBatch{database: s})
}

func (s *fakeDatabase) AssertSchema(context.Context, graph.Schema) error {
	return nil
}

func (s *fakeDatabase) SetDefaultGraph(context.Context, graph.Graph) error {
	return nil
}

func (s *fakeDatabase) Run(context.Context, string, map[string]any) error {
	return nil
}

func (s *fakeDatabase) Close(context.Context) error {
	return nil
}

func (s *fakeDatabase) FetchKinds(context.Context) (graph.Kinds, error) {
	return nil, nil
}

func (s *fakeDatabase) RefreshKinds(context.Context) error {
	return nil
}

func (s *fakeDatabase) OptimizeStorage(context.Context) error {
	return nil
}

type fakeBatch struct {
	database *fakeDatabase
}

func (s *fakeBatch) WithGraph(graph.Graph) graph.Batch {
	return s
}

func (s *fakeBatch) CreateNode(node *graph.Node) error {
	_, err := s.CreateNodes([]*graph.Node{node})
	return err
}

func (s *fakeBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	ids := make([]graph.ID, len(nodes))
	for index, node := range nodes {
		s.database.nextID++
		ids[index] = s.database.nextID
		s.database.nodes = append(s.database.nodes, node)
	}

	return ids, nil
}

func (s *fakeBatch) DeleteNode(graph.ID) error {
	return nil
}

func (s *fakeBatch) Nodes() graph.NodeQuery {
	return nil
}

func (s *fakeBatch) Relationships() graph.RelationshipQuery {
	return nil
}

func (s *fakeBatch) UpdateNodeBy(graph.NodeUpdate) error {
	return nil
}

func (s *fakeBatch) UpdateNodes([]*graph.Node) error {
	return nil
}

func (s *fakeBatch) CreateRelationship(*graph.Relationship) error {
	return nil
}

func (s *fakeBatch) CreateRelationshipByIDs(start, end graph.ID, _ graph.Kind, _ *graph.Properties) error {
	s.database.edges = append(s.database.edges, fakeEdge{start: start, end: end})
	return nil
}

func (s *fakeBatch) DeleteRelationship(graph.ID) error {
	return nil
}

func (s *fakeBatch) UpdateRelationshipBy(graph.RelationshipUpdate) error {
	return nil
}

func (s *fakeBatch) Commit() error {
	return nil
}

func (s *fakeBatch) Close() {}
