package dawgs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ret/dawgs"
	"github.com/specterops/dawgs/ret/entity"
	"github.com/stretchr/testify/require"
)

func TestNewTargetRejectsMissingGraphAndNonPositiveBatchSize(t *testing.T) {
	database := &targetTestDatabase{}
	for name, input := range map[string]struct {
		graphName string
		batchSize int
	}{
		"missing graph":       {batchSize: 1},
		"blank graph":         {graphName: " \t", batchSize: 1},
		"zero batch size":     {graphName: "example"},
		"negative batch size": {graphName: "example", batchSize: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := dawgs.NewTarget(database, input.graphName, input.batchSize)
			require.Error(t, err)
		})
	}
}

func TestTargetRequireEmptyRejectsExistingNodesAndRelationships(t *testing.T) {
	for name, input := range map[string]struct {
		database *targetTestDatabase
		counts   string
	}{
		"nodes":         {database: &targetTestDatabase{nodeCount: 1}, counts: "nodes=1 relationships=0"},
		"relationships": {database: &targetTestDatabase{relationshipCount: 2}, counts: "nodes=0 relationships=2"},
	} {
		t.Run(name, func(t *testing.T) {
			target := newTargetForTest(t, input.database, 2)

			err := target.RequireEmpty(context.Background())

			require.ErrorIs(t, err, dawgs.ErrTargetNotEmpty)
			require.ErrorContains(t, err, `graph "example" is not empty`)
			require.ErrorContains(t, err, input.counts)
			require.Len(t, input.database.readGraphs, 1)
			require.Equal(t, "example", input.database.readGraphs[0].Name)
		})
	}
}

func TestTargetRequireEmptyPreservesSnapshotFailureClassification(t *testing.T) {
	// Break caught: making every failed emptiness probe indistinguishable from
	// a graph whose nonzero counts were actually observed.
	injected := errors.New("injected snapshot failure")
	target := newTargetForTest(t, &targetTestDatabase{readErr: injected}, 1)

	err := target.RequireEmpty(context.Background())

	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, dawgs.ErrTargetNotEmpty)
}

func TestTargetRequireEmptyPreservesCancellationClassification(t *testing.T) {
	// Break caught: converting context cancellation into a not-empty result.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := newTargetForTest(t, &targetTestDatabase{}, 1)

	err := target.RequireEmpty(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, dawgs.ErrTargetNotEmpty)
}

func TestTargetAssertSchemaScopesFirstSeenKindsToSelectedGraph(t *testing.T) {
	database := &targetTestDatabase{}
	target := newTargetForTest(t, database, 3)

	require.NoError(t, target.AssertSchema(context.Background(), []string{"User", "MEMBER_OF", "User", "Group"}))
	require.Len(t, database.schemas, 1)

	schema := database.schemas[0]
	require.Len(t, schema.Graphs, 1)
	require.Equal(t, "example", schema.Graphs[0].Name)
	require.Equal(t, []string{"User", "MEMBER_OF", "Group"}, schema.Graphs[0].Nodes.Strings())
	require.Equal(t, []string{"User", "MEMBER_OF", "Group"}, schema.Graphs[0].Edges.Strings())
	require.Equal(t, "example", schema.DefaultGraph.Name)
}

func TestTargetCreateNodesPreservesKindOrderPropertiesAndCorrelatedIDs(t *testing.T) {
	database := &targetTestDatabase{createdIDs: [][]graph.ID{{20, 10}}}
	target := newTargetForTest(t, database, 7)
	resolver := dawgs.NewResolver(2)
	nested := map[string]any{"value": "shared"}
	nodes := []entity.Node{
		{SourceID: "first", Kinds: []string{"User", "Admin", "User"}, Properties: map[string]any{"name": "Ada", "nested": nested}},
		{SourceID: "second", Kinds: []string{"Group"}, Properties: map[string]any{"name": "Operators"}},
	}

	require.NoError(t, target.CreateNodes(context.Background(), nodes, resolver))

	require.Len(t, database.nodes, 2)
	require.Equal(t, []string{"User", "Admin", "User"}, database.nodes[0].Kinds.Strings())
	require.Equal(t, "Ada", database.nodes[0].Properties.Map["name"])
	nodes[0].Properties["name"] = "Grace"
	nested["value"] = "still-shared"
	require.Equal(t, "Ada", database.nodes[0].Properties.Map["name"])
	require.Equal(t, "still-shared", database.nodes[0].Properties.Map["nested"].(map[string]any)["value"])
	require.Equal(t, graph.ID(20), mustResolve(t, resolver, "first"))
	require.Equal(t, graph.ID(10), mustResolve(t, resolver, "second"))
	require.Equal(t, []targetTestBatchRecord{{graphName: "example", batchSize: 7}}, database.batches)
}

func TestTargetCreateNodesRejectsDestinationIDCountMismatchWithoutMappings(t *testing.T) {
	database := &targetTestDatabase{createdIDs: [][]graph.ID{{10}}}
	target := newTargetForTest(t, database, 2)
	resolver := dawgs.NewResolver(2)
	require.True(t, resolver.Put("already-present", 99))

	err := target.CreateNodes(context.Background(), []entity.Node{{SourceID: "one"}, {SourceID: "two"}}, resolver)

	require.ErrorContains(t, err, "created node ID count: got 1 want 2")
	require.Empty(t, database.nodes)
	require.Equal(t, graph.ID(99), mustResolve(t, resolver, "already-present"))
	_, found := resolver.Resolve("one")
	require.False(t, found)
}

func TestTargetCreateNodesPreflightsDuplicateAndExistingSourceIDs(t *testing.T) {
	for name, input := range map[string]struct {
		present     bool
		nodes       []entity.Node
		destination []graph.ID
	}{
		"duplicate input": {
			nodes:       []entity.Node{{SourceID: "same"}, {SourceID: "same"}},
			destination: []graph.ID{10, 20},
		},
		"existing canonical ID": {
			present:     true,
			nodes:       []entity.Node{{SourceID: "1"}},
			destination: []graph.ID{10},
		},
	} {
		t.Run(name, func(t *testing.T) {
			database := &targetTestDatabase{createdIDs: [][]graph.ID{input.destination}}
			target := newTargetForTest(t, database, 2)
			resolver := dawgs.NewResolver(2)
			if input.present {
				require.True(t, resolver.Put("1", 99))
			}

			err := target.CreateNodes(context.Background(), input.nodes, resolver)

			require.ErrorContains(t, err, "duplicate source node ID")
			require.Empty(t, database.nodes)
			require.Zero(t, database.createNodesCalls)
			if input.present {
				require.Equal(t, graph.ID(99), mustResolve(t, resolver, "1"))
			} else {
				_, found := resolver.Resolve("same")
				require.False(t, found)
			}
		})
	}
}

func TestTargetCreateNodesLeavesResolverUnchangedWhenBatchFailsAfterCreation(t *testing.T) {
	batchErr := errors.New("injected batch failure")
	database := &targetTestDatabase{createdIDs: [][]graph.ID{{10}}, batchErr: batchErr}
	target := newTargetForTest(t, database, 1)
	resolver := dawgs.NewResolver(2)
	require.True(t, resolver.Put("already-present", 99))

	err := target.CreateNodes(context.Background(), []entity.Node{{SourceID: "new"}}, resolver)

	require.ErrorIs(t, err, batchErr)
	require.Empty(t, database.nodes)
	require.Equal(t, graph.ID(99), mustResolve(t, resolver, "already-present"))
	_, found := resolver.Resolve("new")
	require.False(t, found)
}

func TestTargetCreateNodesRequiresCorrelatedBatchCreator(t *testing.T) {
	database := &targetTestDatabase{supportsNodeBatchCreator: false}
	target := newTargetForTest(t, database, 1)

	err := target.CreateNodes(context.Background(), []entity.Node{{SourceID: "one"}}, dawgs.NewResolver(1))

	require.ErrorContains(t, err, "does not support correlated node creation")
	require.Empty(t, database.nodes)
}

func TestTargetCreateRelationshipsResolvesEndpointsAndIgnoresSourceID(t *testing.T) {
	database := &targetTestDatabase{}
	target := newTargetForTest(t, database, 4)
	resolver := dawgs.NewResolver(2)
	require.True(t, resolver.Put("node-a", 30))
	require.True(t, resolver.Put("node-b", 40))
	nested := map[string]any{"value": "shared"}
	relationships := []entity.Relationship{{
		StartID:    "node-a",
		EndID:      "node-b",
		Kind:       "MEMBER_OF",
		Properties: map[string]any{"nested": nested},
	}}

	require.NoError(t, target.CreateRelationships(context.Background(), relationships, resolver))

	require.Equal(t, []targetTestRelationship{{startID: 30, endID: 40, kind: "MEMBER_OF", properties: map[string]any{"nested": nested}}}, database.relationships)
	nested["value"] = "still-shared"
	require.Equal(t, "still-shared", database.relationships[0].properties["nested"].(map[string]any)["value"])
	require.Equal(t, []targetTestBatchRecord{{graphName: "example", batchSize: 4}}, database.batches)
}

func TestTargetCreateRelationshipsRejectsMissingEndpointBeforeCreatingRelationships(t *testing.T) {
	database := &targetTestDatabase{}
	target := newTargetForTest(t, database, 1)
	resolver := dawgs.NewResolver(1)
	require.True(t, resolver.Put("start", 10))

	err := target.CreateRelationships(context.Background(), []entity.Relationship{{StartID: "start", EndID: "missing", Kind: "REL"}}, resolver)

	require.ErrorContains(t, err, `relationship 0 has unresolved endpoints "start" -> "missing"`)
	require.Empty(t, database.relationships)
}

func newTargetForTest(t *testing.T, database *targetTestDatabase, batchSize int) *dawgs.Target {
	t.Helper()
	target, err := dawgs.NewTarget(database, "example", batchSize)
	require.NoError(t, err)
	return target
}

type targetTestDatabase struct {
	graph.Database

	readErr           error
	nodeCount         int64
	relationshipCount int64
	readGraphs        []graph.Graph
	schemas           []graph.Schema

	createdIDs               [][]graph.ID
	supportsNodeBatchCreator bool
	rollbackErr              error
	batchErr                 error
	rollbacks                int
	createNodesCalls         int
	nodes                    []*graph.Node
	relationships            []targetTestRelationship
	batches                  []targetTestBatchRecord
}

func (s *targetTestDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	if s.readErr != nil {
		return s.readErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return delegate(&targetTestTransaction{database: s})
}

func (s *targetTestDatabase) AssertSchema(_ context.Context, schema graph.Schema) error {
	s.schemas = append(s.schemas, schema)
	return nil
}

func (s *targetTestDatabase) BatchOperation(_ context.Context, delegate graph.BatchDelegate, options ...graph.BatchOption) error {
	config := graph.BatchConfig{}
	for _, option := range options {
		option(&config)
	}

	if !s.supportsNodeBatchCreator && s.createdIDs == nil {
		batch := &targetTestUncorrelatedBatch{database: s, batchSize: config.BatchSize}
		if err := delegate(batch); err != nil {
			return s.rollback(err)
		}
		s.commit(batch.nodes, batch.relationships, batch.graphName, batch.batchSize)
		return nil
	}

	batch := &targetTestBatch{database: s, batchSize: config.BatchSize}
	if err := delegate(batch); err != nil {
		return s.rollback(err)
	}
	if s.batchErr != nil {
		return s.batchErr
	}
	s.commit(batch.nodes, batch.relationships, batch.graphName, batch.batchSize)
	return nil
}

func (s *targetTestDatabase) rollback(delegateErr error) error {
	s.rollbacks++
	if s.rollbackErr != nil {
		return s.rollbackErr
	}
	return delegateErr
}

func (s *targetTestDatabase) commit(nodes []*graph.Node, relationships []targetTestRelationship, graphName string, batchSize int) {
	s.nodes = append(s.nodes, nodes...)
	s.relationships = append(s.relationships, relationships...)
	s.batches = append(s.batches, targetTestBatchRecord{graphName: graphName, batchSize: batchSize})
}

type targetTestTransaction struct {
	graph.Transaction
	database *targetTestDatabase
}

func (s *targetTestTransaction) WithGraph(target graph.Graph) graph.Transaction {
	s.database.readGraphs = append(s.database.readGraphs, target)
	return s
}

func (s *targetTestTransaction) Nodes() graph.NodeQuery {
	return targetTestNodeQuery{count: s.database.nodeCount}
}

func (s *targetTestTransaction) Relationships() graph.RelationshipQuery {
	return targetTestRelationshipQuery{count: s.database.relationshipCount}
}

type targetTestNodeQuery struct {
	graph.NodeQuery
	count int64
}

func (s targetTestNodeQuery) Count() (int64, error) { return s.count, nil }

type targetTestRelationshipQuery struct {
	graph.RelationshipQuery
	count int64
}

func (s targetTestRelationshipQuery) Count() (int64, error) { return s.count, nil }

type targetTestBatchRecord struct {
	graphName string
	batchSize int
}

type targetTestRelationship struct {
	startID    graph.ID
	endID      graph.ID
	kind       string
	properties map[string]any
}

type targetTestBatch struct {
	graph.Batch
	database      *targetTestDatabase
	graphName     string
	batchSize     int
	nodes         []*graph.Node
	relationships []targetTestRelationship
}

func (s *targetTestBatch) WithGraph(target graph.Graph) graph.Batch {
	s.graphName = target.Name
	return s
}

func (s *targetTestBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	s.database.createNodesCalls++
	s.nodes = append(s.nodes, nodes...)
	call := len(s.database.batches)
	if call < len(s.database.createdIDs) {
		return append([]graph.ID(nil), s.database.createdIDs[call]...), nil
	}
	ids := make([]graph.ID, len(nodes))
	for index := range ids {
		ids[index] = graph.ID(len(s.database.nodes) + index + 1)
	}
	return ids, nil
}

func (s *targetTestBatch) CreateRelationshipByIDs(startID, endID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	s.relationships = append(s.relationships, targetTestRelationship{startID: startID, endID: endID, kind: kind.String(), properties: properties.Map})
	return nil
}

type targetTestUncorrelatedBatch struct {
	graph.Batch
	database      *targetTestDatabase
	graphName     string
	batchSize     int
	nodes         []*graph.Node
	relationships []targetTestRelationship
}

func (s *targetTestUncorrelatedBatch) WithGraph(target graph.Graph) graph.Batch {
	s.graphName = target.Name
	return s
}

func (s *targetTestUncorrelatedBatch) CreateRelationshipByIDs(startID, endID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	s.relationships = append(s.relationships, targetTestRelationship{startID: startID, endID: endID, kind: kind.String(), properties: properties.Map})
	return nil
}
