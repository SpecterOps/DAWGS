package dawgs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	cypherModel "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

type sourceTestCursor[T any] struct {
	values chan T
	err    error
}

func newSourceTestCursor[T any](values []T) *sourceTestCursor[T] {
	channel := make(chan T, len(values))
	for _, value := range values {
		channel <- value
	}
	close(channel)

	return &sourceTestCursor[T]{values: channel}
}

func (s *sourceTestCursor[T]) Error() error { return s.err }
func (s *sourceTestCursor[T]) Close()       {}
func (s *sourceTestCursor[T]) Chan() chan T { return s.values }

type sourceTestDatabase struct {
	graph.Database

	nodes         []*graph.Node
	relationships []*graph.Relationship
	nodeCount     int64
	edgeCount     int64

	nodeCountErr error
	edgeCountErr error
	nodeFetchErr error
	edgeFetchErr error

	contexts []context.Context
	graphs   []graph.Graph
	nodeOps  []sourceTestQueryOperation
	edgeOps  []sourceTestQueryOperation
}

type sourceTestQueryOperation struct {
	orderBy graph.Criteria
	filter  graph.Criteria
	limit   int
}

func (s *sourceTestDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	s.contexts = append(s.contexts, ctx)
	return delegate(&sourceTestTransaction{database: s})
}

type sourceTestTransaction struct {
	graph.Transaction
	database *sourceTestDatabase
}

func (s *sourceTestTransaction) WithGraph(target graph.Graph) graph.Transaction {
	s.database.graphs = append(s.database.graphs, target)
	return s
}

func (s *sourceTestTransaction) Nodes() graph.NodeQuery {
	return &sourceTestNodeQuery{database: s.database}
}

func (s *sourceTestTransaction) Relationships() graph.RelationshipQuery {
	return &sourceTestRelationshipQuery{database: s.database}
}

type sourceTestNodeQuery struct {
	graph.NodeQuery
	database  *sourceTestDatabase
	operation sourceTestQueryOperation
}

func (s *sourceTestNodeQuery) OrderBy(criteria ...graph.Criteria) graph.NodeQuery {
	if len(criteria) != 1 {
		panic(fmt.Sprintf("unexpected node order criteria: %d", len(criteria)))
	}
	s.operation.orderBy = criteria[0]
	return s
}

func (s *sourceTestNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	s.operation.filter = criteria
	return s
}

func (s *sourceTestNodeQuery) Limit(limit int) graph.NodeQuery {
	s.operation.limit = limit
	return s
}

func (s *sourceTestNodeQuery) Count() (int64, error) {
	if s.database.nodeCountErr != nil {
		return 0, s.database.nodeCountErr
	}
	if s.database.nodeCount != 0 {
		return s.database.nodeCount, nil
	}
	return int64(len(s.database.nodes)), nil
}

func (s *sourceTestNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	s.database.nodeOps = append(s.database.nodeOps, s.operation)
	if s.database.nodeFetchErr != nil {
		return s.database.nodeFetchErr
	}

	return delegate(newSourceTestCursor(sourceTestNodesAfter(s.database.nodes, s.operation)))
}

type sourceTestRelationshipQuery struct {
	graph.RelationshipQuery
	database  *sourceTestDatabase
	operation sourceTestQueryOperation
}

func (s *sourceTestRelationshipQuery) OrderBy(criteria ...graph.Criteria) graph.RelationshipQuery {
	if len(criteria) != 1 {
		panic(fmt.Sprintf("unexpected relationship order criteria: %d", len(criteria)))
	}
	s.operation.orderBy = criteria[0]
	return s
}

func (s *sourceTestRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	s.operation.filter = criteria
	return s
}

func (s *sourceTestRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	s.operation.limit = limit
	return s
}

func (s *sourceTestRelationshipQuery) Count() (int64, error) {
	if s.database.edgeCountErr != nil {
		return 0, s.database.edgeCountErr
	}
	if s.database.edgeCount != 0 {
		return s.database.edgeCount, nil
	}
	return int64(len(s.database.relationships)), nil
}

func (s *sourceTestRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	s.database.edgeOps = append(s.database.edgeOps, s.operation)
	if s.database.edgeFetchErr != nil {
		return s.database.edgeFetchErr
	}

	return delegate(newSourceTestCursor(sourceTestRelationshipsAfter(s.database.relationships, s.operation)))
}

func sourceTestNodesAfter(nodes []*graph.Node, operation sourceTestQueryOperation) []*graph.Node {
	afterID := sourceTestAfterID(operation.filter)
	values := make([]*graph.Node, 0, operation.limit)
	for _, node := range nodes {
		if node.ID > afterID && len(values) < operation.limit {
			values = append(values, node)
		}
	}
	return values
}

func sourceTestRelationshipsAfter(relationships []*graph.Relationship, operation sourceTestQueryOperation) []*graph.Relationship {
	afterID := sourceTestAfterID(operation.filter)
	values := make([]*graph.Relationship, 0, operation.limit)
	for _, relationship := range relationships {
		if relationship.ID > afterID && len(values) < operation.limit {
			values = append(values, relationship)
		}
	}
	return values
}

func sourceTestAfterID(criteria graph.Criteria) graph.ID {
	if criteria == nil {
		return 0
	}
	comparison := criteria.(*cypherModel.Comparison)
	return comparison.Partials[0].Right.(*cypherModel.Parameter).Value.(graph.ID)
}

func newSourceTestDatabase(nodes []*graph.Node, relationships []*graph.Relationship) *sourceTestDatabase {
	return &sourceTestDatabase{nodes: nodes, relationships: relationships}
}

func newSourceForTest(t *testing.T, database *sourceTestDatabase, batchSize int) *Source {
	t.Helper()
	source, err := NewSource(database, "example", batchSize)
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	return source
}

func TestNewSourceRejectsMissingGraphAndNonPositiveBatchSize(t *testing.T) {
	database := newSourceTestDatabase(nil, nil)
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
			if _, err := NewSource(database, input.graphName, input.batchSize); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSnapshotCountsEachEntityKindInSelectedGraph(t *testing.T) {
	database := newSourceTestDatabase(
		[]*graph.Node{graph.NewNode(1, graph.NewProperties(), graph.StringKind("User")), graph.NewNode(2, graph.NewProperties(), graph.StringKind("Group"))},
		[]*graph.Relationship{graph.NewRelationship(3, 1, 2, graph.NewProperties(), graph.StringKind("Member"))},
	)
	source := newSourceForTest(t, database, 2)
	ctx := context.WithValue(context.Background(), sourceTestContextKey{}, "snapshot")

	snapshot, err := source.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if want := (Snapshot{NodeCount: 2, RelationshipCount: 1}); snapshot != want {
		t.Fatalf("snapshot=%+v want=%+v", snapshot, want)
	}
	if len(database.graphs) != 1 || database.graphs[0].Name != "example" {
		t.Fatalf("graphs=%+v", database.graphs)
	}
	if len(database.contexts) != 1 || database.contexts[0] != ctx {
		t.Fatal("snapshot did not pass context to ReadTransaction")
	}
}

func TestNextNodesConvertsBatchesAndAdvancesCursorForOneEntity(t *testing.T) {
	nested := map[string]any{"active": true}
	properties := map[string]any{"name": "Ada", "metadata": nested}
	database := newSourceTestDatabase([]*graph.Node{
		graph.NewNode(7, graph.AsProperties(properties), graph.StringKind("User"), graph.StringKind("Admin"), graph.StringKind("User")),
	}, nil)
	source := newSourceForTest(t, database, 2)
	ctx := context.WithValue(context.Background(), sourceTestContextKey{}, "nodes")

	first, err := source.NextNodes(ctx)
	if err != nil {
		t.Fatalf("next nodes: %v", err)
	}
	if first.LastID != 7 || len(first.Entities) != 1 {
		t.Fatalf("first batch=%+v", first)
	}
	if got, want := first.Entities[0].SourceID, "7"; got != want {
		t.Fatalf("source ID=%q want=%q", got, want)
	}
	if got, want := first.Entities[0].Kinds, []string{"User", "Admin", "User"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kinds=%v want=%v", got, want)
	}
	if got, want := first.Entities[0].Properties, properties; !reflect.DeepEqual(got, want) {
		t.Fatalf("properties=%v want=%v", got, want)
	}

	first.Entities[0].Properties["name"] = "Grace"
	if properties["name"] != "Ada" {
		t.Fatal("returned top-level properties map aliases the database map")
	}
	first.Entities[0].Properties["metadata"].(map[string]any)["active"] = false
	if nested["active"] != false {
		t.Fatal("returned nested property value was deep copied")
	}

	second, err := source.NextNodes(ctx)
	if err != nil {
		t.Fatalf("next nodes after one-row batch: %v", err)
	}
	if len(second.Entities) != 0 || second.LastID != 0 {
		t.Fatalf("second batch=%+v", second)
	}
	if len(database.nodeOps) != 2 {
		t.Fatalf("node operations=%d", len(database.nodeOps))
	}
	assertSourceTestKeysetQuery(t, database.nodeOps[0], "n", 0, 2)
	assertSourceTestKeysetQuery(t, database.nodeOps[1], "n", 7, 2)
	if len(database.contexts) != 2 || database.contexts[0] != ctx || database.contexts[1] != ctx {
		t.Fatal("node reads did not pass context to ReadTransaction")
	}
}

func TestNextRelationshipsUsesIndependentCursorAndCanonicalEndpoints(t *testing.T) {
	database := newSourceTestDatabase(
		[]*graph.Node{graph.NewNode(9, graph.NewProperties(), graph.StringKind("User"))},
		[]*graph.Relationship{graph.NewRelationship(4, 10, 20, graph.AsProperties(map[string]any{"role": "owner"}), graph.StringKind("Owns"))},
	)
	source := newSourceForTest(t, database, 1)
	source.SetNodeCursor(9)
	source.SetRelationshipCursor(3)

	batch, err := source.NextRelationships(context.Background())
	if err != nil {
		t.Fatalf("next relationships: %v", err)
	}
	if batch.LastID != 4 || len(batch.Entities) != 1 {
		t.Fatalf("relationship batch=%+v", batch)
	}
	if want := (struct {
		SourceID string
		StartID  string
		EndID    string
		Kind     string
	}{"4", "10", "20", "Owns"}); struct {
		SourceID string
		StartID  string
		EndID    string
		Kind     string
	}{batch.Entities[0].SourceID, batch.Entities[0].StartID, batch.Entities[0].EndID, batch.Entities[0].Kind} != want {
		t.Fatalf("relationship=%+v", batch.Entities[0])
	}
	assertSourceTestKeysetQuery(t, database.edgeOps[0], "r", 3, 1)

	nodes, err := source.NextNodes(context.Background())
	if err != nil {
		t.Fatalf("next nodes: %v", err)
	}
	if len(nodes.Entities) != 0 {
		t.Fatalf("node cursor was not independent: %+v", nodes)
	}
	assertSourceTestKeysetQuery(t, database.nodeOps[0], "n", 9, 1)
}

func TestNextNodesReturnsEmptyBatchWithoutChangingCursor(t *testing.T) {
	database := newSourceTestDatabase(nil, nil)
	source := newSourceForTest(t, database, 3)
	source.SetNodeCursor(12)

	batch, err := source.NextNodes(context.Background())
	if err != nil {
		t.Fatalf("next nodes: %v", err)
	}
	if len(batch.Entities) != 0 || batch.LastID != 0 {
		t.Fatalf("batch=%+v", batch)
	}

	_, err = source.NextNodes(context.Background())
	if err != nil {
		t.Fatalf("second next nodes: %v", err)
	}
	assertSourceTestKeysetQuery(t, database.nodeOps[1], "n", 12, 3)
}

func TestSourceErrorsIncludeGraphAndPhaseAndWrapCause(t *testing.T) {
	nodeCountErr := errors.New("node count failed")
	database := newSourceTestDatabase(nil, nil)
	database.nodeCountErr = nodeCountErr
	source := newSourceForTest(t, database, 1)

	_, err := source.Snapshot(context.Background())
	assertSourceTestError(t, err, nodeCountErr, "graph \"example\"", "snapshot", "nodes")

	nodeFetchErr := errors.New("node query failed")
	database.nodeCountErr = nil
	database.nodeFetchErr = nodeFetchErr
	_, err = source.NextNodes(context.Background())
	assertSourceTestError(t, err, nodeFetchErr, "graph \"example\"", "nodes")

	edgeCountErr := errors.New("relationship count failed")
	database.nodeFetchErr = nil
	database.edgeCountErr = edgeCountErr
	_, err = source.Snapshot(context.Background())
	assertSourceTestError(t, err, edgeCountErr, "graph \"example\"", "snapshot", "relationships")

	edgeFetchErr := errors.New("relationship query failed")
	database.edgeCountErr = nil
	database.edgeFetchErr = edgeFetchErr
	_, err = source.NextRelationships(context.Background())
	assertSourceTestError(t, err, edgeFetchErr, "graph \"example\"", "relationships")
}

type sourceTestContextKey struct{}

func assertSourceTestKeysetQuery(t *testing.T, operation sourceTestQueryOperation, variable string, afterID graph.ID, limit int) {
	t.Helper()
	if operation.limit != limit {
		t.Fatalf("limit=%d want=%d", operation.limit, limit)
	}
	assertSourceTestIDExpression(t, operation.orderBy, variable)
	comparison, ok := operation.filter.(*cypherModel.Comparison)
	if !ok {
		t.Fatalf("filter=%T want keyset comparison", operation.filter)
	}
	assertSourceTestIDExpression(t, comparison.Left, variable)
	if len(comparison.Partials) != 1 {
		t.Fatalf("filter partials=%d", len(comparison.Partials))
	}
	if comparison.Partials[0].Operator != cypherModel.OperatorGreaterThan {
		t.Fatalf("filter operator=%q want=%q", comparison.Partials[0].Operator, cypherModel.OperatorGreaterThan)
	}
	parameter, ok := comparison.Partials[0].Right.(*cypherModel.Parameter)
	if !ok || parameter.Value != afterID {
		t.Fatalf("filter right=%#v want graph.ID(%d)", comparison.Partials[0].Right, afterID)
	}
}

func assertSourceTestIDExpression(t *testing.T, criteria graph.Criteria, variable string) {
	t.Helper()
	function, ok := criteria.(*cypherModel.FunctionInvocation)
	if !ok || function.Name != "id" || len(function.Arguments) != 1 {
		t.Fatalf("ID criteria=%#v", criteria)
	}
	argument, ok := function.Arguments[0].(*cypherModel.Variable)
	if !ok || argument.Symbol != variable {
		t.Fatalf("ID variable=%#v want %q", function.Arguments[0], variable)
	}
}

func assertSourceTestError(t *testing.T, err, cause error, fragments ...string) {
	t.Helper()
	if !errors.Is(err, cause) {
		t.Fatalf("error %v does not wrap %v", err, cause)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q does not contain %q", err, fragment)
		}
	}
}
