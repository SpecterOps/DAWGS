// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0
package bdd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cucumber/godog"
	messages "github.com/cucumber/messages/go/v34"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/integration"
	"github.com/stretchr/testify/require"
)

type stubDatabase struct {
	graph.Database

	transaction             graph.Transaction
	readTransactionCalled   bool
	readTransactionContext  context.Context
	writeTransactionErr     error
	writeTransactionCalled  bool
	writeTransactionContext context.Context
}

func (s *stubDatabase) ReadTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	s.readTransactionCalled = true
	s.readTransactionContext = ctx

	return delegate(s.transaction)
}

func (s *stubDatabase) WriteTransaction(ctx context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	s.writeTransactionCalled = true
	s.writeTransactionContext = ctx

	if s.writeTransactionErr != nil {
		return s.writeTransactionErr
	}

	return delegate(s.transaction)
}

type stubTransaction struct {
	graph.Transaction

	nodeQuery         graph.NodeQuery
	relationshipQuery graph.RelationshipQuery
	result            graph.Result
	queryCalled       bool
	query             string
	queryParameters   map[string]any
}

func (s *stubTransaction) Relationships() graph.RelationshipQuery {
	return s.relationshipQuery
}

func (s *stubTransaction) Nodes() graph.NodeQuery {
	return s.nodeQuery
}

func (s *stubTransaction) Query(query string, parameters map[string]any) graph.Result {
	s.queryCalled = true
	s.query = query
	s.queryParameters = parameters

	return s.result
}

type stubResult struct {
	graph.Result

	rows       [][]any
	currentRow int
	err        error
	closed     bool
}

func (s *stubResult) Next() bool {
	if s.currentRow+1 >= len(s.rows) {
		return false
	}

	s.currentRow++
	return true
}

func (s *stubResult) Values() []any {
	return s.rows[s.currentRow]
}

func (s *stubResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(func(value, target any) bool {
		switch target := target.(type) {
		case *graph.Node:
			node, ok := value.(graph.Node)
			if ok {
				*target = node
			}
			return ok
		case *graph.Relationship:
			relationship, ok := value.(graph.Relationship)
			if ok {
				*target = relationship
			}
			return ok
		case *graph.Path:
			path, ok := value.(graph.Path)
			if ok {
				*target = path
			}
			return ok
		}
		return false
	})
}

func (s *stubResult) Error() error {
	return s.err
}

func (s *stubResult) Close() {
	s.closed = true
}

type stubCursor[T any] struct {
	values chan T
}

func newStubCursor[T any](values []T) *stubCursor[T] {
	cursor := &stubCursor[T]{values: make(chan T, len(values))}
	for _, value := range values {
		cursor.values <- value
	}
	close(cursor.values)
	return cursor
}

func (s *stubCursor[T]) Chan() chan T { return s.values }
func (s *stubCursor[T]) Error() error { return nil }
func (s *stubCursor[T]) Close()       {}

type stubNodeQuery struct {
	graph.NodeQuery

	deleteErr    error
	deleteCalled bool
	count        int64
	countErr     error
	nodes        []*graph.Node
}

func (s *stubNodeQuery) Delete() error {
	s.deleteCalled = true
	return s.deleteErr
}

func (s *stubNodeQuery) Count() (int64, error) {
	return s.count, s.countErr
}

func (s *stubNodeQuery) Fetch(delegate func(graph.Cursor[*graph.Node]) error, _ ...graph.Criteria) error {
	cursor := newStubCursor(s.nodes)
	defer cursor.Close()
	return delegate(cursor)
}

type stubRelationshipQuery struct {
	graph.RelationshipQuery
	count         int64
	countErr      error
	relationships []*graph.Relationship
}

func (s *stubRelationshipQuery) Count() (int64, error) {
	return s.count, s.countErr
}

func (s *stubRelationshipQuery) Fetch(delegate func(graph.Cursor[*graph.Relationship]) error) error {
	cursor := newStubCursor(s.relationships)
	defer cursor.Close()
	return delegate(cursor)
}

func TestDBContextAnEmptyGraph(t *testing.T) {
	deleteErr := errors.New("failed to delete nodes")
	writeTransactionErr := errors.New("failed to open write transaction")

	tests := []struct {
		name                string
		deleteErr           error
		writeTransactionErr error
		expectDelete        bool
		expectedErr         error
	}{
		{
			name:         "deletes all nodes",
			expectDelete: true,
		},
		{
			name:         "returns node deletion error",
			deleteErr:    deleteErr,
			expectDelete: true,
			expectedErr:  deleteErr,
		},
		{
			name:                "returns write transaction error",
			writeTransactionErr: writeTransactionErr,
			expectedErr:         writeTransactionErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testContext := context.WithValue(context.Background(), struct{}{}, test.name)
			nodeQuery := &stubNodeQuery{deleteErr: test.deleteErr}
			database := &stubDatabase{
				transaction:         &stubTransaction{nodeQuery: nodeQuery},
				writeTransactionErr: test.writeTransactionErr,
			}
			context := &dbContext{db: database}

			err := context.anEmptyGraph(testContext)

			if test.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.expectedErr)
			}
			require.True(t, database.writeTransactionCalled)
			require.Equal(t, testContext, database.writeTransactionContext)
			require.Equal(t, test.expectDelete, nodeQuery.deleteCalled)
		})
	}
}

// TestDBContextResetBeforeScenario checks that a scenario opens its session and clears prior result state.
func TestDBContextResetBeforeScenario(t *testing.T) {
	testContext := context.WithValue(context.Background(), struct{}{}, "scenario")
	oldDatabase := &stubDatabase{}
	nodeQuery := &stubNodeQuery{}
	database := &stubDatabase{
		transaction: &stubTransaction{
			nodeQuery: nodeQuery,
		},
	}
	databaseContext := &dbContext{
		db: oldDatabase,
		openSession: func(opts integration.Options) (*integration.Session, error) {
			require.Equal(t, &graph.Schema{
				DefaultGraph: graph.Graph{
					Name: "dawgs-bdd",
				},
			}, opts.Schema)
			return &integration.Session{
				DB: database,
			}, nil
		},
		beforeExecution: graphSnapshot{
			NodesCount:         2,
			RelationshipsCount: 1,
		},
		actualResult: &stubResult{},
		rowCount:     2,
		actualRows:   [][]string{{}},
		testData:     []string{"testdata/binary-tree-1.json", "testdata/binary-tree-2.json"},
	}

	returnedContext, err := databaseContext.resetBeforeScenario(testContext, nil)

	require.NoError(t, err)
	require.Equal(t, testContext, returnedContext)
	require.Same(t, database, databaseContext.db)
	require.False(t, oldDatabase.writeTransactionCalled)
	require.True(t, database.writeTransactionCalled)
	require.Equal(t, testContext, database.writeTransactionContext)
	require.True(t, nodeQuery.deleteCalled)
	require.Equal(t, []string{"testdata/binary-tree-1.json", "testdata/binary-tree-2.json"}, databaseContext.testData)
	require.Equal(t, graphSnapshot{}, databaseContext.beforeExecution)
	require.Nil(t, databaseContext.actualResult)
	require.Zero(t, databaseContext.rowCount)
	require.Nil(t, databaseContext.actualRows)
}

// TestDBContextResetBeforeScenarioReturnsCleanupError checks that graph reset failures propagate from scenario setup.
func TestDBContextResetBeforeScenarioReturnsCleanupError(t *testing.T) {
	deleteErr := errors.New("failed to delete nodes")
	writeErr := errors.New("failed to open write transaction")
	tests := []struct {
		// name identifies the graph cleanup failure scenario.
		name string
		// deleteErr injects a node-deletion failure into the transaction delegate.
		deleteErr error
		// writeErr injects a failure at the write-transaction boundary.
		writeErr error
		// expectedErr identifies the error that scenario setup must preserve.
		expectedErr error
		// expectDelete reports whether setup is expected to reach node deletion.
		expectDelete bool
	}{
		{
			name:         "node deletion error",
			deleteErr:    deleteErr,
			expectedErr:  deleteErr,
			expectDelete: true,
		},
		{
			name:        "write transaction error",
			writeErr:    writeErr,
			expectedErr: writeErr,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testContext := context.WithValue(context.Background(), struct{}{}, test.name)
			oldDatabase := &stubDatabase{}
			nodeQuery := &stubNodeQuery{
				deleteErr: test.deleteErr,
			}
			database := &stubDatabase{
				transaction: &stubTransaction{
					nodeQuery: nodeQuery,
				},
				writeTransactionErr: test.writeErr,
			}
			result := &stubResult{}
			snapshot := graphSnapshot{
				NodesCount:         2,
				RelationshipsCount: 1,
			}
			databaseContext := &dbContext{
				db: oldDatabase,
				openSession: func(integration.Options) (*integration.Session, error) {
					return &integration.Session{
						DB: database,
					}, nil
				},
				beforeExecution: snapshot,
				actualResult:    result,
				rowCount:        2,
				actualRows:      [][]string{{"stale"}},
			}

			returnedContext, err := databaseContext.resetBeforeScenario(testContext, nil)

			require.ErrorIs(t, err, test.expectedErr)
			require.ErrorContains(t, err, "reset graph before scenario")
			require.Equal(t, testContext, returnedContext)
			require.Same(t, database, databaseContext.db)
			require.False(t, oldDatabase.writeTransactionCalled)
			require.True(t, database.writeTransactionCalled)
			require.Equal(t, testContext, database.writeTransactionContext)
			require.Equal(t, test.expectDelete, nodeQuery.deleteCalled)
			require.Equal(t, graphSnapshot{}, databaseContext.beforeExecution)
			require.Nil(t, databaseContext.actualResult)
			require.Zero(t, databaseContext.rowCount)
			require.Nil(t, databaseContext.actualRows)
		})
	}
}

// TestDBContextResetBeforeScenarioReturnsSessionError checks that session creation failures stop scenario setup.
func TestDBContextResetBeforeScenarioReturnsSessionError(t *testing.T) {
	testContext := context.WithValue(context.Background(), struct{}{}, "scenario")
	openErr := errors.New("failed to open session")
	database := &stubDatabase{}
	result := &stubResult{}
	snapshot := graphSnapshot{
		NodesCount:         2,
		RelationshipsCount: 1,
	}
	databaseContext := &dbContext{
		db: database,
		openSession: func(integration.Options) (*integration.Session, error) {
			return nil, openErr
		},
		testData:        []string{"existing fixture"},
		beforeExecution: snapshot,
		actualResult:    result,
		rowCount:        2,
		actualRows:      [][]string{{"stale"}},
	}

	returnedContext, err := databaseContext.resetBeforeScenario(testContext, nil)

	require.ErrorIs(t, err, openErr)
	require.ErrorContains(t, err, "failed to establish database connection error")
	require.Equal(t, testContext, returnedContext)
	require.Nil(t, databaseContext.db)
	require.False(t, database.writeTransactionCalled)
	require.Equal(t, []string{"existing fixture"}, databaseContext.testData)
	require.Equal(t, graphSnapshot{
		NodesCount:         0,
		RelationshipsCount: 0,
	}, databaseContext.beforeExecution)
	require.Nil(t, databaseContext.actualResult)
	require.Equal(t, 0, databaseContext.rowCount)
	require.Nil(t, databaseContext.actualRows)
}

func TestDBContextExecutingQuery(t *testing.T) {
	nodeA := *graph.NewNode(1, graph.NewProperties(), graph.StringKind("A"))
	nodeB := *graph.NewNode(2, graph.NewProperties(), graph.StringKind("B"))
	nodeC := *graph.NewNode(3, graph.NewProperties(), graph.StringKind("C"))
	result := &stubResult{
		rows:       [][]any{{nodeA, nodeB}, {nodeC}},
		currentRow: -1,
	}
	transaction := &stubTransaction{
		result:            result,
		nodeQuery:         &stubNodeQuery{count: 3},
		relationshipQuery: &stubRelationshipQuery{count: 0},
	}
	database := &stubDatabase{transaction: transaction}
	databaseContext := &dbContext{
		db:         database,
		rowCount:   99,
		actualRows: [][]string{{"stale result"}},
	}
	testContext := context.Background()
	query := "MATCH (n) RETURN n"

	err := databaseContext.executingQuery(testContext, &godog.DocString{Content: query})

	require.NoError(t, err)
	require.True(t, database.readTransactionCalled)
	require.Equal(t, testContext, database.readTransactionContext)
	require.True(t, transaction.queryCalled)
	require.Equal(t, query, transaction.query)
	require.Nil(t, transaction.queryParameters)
	require.True(t, result.closed)
	require.Equal(t, 2, databaseContext.rowCount)
	require.Equal(t, [][]string{{"(:A)", "(:B)"}, {"(:C)"}}, databaseContext.actualRows)
}

func TestDBContextExecutingQueryWithPaths(t *testing.T) {
	nodeA := graph.NewNode(1, graph.AsProperties(map[string]any{
		"objectid": "source-513",
		"name":     "source-513",
	}), graph.StringKind("Group"))
	nodeB := graph.NewNode(2, graph.NewProperties(), graph.StringKind("Tag_Tier_Zero"), graph.StringKind("Computer"))
	nodeC := graph.NewNode(3, graph.NewProperties(), graph.StringKind("Group"))
	path := graph.Path{
		Nodes: []*graph.Node{nodeA, nodeB, nodeC, nodeC},
		Edges: []*graph.Relationship{
			graph.NewRelationship(1, 1, 2, graph.NewProperties(), graph.StringKind("AdminTo")),
			graph.NewRelationship(2, 3, 2, graph.AsProperties(map[string]any{
				"source": "fixture",
				"name":   "grant",
			}), graph.StringKind("GenericAll")),
			graph.NewRelationship(3, 3, 3, graph.NewProperties(), graph.StringKind("MemberOf")),
		},
	}
	result := &stubResult{
		rows:       [][]any{{path}, {graph.Path{Nodes: []*graph.Node{nodeB}}}},
		currentRow: -1,
	}
	databaseContext := &dbContext{db: &stubDatabase{transaction: &stubTransaction{
		result:            result,
		nodeQuery:         &stubNodeQuery{count: 3},
		relationshipQuery: &stubRelationshipQuery{count: 3},
	}}}

	require.NoError(t, databaseContext.executingQuery(context.Background(), &godog.DocString{Content: "MATCH p = ()-[*0..3]-() RETURN p"}))
	require.Equal(t, [][]string{
		{`(:Group{name:'source-513',objectid:'source-513'})-[:AdminTo]->(:Computer:Tag_Tier_Zero)<-[:GenericAll{name:'grant',source:'fixture'}]-(:Group)-[:MemberOf]->(:Group)`},
		{`(:Computer:Tag_Tier_Zero)`},
	}, databaseContext.actualRows)
	require.NoError(t, databaseContext.theResultShouldBeInAnyOrder(newResultTable(
		`(:Computer:Tag_Tier_Zero)`,
		`(:Group {name: "source-513", objectid: "source-513"})-[:AdminTo]->(:Computer:Tag_Tier_Zero)<-[:GenericAll {name: "grant", source: "fixture"}]-(:Group)-[:MemberOf]->(:Group)`,
	)))
	require.Equal(t, []string{"Tag_Tier_Zero", "Computer"}, nodeB.Kinds.Strings())
}

func TestFormatGraphValueRejectsMalformedPaths(t *testing.T) {
	nodeA := graph.NewNode(1, graph.NewProperties())
	nodeB := graph.NewNode(2, graph.NewProperties())
	for _, test := range []struct {
		name string
		path graph.Path
		want string
	}{
		{
			name: "empty path",
			want: "0 nodes for 0 relationships",
		},
		{
			name: "missing node",
			path: graph.Path{Nodes: []*graph.Node{nil}},
			want: "missing node at index 0",
		},
		{
			name: "missing relationship",
			path: graph.Path{Nodes: []*graph.Node{nodeA, nodeB}, Edges: []*graph.Relationship{nil}},
			want: "missing relationship or relationship kind at index 0",
		},
		{
			name: "disconnected relationship",
			path: graph.Path{
				Nodes: []*graph.Node{nodeA, nodeB},
				Edges: []*graph.Relationship{graph.NewRelationship(1, 1, 3, graph.NewProperties(), graph.StringKind("R"))},
			},
			want: "relationship at index 0 does not connect nodes 1 and 2",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := formatGraphValue((&stubResult{}).Mapper(), test.path)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestFormatGraphValueRelationship(t *testing.T) {
	relationship := *graph.NewRelationship(1, 2, 3, graph.NewProperties(), graph.StringKind("T1"))

	formatted, err := formatGraphValue((&stubResult{}).Mapper(), relationship)

	require.NoError(t, err)
	require.Equal(t, "[:T1]", formatted)
}

func TestFormatGraphValueRejectsUnsupportedValue(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
		err      error
	}{
		{
			value:    int16(123),
			expected: "",
			err:      fmt.Errorf("unsupported returned value of type int16"),
		},
		{
			value:    nil,
			expected: "null",
			err:      nil,
		},
	}

	for _, test := range tests {
		actual, err := formatGraphValue((&stubResult{}).Mapper(), test.value)
		require.Equal(t, err, test.err)
		require.Equal(t, test.expected, actual)
	}
}

func TestDBContextHavingExecuted(t *testing.T) {
	resultErr := errors.New("query failed")
	writeTransactionErr := errors.New("write transaction failed")

	tests := []struct {
		name                string
		resultErr           error
		writeTransactionErr error
		expectQuery         bool
		expectClose         bool
		expectedErr         error
	}{
		{
			name:        "executes write query",
			expectQuery: true,
			expectClose: true,
		},
		{
			name:        "returns result error",
			resultErr:   resultErr,
			expectQuery: true,
			expectClose: true,
			expectedErr: resultErr,
		},
		{
			name:                "returns write transaction error",
			writeTransactionErr: writeTransactionErr,
			expectedErr:         writeTransactionErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := &stubResult{err: test.resultErr}
			transaction := &stubTransaction{result: result}
			database := &stubDatabase{
				transaction:         transaction,
				writeTransactionErr: test.writeTransactionErr,
			}
			databaseContext := &dbContext{db: database}
			testContext := context.Background()
			query := "CREATE (:A)"

			err := databaseContext.havingExecuted(testContext, &godog.DocString{Content: query})

			if test.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.expectedErr)
			}
			require.True(t, database.writeTransactionCalled)
			require.Equal(t, testContext, database.writeTransactionContext)
			require.Equal(t, test.expectQuery, transaction.queryCalled)
			require.Equal(t, test.expectClose, result.closed)

			if test.expectQuery {
				require.Equal(t, query, transaction.query)
				require.Nil(t, transaction.queryParameters)
			}
		})
	}
}

func TestDBContexttheResultShouldBeInAnyOrder(t *testing.T) {
	tests := []struct {
		name          string
		context       dbContext
		expectedTable *godog.Table
		expectedError string
	}{
		{
			name: "matches normalized rows",
			context: dbContext{
				rowCount:   2,
				actualRows: [][]string{{"(:A{name:'a'})"}, {"(:B)"}},
			},
			expectedTable: newResultTable(`(:A {name: "a"})`, "(:B)"),
		},
		{
			name:          "matches an empty result",
			expectedTable: newResultTable(),
		},
		{
			name: "returns row count mismatch",
			context: dbContext{
				rowCount:   1,
				actualRows: [][]string{{"(:A)"}},
			},
			expectedTable: newResultTable("(:A)", "(:B)"),
			expectedError: "Invalid row count expected 2 actual 1",
		},
		{
			name: "returns row content drift",
			context: dbContext{
				rowCount:   1,
				actualRows: [][]string{{"(:B)"}},
			},
			expectedTable: newResultTable("(:A)"),
			expectedError: "Detected a drift expected (:A), actual (:B)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.context.theResultShouldBeInAnyOrder(test.expectedTable)

			if test.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectedError)
			}
		})
	}
}

func TestDBContextResultComparisonPreservesColumns(t *testing.T) {
	context := dbContext{
		rowCount:   2,
		actualRows: [][]string{{"(:A)", "(:B)"}, {"(:C)", "(:D)"}},
	}
	expected := &godog.Table{Rows: []*messages.PickleTableRow{
		{Cells: []*messages.PickleTableCell{{Value: "left"}, {Value: "right"}}},
		{Cells: []*messages.PickleTableCell{{Value: "(:C)"}, {Value: "(:D)"}}},
		{Cells: []*messages.PickleTableCell{{Value: "(:A)"}, {Value: "(:B)"}}},
	}}

	require.NoError(t, context.theResultShouldBeInAnyOrder(expected))
}

func TestDBContextSideEffects(t *testing.T) {
	tests := []struct {
		name          string
		before        graphSnapshot
		after         graphSnapshot
		expectedTable *godog.Table
		expectedError string
	}{
		{
			name:          "detect graph change",
			before:        graphSnapshot{NodesCount: 0, RelationshipsCount: 0},
			after:         graphSnapshot{NodesCount: 1, RelationshipsCount: 0},
			expectedTable: newResultTableNoHeader("+nodes", "1"),
			expectedError: "",
		},
		{
			name:          "detect unchanged graph",
			before:        graphSnapshot{NodesCount: 1},
			after:         graphSnapshot{NodesCount: 1},
			expectedTable: newResultTableNoHeader("+nodes", "0"),
			expectedError: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := &stubDatabase{
				transaction: &stubTransaction{
					nodeQuery:         &stubNodeQuery{count: test.after.NodesCount},
					relationshipQuery: &stubRelationshipQuery{count: test.after.RelationshipsCount},
				},
			}
			databaseContext := &dbContext{
				db:              database,
				beforeExecution: test.before,
				afterExecution:  test.after,
			}

			err := databaseContext.theSideEffectsShouldBe(test.expectedTable)

			if test.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectedError)
			}
		})
	}
}

func TestDBContextNoSideEffects(t *testing.T) {
	nodeA := graph.NewNode(1, graph.NewProperties())
	nodeB := graph.NewNode(2, graph.NewProperties())
	nodeC := graph.NewNode(3, graph.NewProperties())
	nodeWithProperty := graph.NewNode(1, graph.NewProperties().Set("name", "a"))
	relationship := graph.NewRelationship(1, 1, 2, graph.NewProperties(), graph.StringKind("T"))
	relationshipWithProperty := graph.NewRelationship(1, 1, 2, graph.NewProperties().Set("name", "r"), graph.StringKind("T"))

	tests := []struct {
		name          string
		before        graphSnapshot
		nodes         []*graph.Node
		relationships []*graph.Relationship
		expectedError string
	}{
		{
			name:          "accepts unchanged graph",
			before:        graphSnapshot{NodesCount: 2, RelationshipsCount: 1, PropertiesCount: 2},
			nodes:         []*graph.Node{nodeWithProperty, nodeB},
			relationships: []*graph.Relationship{relationshipWithProperty},
		},
		{
			name:          "detects node count drift",
			before:        graphSnapshot{NodesCount: 2, RelationshipsCount: 1},
			nodes:         []*graph.Node{nodeA, nodeB, nodeC},
			relationships: []*graph.Relationship{relationship},
			expectedError: "Graph state drift detected",
		},
		{
			name:          "detects relationship count drift",
			before:        graphSnapshot{NodesCount: 2, RelationshipsCount: 1},
			nodes:         []*graph.Node{nodeA, nodeB},
			expectedError: "Graph state drift detected",
		},
		{
			name:          "detects node property count drift",
			before:        graphSnapshot{NodesCount: 2, RelationshipsCount: 1},
			nodes:         []*graph.Node{nodeWithProperty, nodeB},
			relationships: []*graph.Relationship{relationship},
			expectedError: "Graph state drift detected",
		},
		{
			name:          "detects relationship property count drift",
			before:        graphSnapshot{NodesCount: 2, RelationshipsCount: 1},
			nodes:         []*graph.Node{nodeA, nodeB},
			relationships: []*graph.Relationship{relationshipWithProperty},
			expectedError: "Graph state drift detected",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := &stubDatabase{
				transaction: &stubTransaction{
					nodeQuery:         &stubNodeQuery{nodes: test.nodes},
					relationshipQuery: &stubRelationshipQuery{relationships: test.relationships},
				},
			}
			databaseContext := &dbContext{
				db:              database,
				beforeExecution: test.before,
			}

			err := databaseContext.noSideEffects(context.Background())

			if test.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectedError)
			}
		})
	}
}

func newResultTableNoHeader(key string, values ...string) *godog.Table {
	var rows []*messages.PickleTableRow
	for _, value := range values {
		rows = append(rows, &messages.PickleTableRow{
			Cells: []*messages.PickleTableCell{{Value: key}, {Value: value}},
		})
	}

	return &godog.Table{Rows: rows}
}

func newResultTable(values ...string) *godog.Table {
	rows := []*messages.PickleTableRow{
		{Cells: []*messages.PickleTableCell{{Value: "n"}}},
	}

	for _, value := range values {
		rows = append(rows, &messages.PickleTableRow{
			Cells: []*messages.PickleTableCell{{Value: value}},
		})
	}

	return &godog.Table{Rows: rows}
}

func TestFormatGraphResults(t *testing.T) {
	nodes := []graph.Node{
		{
			ID:    1,
			Kinds: graph.Kinds{graph.StringKind("A")},
			Properties: &graph.Properties{
				Map: map[string]any{"name": "a"},
			},
		},
		{
			ID:    2,
			Kinds: graph.Kinds{graph.StringKind("B")},
			Properties: &graph.Properties{
				Map: map[string]any{"name": "b"},
			},
		},
		{
			ID: 3,
			Properties: &graph.Properties{
				Map: map[string]any{"name": "c"},
			},
		},
		{
			ID:    4,
			Kinds: graph.Kinds{graph.StringKind("B"), graph.StringKind("A")},
		},
	}
	actualList, err := formatGraphResults(nodes)
	require.Nil(t, err)

	expectedList := []string{"(:A{name:'a'})", "(:B{name:'b'})", "({name:'c'})", "(:B:A)"}

	for i := range len(expectedList) {
		require.Equal(t, expectedList[i], actualList[i])
	}
}

func TestFormatString(t *testing.T) {
	tests := []struct {
		input          string
		expectedOutput string
	}{
		{
			input:          `(:B {prefix: 'c', name: 'b'})`,
			expectedOutput: `(:B{prefix:'c',name:'b'})`,
		},
		{
			input:          `(:B {prefix: "c", name: "b"})`,
			expectedOutput: `(:B{prefix:'c',name:'b'})`,
		},
		{
			input:          `(:B{prefix:"c",name:"b"})`,
			expectedOutput: `(:B{prefix:'c',name:'b'})`,
		},
		{
			input:          `(:B{prefix:'c',name:'b'})`,
			expectedOutput: `(:B{prefix:'c',name:'b'})`,
		},
	}

	for _, test := range tests {
		actual := formatString(test.input)
		require.Equal(t, test.expectedOutput, actual)
	}
}

func TestTheResultShouldBeEmpty(t *testing.T) {
	tests := []struct {
		dbContext     dbContext
		expectedError string
	}{
		{
			dbContext: dbContext{
				rowCount: 0,
				actualRows: [][]string{
					{"foo"},
				},
			},
			expectedError: "The result set is not empty",
		},
		{
			dbContext: dbContext{
				rowCount:   1,
				actualRows: [][]string{},
			},
			expectedError: "The result set is not empty",
		},
		{
			dbContext: dbContext{
				rowCount:   0,
				actualRows: [][]string{},
			},
			expectedError: "",
		},
	}

	for _, test := range tests {
		err := test.dbContext.theResultShouldBeEmpty()
		if test.expectedError == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, test.expectedError)
		}
	}
}

func TestAnyGraph(t *testing.T) {
	dbCtx := dbContext{
		testData: []string{"testdata/binary-tree-a.json"},
	}
	ctx := context.Background()
	err := dbCtx.anyGraph(ctx)
	require.EqualError(t, err, "open graph fixture: open testdata/binary-tree-a.json: no such file or directory")
}

func TestFormatGraphRelationship(t *testing.T) {
	relationship := graph.Relationship{
		ID:      1,
		StartID: 1,
		EndID:   2,
		Kind:    graph.StringKind("MemberOf"),
		Properties: &graph.Properties{
			Map: map[string]any{"name": "a"},
		},
	}

	output := formatGraphRelationship(relationship)
	require.Equal(t, "[:MemberOf{name: 'a'}]", output)
}

func TestHasReturnClause(t *testing.T) {
	tests := []struct {
		queryString string
		expected    bool
	}{
		{
			queryString: "CREATE ()-[]->()",
			expected:    false,
		},
		{
			queryString: "CREATE(n:A) RETURN n",
			expected:    true,
		},
	}

	for _, test := range tests {
		actual, err := hasReturnClause(test.queryString)
		require.Nil(t, err)
		require.Equal(t, test.expected, actual)
	}
}
