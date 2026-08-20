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
	"testing"

	"github.com/cucumber/godog"
	"github.com/cucumber/messages/go/v34"
	"github.com/specterops/dawgs/graph"
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

	nodeQuery       graph.NodeQuery
	result          graph.Result
	queryCalled     bool
	query           string
	queryParameters map[string]any
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
		node, valueIsNode := value.(graph.Node)
		targetNode, targetIsNode := target.(*graph.Node)
		if !valueIsNode || !targetIsNode {
			return false
		}

		*targetNode = node
		return true
	})
}

func (s *stubResult) Error() error {
	return s.err
}

func (s *stubResult) Close() {
	s.closed = true
}

type stubNodeQuery struct {
	graph.NodeQuery

	deleteErr    error
	deleteCalled bool
}

func (s *stubNodeQuery) Delete() error {
	s.deleteCalled = true
	return s.deleteErr
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

func TestDBContextExecutingQuery(t *testing.T) {
	nodeA := *graph.NewNode(1, graph.NewProperties(), graph.StringKind("A"))
	nodeB := *graph.NewNode(2, graph.NewProperties(), graph.StringKind("B"))
	nodeC := *graph.NewNode(3, graph.NewProperties(), graph.StringKind("C"))
	result := &stubResult{
		rows:       [][]any{{nodeA, nodeB}, {nodeC}},
		currentRow: -1,
	}
	transaction := &stubTransaction{result: result}
	database := &stubDatabase{transaction: transaction}
	databaseContext := &dbContext{
		db:         database,
		rowCount:   99,
		actualRows: []string{"stale result"},
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
	require.Equal(t, []string{"(:A)", "(:B)", "(:C)"}, databaseContext.actualRows)
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

func TestDBContextTheResultShouldBe(t *testing.T) {
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
				actualRows: []string{"(:A{name:'a'})", "(:B)"},
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
				actualRows: []string{"(:A)"},
			},
			expectedTable: newResultTable("(:A)", "(:B)"),
			expectedError: "Invalid row count expected 2 actual 1",
		},
		{
			name: "returns row content drift",
			context: dbContext{
				rowCount:   1,
				actualRows: []string{"(:B)"},
			},
			expectedTable: newResultTable("(:A)"),
			expectedError: "Detected a drift expected (:A), actual (:B)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.context.theResultShouldBe(test.expectedTable)

			if test.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectedError)
			}
		})
	}
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
	}
	actualList, err := formatGraphResults(nodes)
	require.Nil(t, err)

	expectedList := []string{"(:A{name: 'a'})", "(:B{name: 'b'})", "({name: 'c'})"}

	for i := range len(expectedList) {
		require.Equal(t, actualList[i], expectedList[i])
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
