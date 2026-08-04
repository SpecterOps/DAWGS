// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestMeasureWriteCypherRollsBackWarmupAndEveryIteration(t *testing.T) {
	database := &scaleWriteTestDatabase{nodes: 2, relationships: 3, deleteCount: 1}
	postStateCount := int64(2)
	scenario := resolvedWriteScenario{
		SelectionCypher:  "selection",
		AffectedEntity:   "relationship",
		ExpectedMatched:  1,
		ExpectedAffected: 1,
		PostState: []resolvedStateQuery{{
			Name:     "surviving relationships",
			Cypher:   "relationship count",
			Expected: ExpectedResult{ScalarInt: &postStateCount},
		}},
	}

	measurement, stats, err := measureWriteCypher(context.Background(), database, "delete", nil, scenario, 2)

	require.NoError(t, err)
	require.Equal(t, int64(1), measurement.Matched)
	require.Equal(t, int64(1), measurement.Affected)
	require.Equal(t, int64(2), *measurement.PostState[0].ScalarInt)
	require.Equal(t, 2, stats.Iterations)
	require.Equal(t, 3, database.writeTransactions)
	require.Equal(t, int64(3), database.relationships, "every write transaction must roll back")
}

func TestMeasureWriteCypherRejectsOverBroadMutation(t *testing.T) {
	database := &scaleWriteTestDatabase{nodes: 2, relationships: 3, deleteCount: 2}
	scenario := resolvedWriteScenario{
		SelectionCypher:  "selection",
		AffectedEntity:   "relationship",
		ExpectedMatched:  1,
		ExpectedAffected: 1,
		PostState: []resolvedStateQuery{{
			Name:     "survivors",
			Cypher:   "relationship count",
			Expected: ExpectedResult{RowCount: int64Pointer(1)},
		}},
	}

	_, _, err := measureWriteCypher(context.Background(), database, "delete", nil, scenario, 1)
	require.ErrorContains(t, err, "expected 1 affected relationships, got 2")
	require.Equal(t, int64(3), database.relationships)
}

func TestMeasureWriteCypherRejectsUnderBroadMutation(t *testing.T) {
	database := &scaleWriteTestDatabase{nodes: 2, relationships: 3, deleteCount: 0}
	scenario := resolvedWriteScenario{
		SelectionCypher:  "selection",
		AffectedEntity:   "relationship",
		ExpectedMatched:  1,
		ExpectedAffected: 1,
		PostState: []resolvedStateQuery{{
			Name:     "survivors",
			Cypher:   "relationship count",
			Expected: ExpectedResult{RowCount: int64Pointer(1)},
		}},
	}

	_, _, err := measureWriteCypher(context.Background(), database, "delete", nil, scenario, 1)
	require.ErrorContains(t, err, "expected 1 affected relationships, got 0")
	require.Equal(t, int64(3), database.relationships)
}

func int64Pointer(value int64) *int64 {
	return &value
}

type scaleWriteTestDatabase struct {
	graph.Database
	nodes             int64
	relationships     int64
	deleteCount       int64
	writeTransactions int
}

func (s *scaleWriteTestDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	s.writeTransactions++
	originalNodes := s.nodes
	originalRelationships := s.relationships
	err := delegate(&scaleWriteTestTransaction{database: s})
	if err != nil {
		s.nodes = originalNodes
		s.relationships = originalRelationships
	}

	return err
}

type scaleWriteTestTransaction struct {
	graph.Transaction
	database *scaleWriteTestDatabase
}

func (s *scaleWriteTestTransaction) Query(cypher string, _ map[string]any) graph.Result {
	switch cypher {
	case "selection":
		return &scaleWriteTestResult{rows: [][]any{{int64(1)}}}
	case "delete":
		s.database.relationships -= s.database.deleteCount
		return &scaleWriteTestResult{}
	case "relationship count":
		return &scaleWriteTestResult{rows: [][]any{{s.database.relationships}}}
	default:
		return &scaleWriteTestResult{err: errors.New("unexpected query")}
	}
}

func (s *scaleWriteTestTransaction) Nodes() graph.NodeQuery {
	return &scaleWriteTestNodeQuery{count: s.database.nodes}
}

func (s *scaleWriteTestTransaction) Relationships() graph.RelationshipQuery {
	return &scaleWriteTestRelationshipQuery{count: s.database.relationships}
}

type scaleWriteTestNodeQuery struct {
	graph.NodeQuery
	count int64
}

func (s *scaleWriteTestNodeQuery) Count() (int64, error) {
	return s.count, nil
}

type scaleWriteTestRelationshipQuery struct {
	graph.RelationshipQuery
	count int64
}

func (s *scaleWriteTestRelationshipQuery) Count() (int64, error) {
	return s.count, nil
}

type scaleWriteTestResult struct {
	rows [][]any
	idx  int
	err  error
}

func (s *scaleWriteTestResult) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}

func (s *scaleWriteTestResult) Keys() []string {
	return nil
}

func (s *scaleWriteTestResult) Values() []any {
	if s.idx == 0 || s.idx > len(s.rows) {
		return nil
	}

	return s.rows[s.idx-1]
}

func (s *scaleWriteTestResult) Mapper() graph.ValueMapper {
	return graph.ValueMapper{}
}

func (s *scaleWriteTestResult) Scan(...any) error {
	return nil
}

func (s *scaleWriteTestResult) Error() error {
	return s.err
}

func (s *scaleWriteTestResult) Close() {}
