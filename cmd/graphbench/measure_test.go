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
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// TestStableRowValuesReverseMapsNodeIDs verifies that scalar and node IDs become fixture keys while node-kind metadata is preserved.
func TestStableRowValuesReverseMapsNodeIDs(t *testing.T) {
	values, err := stableRowValues(
		[]any{int64(101), graph.NewNode(102, nil, graph.StringKind("Group"))},
		graph.NewValueMapper(),
		reverseIDMap(opengraph.IDMap{"start": 101, "end": 102}),
		true,
		false,
	)
	require.NoError(t, err)
	require.Equal(t, "start", values[0])
	require.Equal(t, stableNodeObservation{
		Identity: "end",
		Kinds:    []string{"Group"},
	}, values[1])
}

// TestResultContainsNodeIDs verifies that only ID-set and ID-row expectations request physical-to-logical ID normalization.
func TestResultContainsNodeIDs(t *testing.T) {
	require.True(t, resultContainsNodeIDs(ExpectedResult{ResultKind: "id_set"}))
	require.True(t, resultContainsNodeIDs(ExpectedResult{ResultKind: "id_rows"}))
	require.False(t, resultContainsNodeIDs(ExpectedResult{ResultKind: "scalar"}))
	require.False(t, resultContainsNodeIDs(ExpectedResult{ResultKind: "path_set"}))
}

// TestStableRowValuesMapsNativePathValues verifies that driver-native paths normalize into logical node identities and directed relationship observations.
func TestStableRowValuesMapsNativePathValues(t *testing.T) {
	start := graph.NewNode(1, nil, graph.StringKind("Start"))
	end := graph.NewNode(2, nil, graph.StringKind("End"))
	edge := graph.NewRelationship(3, 1, 2, nil, graph.StringKind("Edge"))
	mapper := graph.NewValueMapper(func(value, target any) bool {
		path, sourceOK := value.(string)
		mapped, targetOK := target.(*graph.Path)
		if sourceOK && targetOK && path == "native-path" {
			*mapped = graph.Path{
				Nodes: []*graph.Node{start, end},
				Edges: []*graph.Relationship{edge},
			}
			return true
		}
		return false
	})

	values, err := stableRowValues(
		[]any{"native-path"},
		mapper,
		reverseIDMap(opengraph.IDMap{"start": 1, "end": 2}),
		false,
		true,
	)

	require.NoError(t, err)
	require.Equal(t, stablePathObservation{
		Nodes: []stableNodeObservation{
			{
				Identity: "start",
				Kinds:    []string{"Start"},
			},
			{
				Identity: "end",
				Kinds:    []string{"End"},
			},
		},
		Relationships: []stableRelationshipObservation{{
			Start: "start",
			End:   "end",
			Kind:  "Edge",
		}},
	}, values[0])
}

// TestStableRowValuesRejectsRelationshipReuseWithinPath verifies that observation normalization rejects a trail containing the same physical relationship twice.
func TestStableRowValuesRejectsRelationshipReuseWithinPath(t *testing.T) {
	start := graph.NewNode(1, nil)
	end := graph.NewNode(2, nil)
	relationship := graph.NewRelationship(10, 1, 2, nil, graph.StringKind("Edge"))
	_, err := stableRowValues([]any{graph.Path{
		Nodes: []*graph.Node{start, end, start},
		Edges: []*graph.Relationship{relationship, relationship},
	}}, graph.NewValueMapper(), reverseIDMap(opengraph.IDMap{"start": 1, "end": 2}), false, true)
	require.ErrorContains(t, err, "reuses relationship ID 10")
}

// TestStableRelationshipUsesLogicalFixtureKeyAsCrossBackendIdentity verifies that a relationship's logical_key property, rather than its backend ID, identifies it across engines.
func TestStableRelationshipUsesLogicalFixtureKeyAsCrossBackendIdentity(t *testing.T) {
	properties := graph.NewProperties().Set("logical_key", "branch-0001-level-02")
	relationship := graph.NewRelationship(99, 1, 2, properties, graph.StringKind("MemberOf"))

	stable := stableRelationship(relationship, map[graph.ID]string{1: "start", 2: "end"})
	require.Equal(t, "branch-0001-level-02", stable.Identity)
	require.Equal(t, "start", stable.Start)
	require.Equal(t, "end", stable.End)
}

// TestObserveCypherReturnsZeroValueOnResultError verifies that an iterator failure cannot leak a partially populated state observation.
func TestObserveCypherReturnsZeroValueOnResultError(t *testing.T) {
	tx := &scaleWriteTestTransaction{
		database: &scaleWriteTestDatabase{},
	}

	observation, err := observeCypher(tx, "unexpected", nil)

	require.ErrorContains(t, err, "unexpected query")
	require.Equal(t, StateQueryResult{}, observation)
}

// TestMeasureWriteCypherRollsBackWarmupAndEveryIteration verifies matched/affected/post-state measurements, cold-versus-warm classification, and rollback after every sampled mutation.
func TestMeasureWriteCypherRollsBackWarmupAndEveryIteration(t *testing.T) {
	database := &scaleWriteTestDatabase{
		nodes:         2,
		relationships: 3,
		deleteCount:   1,
	}
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
	require.Len(t, stats.Samples, 3)
	require.Equal(t, "cold", stats.Samples[0].Classification)
	require.Equal(t, "warm", stats.Samples[1].Classification)
	require.Equal(t, 3, database.writeTransactions)
	require.Equal(t, int64(3), database.relationships, "every write transaction must roll back")
}

// TestMeasureWriteCypherRecordsConfiguredUntimedWarmups verifies that configured warmups execute transactions and update metadata without entering the timing sample set.
func TestMeasureWriteCypherRecordsConfiguredUntimedWarmups(t *testing.T) {
	database := &scaleWriteTestDatabase{
		nodes:         2,
		relationships: 3,
		deleteCount:   1,
	}
	scenario := resolvedWriteScenario{
		SelectionCypher:  "selection",
		AffectedEntity:   "relationship",
		ExpectedMatched:  1,
		ExpectedAffected: 1,
	}

	_, stats, err := measureWriteCypherWithWarmups(context.Background(), database, "delete", nil, scenario, 2, 1)
	require.NoError(t, err)
	require.Equal(t, 2, stats.WarmupIterations)
	require.Len(t, stats.Samples, 2, "configured warmups must not become samples")
	require.Equal(t, 4, database.writeTransactions, "cold + two warmups + one timed transaction")
}

// TestMeasureWriteCypherRejectsOverBroadMutation verifies that deleting more relationships than declared fails validation and leaves the fixture unchanged.
func TestMeasureWriteCypherRejectsOverBroadMutation(t *testing.T) {
	database := &scaleWriteTestDatabase{
		nodes:         2,
		relationships: 3,
		deleteCount:   2,
	}
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

// TestMeasureWriteCypherRejectsUnderBroadMutation verifies that deleting fewer relationships than declared fails validation and leaves the fixture unchanged.
func TestMeasureWriteCypherRejectsUnderBroadMutation(t *testing.T) {
	database := &scaleWriteTestDatabase{
		nodes:         2,
		relationships: 3,
		deleteCount:   0,
	}
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

// int64Pointer returns a pointer to the supplied integer for optional expectations.
func int64Pointer(value int64) *int64 {
	return &value
}

// scaleWriteTestDatabase models mutable entity counts and rollback boundaries for write measurements.
type scaleWriteTestDatabase struct {
	// Database supplies methods outside the transaction interaction under test.
	graph.Database

	// nodes is the mutable node cardinality visible to count queries.
	nodes int64

	// relationships is the mutable relationship cardinality restored on rollback.
	relationships int64

	// deleteCount controls how many relationships the synthetic mutation removes.
	deleteCount int64

	// writeTransactions counts cold, warmup, and measured transaction attempts.
	writeTransactions int
}

// WriteTransaction runs the delegate and restores entity counts when its sentinel error requests rollback.
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

// scaleWriteTestTransaction interprets the synthetic selection, deletion, and post-state query names used by write measurements.
type scaleWriteTestTransaction struct {
	// Transaction supplies operations outside the query and count surfaces under test.
	graph.Transaction

	// database owns the mutable cardinalities affected by synthetic queries.
	database *scaleWriteTestDatabase
}

// Query maps synthetic query names to selection rows, cardinality mutation, post-state counts, or a terminal error.
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

// Nodes returns the current node-cardinality snapshot used to compute affected entities.
func (s *scaleWriteTestTransaction) Nodes() graph.NodeQuery {
	return &scaleWriteTestNodeQuery{count: s.database.nodes}
}

// Relationships returns the current relationship-cardinality snapshot used to compute affected entities.
func (s *scaleWriteTestTransaction) Relationships() graph.RelationshipQuery {
	return &scaleWriteTestRelationshipQuery{count: s.database.relationships}
}

// scaleWriteTestNodeQuery exposes a fixed node cardinality through the graph query interface.
type scaleWriteTestNodeQuery struct {
	// NodeQuery supplies query methods other than Count.
	graph.NodeQuery

	// count is the node cardinality returned to mutation accounting.
	count int64
}

// Count returns the node snapshot without a query failure.
func (s *scaleWriteTestNodeQuery) Count() (int64, error) {
	return s.count, nil
}

// scaleWriteTestRelationshipQuery exposes a fixed relationship cardinality through the graph query interface.
type scaleWriteTestRelationshipQuery struct {
	// RelationshipQuery supplies query methods other than Count.
	graph.RelationshipQuery

	// count is the relationship cardinality returned to mutation accounting.
	count int64
}

// Count returns the relationship snapshot without a query failure.
func (s *scaleWriteTestRelationshipQuery) Count() (int64, error) {
	return s.count, nil
}

// scaleWriteTestResult iterates configured rows and errors for write-measurement tests.
type scaleWriteTestResult struct {
	// rows contains the synthetic values exposed by iteration.
	rows [][]any

	// idx is the one-based cursor position after a successful Next call.
	idx int

	// err is returned after iteration completes.
	err error
}

// Next advances the one-based cursor while synthetic rows remain.
func (s *scaleWriteTestResult) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}

// Keys returns no column names because write-measurement observations consume values positionally.
func (s *scaleWriteTestResult) Keys() []string {
	return nil
}

// Values returns the current synthetic row or nil before and after valid iteration.
func (s *scaleWriteTestResult) Values() []any {
	if s.idx == 0 || s.idx > len(s.rows) {
		return nil
	}

	return s.rows[s.idx-1]
}

// Mapper returns the zero mapper because the synthetic rows contain primitive counts only.
func (s *scaleWriteTestResult) Mapper() graph.ValueMapper {
	return graph.ValueMapper{}
}

// Scan satisfies graph.Result; these tests consume rows through Values.
func (s *scaleWriteTestResult) Scan(...any) error {
	return nil
}

// Error returns the configured terminal iterator error.
func (s *scaleWriteTestResult) Error() error {
	return s.err
}

// Close satisfies graph.Result; this fake owns no resource.
func (s *scaleWriteTestResult) Close() {}
