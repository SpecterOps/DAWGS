package main

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestGeneratedADCSV2DatasetCarriesExactExpectations(t *testing.T) {
	name := "generated_adcs_v2_d16_f1000_r1_x1_i0_m2_z1_p0"
	config, ok := parseADCSV2DatasetName(name)
	require.True(t, ok)
	require.Equal(t, 16, config.MemberOfDepth)
	require.Equal(t, 1, *config.ExactReachableSuffixSources)
	require.Equal(t, 2, config.SuffixPathsPerBoundary)

	metadata, err := fixtureMetadata("unused", name)
	require.NoError(t, err)
	require.NotNil(t, metadata.ADCS)
	require.Equal(t, int64(16_001), metadata.ADCS.ForwardMemberStates)
	require.Equal(t, int64(6), metadata.ADCS.SuffixRows)
	require.Equal(t, int64(3), metadata.ADCS.DistinctBoundaries)
	require.Equal(t, int64(19), metadata.ADCS.ExpectedReverseStates)
	require.Equal(t, int64(4), metadata.ADCS.CompleteOutputTrails)
}

func TestGeneratedADCSV2DatasetRejectsInvalidOrNonCanonicalNames(t *testing.T) {
	for _, name := range []string{
		"generated_adcs_v2_d16_f1000_r1001_x1_i0_m1_z1_p0",
		"generated_adcs_v2_d16_f1000_r1_x1_i0_m0_z1_p0",
		"generated_adcs_v2_d16_f1000_r1_x1_i0_m1_z2_p0",
		"generated_adcs_v2_d016_f1000_r1_x1_i0_m1_z1_p0",
	} {
		_, ok := parseADCSV2DatasetName(name)
		require.False(t, ok, name)
		require.Nil(t, generatedDataset(name), name)
	}
}

func TestClearGraphDeletesRelationshipsBeforeNodes(t *testing.T) {
	database := &clearGraphTestDatabase{}

	require.NoError(t, clearGraph(context.Background(), database))
	require.Equal(t, []string{"relationships", "nodes"}, database.deletes)
}

func TestClearGraphStopsWhenRelationshipDeleteFails(t *testing.T) {
	database := &clearGraphTestDatabase{relationshipError: errors.New("relationship failure")}

	err := clearGraph(context.Background(), database)
	require.ErrorContains(t, err, "delete relationships: relationship failure")
	require.Equal(t, []string{"relationships"}, database.deletes)
}

func TestClearGraphReportsNodeDeleteFailure(t *testing.T) {
	database := &clearGraphTestDatabase{nodeError: errors.New("node failure")}

	err := clearGraph(context.Background(), database)
	require.ErrorContains(t, err, "delete nodes: node failure")
	require.Equal(t, []string{"relationships", "nodes"}, database.deletes)
}

func TestClearPostgresGraphTruncatesPhysicalPartitionsTogether(t *testing.T) {
	database := &clearPostgresGraphTestDatabase{}

	require.NoError(t, clearPostgresGraph(context.Background(), database, 42))
	require.Equal(t, []string{"truncate table edge_42, node_42"}, database.statements)
	require.Equal(t, []map[string]any{nil}, database.parameters)
}

func TestClearPostgresGraphRollsBackAfterRawDeleteFailure(t *testing.T) {
	database := &clearPostgresGraphTestDatabase{failAt: 1}

	err := clearPostgresGraph(context.Background(), database, 42)
	require.ErrorContains(t, err, "execute PostgreSQL graph reset")
	require.Equal(t, []string{"truncate table edge_42, node_42"}, database.statements)
}

type clearGraphTestDatabase struct {
	graph.Database
	deletes           []string
	relationshipError error
	nodeError         error
}

func (s *clearGraphTestDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&clearGraphTestTransaction{database: s})
}

type clearGraphTestTransaction struct {
	graph.Transaction
	database *clearGraphTestDatabase
}

func (s *clearGraphTestTransaction) Relationships() graph.RelationshipQuery {
	return &clearGraphTestRelationshipQuery{database: s.database}
}

func (s *clearGraphTestTransaction) Nodes() graph.NodeQuery {
	return &clearGraphTestNodeQuery{database: s.database}
}

type clearGraphTestRelationshipQuery struct {
	graph.RelationshipQuery
	database *clearGraphTestDatabase
}

func (s *clearGraphTestRelationshipQuery) Delete() error {
	s.database.deletes = append(s.database.deletes, "relationships")
	return s.database.relationshipError
}

type clearGraphTestNodeQuery struct {
	graph.NodeQuery
	database *clearGraphTestDatabase
}

func (s *clearGraphTestNodeQuery) Delete() error {
	s.database.deletes = append(s.database.deletes, "nodes")
	return s.database.nodeError
}

type clearPostgresGraphTestDatabase struct {
	graph.Database
	statements []string
	parameters []map[string]any
	failAt     int
}

func (s *clearPostgresGraphTestDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&clearPostgresGraphTestTransaction{database: s})
}

type clearPostgresGraphTestTransaction struct {
	graph.Transaction
	database *clearPostgresGraphTestDatabase
}

func (s *clearPostgresGraphTestTransaction) Raw(statement string, parameters map[string]any) graph.Result {
	s.database.statements = append(s.database.statements, statement)
	s.database.parameters = append(s.database.parameters, parameters)
	if s.database.failAt > 0 && len(s.database.statements) == s.database.failAt {
		return &clearPostgresGraphTestResult{err: errors.New("raw delete failure")}
	}

	return &clearPostgresGraphTestResult{}
}

type clearPostgresGraphTestResult struct {
	graph.Result
	err error
}

func (s *clearPostgresGraphTestResult) Error() error {
	return s.err
}

func (s *clearPostgresGraphTestResult) Close() {}
