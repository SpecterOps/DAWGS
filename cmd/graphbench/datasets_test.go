package main

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// TestGeneratedFixedSuffixExpansionV2DatasetCarriesExactExpectations verifies that a canonical encoded name derives exact forward, reverse, boundary, suffix-row, and output-trail counts.
func TestGeneratedFixedSuffixExpansionV2DatasetCarriesExactExpectations(t *testing.T) {
	name := "generated_fixed_suffix_expansion_v2_d16_f1000_r1_x1_i0_m2_z1_p0"
	config, ok := parseFixedSuffixExpansionV2DatasetName(name)
	require.True(t, ok)
	require.Equal(t, 16, config.ExpansionDepth)
	require.Equal(t, 1, *config.ExactReachableSuffixSources)
	require.Equal(t, 2, config.SuffixPathsPerBoundary)

	metadata, err := fixtureMetadata("unused", name)
	require.NoError(t, err)
	require.NotNil(t, metadata.FixedSuffixExpansion)
	require.Equal(t, int64(16_001), metadata.FixedSuffixExpansion.ForwardExpansionStates)
	require.Equal(t, int64(6), metadata.FixedSuffixExpansion.SuffixRows)
	require.Equal(t, int64(3), metadata.FixedSuffixExpansion.DistinctBoundaries)
	require.Equal(t, int64(19), metadata.FixedSuffixExpansion.ExpectedReverseStates)
	require.Equal(t, int64(4), metadata.FixedSuffixExpansion.CompleteOutputTrails)
}

// TestGeneratedEndpointSeededExpansionDatasetRoundTripsWithExactExpectations verifies lossless name encoding and the expected endpoint, prefix, output, and reverse-search cardinalities.
func TestGeneratedEndpointSeededExpansionDatasetRoundTripsWithExactExpectations(t *testing.T) {
	config := testutil.EndpointSeededExpansionScaleConfig{
		Depth: 3, MatchingEndpoints: 2, OtherEndpoints: 1,
		MatchingEligibleLanes: 2, OtherEligibleLanes: 1, MatchingIneligibleLanes: 1,
		ParallelEdges: 1, AddCycle: false, PropertyPayloadSize: 8,
	}
	name := endpointSeededExpansionDatasetName(config)
	parsed, ok := parseEndpointSeededExpansionDatasetName(name)
	require.True(t, ok)
	require.Equal(t, config, parsed)
	metadata, err := fixtureMetadata("unused", name)
	require.NoError(t, err)
	require.NotNil(t, metadata.EndpointSeededExpansion)
	require.Equal(t, int64(2), metadata.EndpointSeededExpansion.MatchingEndpoints)
	require.Equal(t, int64(3), metadata.EndpointSeededExpansion.EligiblePrefixRows)
	require.Equal(t, int64(2), metadata.EndpointSeededExpansion.ExpectedOutputTrails)
	require.Greater(t, metadata.EndpointSeededExpansion.ExpectedReverseStates, metadata.EndpointSeededExpansion.ExpectedOutputTrails)
}

// TestGeneratedEndpointSeededExpansionRejectsInvalidNames verifies that zero dimensions, padded numbers, invalid booleans, and inconsistent parallelism cannot select a generated fixture.
func TestGeneratedEndpointSeededExpansionRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{
		"generated_endpoint_seeded_expansion_v1_d0_e1_q0_w1_o0_x0_m1_c0_p0",
		"generated_endpoint_seeded_expansion_v1_d3_e0_q0_w1_o0_x0_m1_c0_p0",
		"generated_endpoint_seeded_expansion_v1_d3_e1_q0_w1_o0_x0_m0_c0_p0",
		"generated_endpoint_seeded_expansion_v1_d03_e1_q0_w1_o0_x0_m1_c0_p0",
		"generated_endpoint_seeded_expansion_v1_d3_e1_q0_w1_o0_x0_m1_c2_p0",
		"generated_endpoint_seeded_expansion_v1_d3_e1_q0_w1_o0_x0_m2_c0_p0",
	} {
		_, ok := parseEndpointSeededExpansionDatasetName(name)
		require.False(t, ok, name)
		require.Nil(t, generatedDataset(name), name)
	}
}

// TestGeneratedShortestPathV2DatasetRoundTripsAndCarriesExactExpectations verifies lossless configuration naming and exact topology metrics for branching, parallel, disconnected, cyclic, and self-loop shapes.
func TestGeneratedShortestPathV2DatasetRoundTripsAndCarriesExactExpectations(t *testing.T) {
	config := testutil.ShortestPathScaleV2Config{
		Depth:                    3,
		ForwardRootFanOut:        2,
		ReverseRootFanIn:         2,
		IntermediateFanOut:       1,
		IntermediateReverseFanIn: 4,
		FanInLevel:               2,
		ParallelKindCount:        3,
		ParallelTargetCount:      2,
		DiamondWidth:             2,
		DisconnectedWidth:        3,
		PropertyPayloadSize:      8,
		AddCycle:                 true,
		AddSelfLoop:              true,
	}
	name := shortestPathV2DatasetName(config)
	parsed, ok := parseShortestPathV2DatasetName(name)
	require.True(t, ok)
	require.Equal(t, config, parsed)

	metadata, err := fixtureMetadata("unused", name)
	require.NoError(t, err)
	require.NotNil(t, metadata.Shortest)
	require.Equal(t, 32, metadata.NodeCount)
	require.Equal(t, 33, metadata.EdgeCount)
	require.Equal(t, int64(5), metadata.Shortest.RootForwardDegree)
	require.Equal(t, int64(3), metadata.Shortest.RootReverseDegree)
	require.Equal(t, int64(2), metadata.Shortest.MaximumIntermediateForwardByLevel["2"])
	require.Equal(t, int64(5), metadata.Shortest.MaximumIntermediateReverseByLevel["2"])
	require.Equal(t, int64(23), metadata.Shortest.PhysicalTraversableEdgesByKind["Traverse"])
	require.Equal(t, int64(6), metadata.Shortest.ParallelPhysicalEdges)
	require.Equal(t, int64(2), metadata.Shortest.ParallelDistinctTargets)
	require.Equal(t, int64(3), metadata.Shortest.ExpectedMinimumDistance)
	require.Equal(t, int64(3), metadata.Shortest.ExpectedPredecessorEdges)
	require.Equal(t, int64(4), metadata.Shortest.DisconnectedStateCardinality)
	require.Equal(t, int64(5), metadata.Shortest.DistinctReachableNodesByLevel["1"])
	require.NotEmpty(t, metadata.Checksum)
}

// TestGeneratedShortestPathV2DatasetRejectsInvalidOrNonCanonicalNames verifies that inconsistent levels, empty required dimensions, padded or negative values, invalid booleans, and trailing tokens are rejected.
func TestGeneratedShortestPathV2DatasetRejectsInvalidOrNonCanonicalNames(t *testing.T) {
	for _, name := range []string{
		"generated_shortest_paths_v2_d3_o2_r2_fo1_fi4_l3_k3_t2_w2_x3_p8_c1_s1",
		"generated_shortest_paths_v2_d3_o2_r2_fo1_fi4_l2_k3_t0_w2_x3_p8_c1_s1",
		"generated_shortest_paths_v2_d03_o2_r2_fo1_fi4_l2_k3_t2_w2_x3_p8_c1_s1",
		"generated_shortest_paths_v2_d3_o2_r2_fo1_fi4_l2_k3_t2_w2_x3_p8_c2_s1",
		"generated_shortest_paths_v2_d3_o2_r2_fo1_fi4_l2_k3_t2_w2_x3_p8_c1_s1_unknown",
		"generated_shortest_paths_v2_d-1_o2_r2_fo1_fi4_l2_k3_t2_w2_x3_p8_c1_s1",
	} {
		_, ok := parseShortestPathV2DatasetName(name)
		require.False(t, ok, name)
		require.Nil(t, generatedDataset(name), name)
	}
}

// TestGeneratedFixedSuffixExpansionV2DatasetRejectsInvalidOrNonCanonicalNames verifies that impossible reachability, zero multiplicity, invalid booleans, and padded dimensions cannot identify a fixture.
func TestGeneratedFixedSuffixExpansionV2DatasetRejectsInvalidOrNonCanonicalNames(t *testing.T) {
	for _, name := range []string{
		"generated_fixed_suffix_expansion_v2_d16_f1000_r1001_x1_i0_m1_z1_p0",
		"generated_fixed_suffix_expansion_v2_d16_f1000_r1_x1_i0_m0_z1_p0",
		"generated_fixed_suffix_expansion_v2_d16_f1000_r1_x1_i0_m1_z2_p0",
		"generated_fixed_suffix_expansion_v2_d016_f1000_r1_x1_i0_m1_z1_p0",
	} {
		_, ok := parseFixedSuffixExpansionV2DatasetName(name)
		require.False(t, ok, name)
		require.Nil(t, generatedDataset(name), name)
	}
}

// TestClearGraphDeletesRelationshipsBeforeNodes verifies that cleanup removes relationships before nodes so attached edges cannot block node deletion.
func TestClearGraphDeletesRelationshipsBeforeNodes(t *testing.T) {
	database := &clearGraphTestDatabase{}

	require.NoError(t, clearGraph(context.Background(), database))
	require.Equal(t, []string{"relationships", "nodes"}, database.deletes)
}

// TestClearGraphStopsWhenRelationshipDeleteFails verifies that a relationship deletion error is wrapped and prevents the subsequent node deletion.
func TestClearGraphStopsWhenRelationshipDeleteFails(t *testing.T) {
	database := &clearGraphTestDatabase{relationshipError: errors.New("relationship failure")}

	err := clearGraph(context.Background(), database)
	require.ErrorContains(t, err, "delete relationships: relationship failure")
	require.Equal(t, []string{"relationships"}, database.deletes)
}

// TestClearGraphReportsNodeDeleteFailure verifies that cleanup reports a node deletion failure only after relationships have been removed.
func TestClearGraphReportsNodeDeleteFailure(t *testing.T) {
	database := &clearGraphTestDatabase{nodeError: errors.New("node failure")}

	err := clearGraph(context.Background(), database)
	require.ErrorContains(t, err, "delete nodes: node failure")
	require.Equal(t, []string{"relationships", "nodes"}, database.deletes)
}

// TestClearPostgresGraphTruncatesPhysicalPartitionsTogether verifies that PostgreSQL cleanup issues one parameter-free TRUNCATE for both graph-specific physical tables.
func TestClearPostgresGraphTruncatesPhysicalPartitionsTogether(t *testing.T) {
	database := &clearPostgresGraphTestDatabase{}

	require.NoError(t, clearPostgresGraph(context.Background(), database, 42))
	require.Equal(t, []string{"truncate table edge_42, node_42"}, database.statements)
	require.Equal(t, []map[string]any{nil}, database.parameters)
}

// TestClearPostgresGraphRollsBackAfterRawDeleteFailure verifies that a failed physical-table reset is surfaced from the enclosing write transaction.
func TestClearPostgresGraphRollsBackAfterRawDeleteFailure(t *testing.T) {
	database := &clearPostgresGraphTestDatabase{failAt: 1}

	err := clearPostgresGraph(context.Background(), database, 42)
	require.ErrorContains(t, err, "execute PostgreSQL graph reset")
	require.Equal(t, []string{"truncate table edge_42, node_42"}, database.statements)
}

// clearGraphTestDatabase supplies a fake transaction for graph-cleanup tests.
type clearGraphTestDatabase struct {
	// Database supplies methods irrelevant to the cleanup interaction under test.
	graph.Database

	// deletes records whether relationship or node deletion was requested first.
	deletes []string

	// relationshipError is returned when cleanup attempts relationship deletion.
	relationshipError error

	// nodeError is returned when cleanup attempts node deletion.
	nodeError error
}

// WriteTransaction routes cleanup through a transaction that shares the deletion trace and injected failures.
func (s *clearGraphTestDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&clearGraphTestTransaction{database: s})
}

// clearGraphTestTransaction routes relationship and node queries to graph-cleanup fakes.
type clearGraphTestTransaction struct {
	// Transaction supplies methods outside the cleanup query surface.
	graph.Transaction

	// database owns the call trace and injected deletion failures.
	database *clearGraphTestDatabase
}

// Relationships returns a deletion recorder backed by the owning database trace.
func (s *clearGraphTestTransaction) Relationships() graph.RelationshipQuery {
	return &clearGraphTestRelationshipQuery{database: s.database}
}

// Nodes returns a deletion recorder backed by the owning database trace.
func (s *clearGraphTestTransaction) Nodes() graph.NodeQuery {
	return &clearGraphTestNodeQuery{database: s.database}
}

// clearGraphTestRelationshipQuery records relationship deletion and injects configured failures.
type clearGraphTestRelationshipQuery struct {
	// RelationshipQuery supplies methods other than the deletion operation under test.
	graph.RelationshipQuery

	// database receives the relationship deletion trace and supplies its error.
	database *clearGraphTestDatabase
}

// Delete records the deletion request and returns the configured failure.
func (s *clearGraphTestRelationshipQuery) Delete() error {
	s.database.deletes = append(s.database.deletes, "relationships")
	return s.database.relationshipError
}

// clearGraphTestNodeQuery records node deletion and injects configured failures.
type clearGraphTestNodeQuery struct {
	// NodeQuery supplies methods other than the deletion operation under test.
	graph.NodeQuery

	// database receives the node deletion trace and supplies its error.
	database *clearGraphTestDatabase
}

// Delete records the deletion request and returns the configured failure.
func (s *clearGraphTestNodeQuery) Delete() error {
	s.database.deletes = append(s.database.deletes, "nodes")
	return s.database.nodeError
}

// clearPostgresGraphTestDatabase supplies a fake raw transaction for PostgreSQL cleanup tests.
type clearPostgresGraphTestDatabase struct {
	// Database supplies methods irrelevant to the raw cleanup interaction.
	graph.Database

	// statements records every raw SQL statement issued by cleanup.
	statements []string

	// parameters records the bind arguments paired with each captured statement.
	parameters []map[string]any

	// failAt selects the one-based raw call that returns a terminal result error.
	failAt int
}

// WriteTransaction routes PostgreSQL cleanup through a raw-SQL recorder owned by the fake database.
func (s *clearPostgresGraphTestDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	return delegate(&clearPostgresGraphTestTransaction{database: s})
}

// clearPostgresGraphTestTransaction records PostgreSQL cleanup SQL and returns a configured result.
type clearPostgresGraphTestTransaction struct {
	// Transaction supplies methods outside the raw SQL cleanup surface.
	graph.Transaction

	// database owns the captured statements, parameters, and failure injection.
	database *clearPostgresGraphTestDatabase
}

// Raw captures one statement and its parameters, injecting a terminal error on the selected call.
func (s *clearPostgresGraphTestTransaction) Raw(statement string, parameters map[string]any) graph.Result {
	s.database.statements = append(s.database.statements, statement)
	s.database.parameters = append(s.database.parameters, parameters)
	if s.database.failAt > 0 && len(s.database.statements) == s.database.failAt {
		return &clearPostgresGraphTestResult{err: errors.New("raw delete failure")}
	}

	return &clearPostgresGraphTestResult{}
}

// clearPostgresGraphTestResult exposes a configured terminal raw-statement failure to cleanup code.
type clearPostgresGraphTestResult struct {
	// Result supplies result methods that cleanup does not exercise.
	graph.Result

	// err is exposed as the terminal raw-statement failure.
	err error
}

// Error returns the configured terminal iterator error.
func (s *clearPostgresGraphTestResult) Error() error {
	return s.err
}

// Close satisfies graph.Result; this fake has no close state to record.
func (s *clearPostgresGraphTestResult) Close() {}
