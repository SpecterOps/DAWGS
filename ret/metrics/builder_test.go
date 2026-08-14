package metrics_test

import (
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/ret/entity"
	"github.com/specterops/dawgs/ret/metrics"
	"github.com/stretchr/testify/require"
)

func TestOrderedKindsKeyPreservesOrderMultiplicityAndBoundaries(t *testing.T) {
	first := metrics.OrderedKindsKey([]string{"A", "B", "A"})
	second := metrics.OrderedKindsKey([]string{"A", "A", "B"})

	require.Equal(t, "1:A1:B1:A", first)
	require.Equal(t, "1:A1:A1:B", second)
	require.NotEqual(t, first, second)
	require.NotEqual(t, metrics.OrderedKindsKey([]string{"12", "3"}), metrics.OrderedKindsKey([]string{"1", "23"}))
}

func TestOrderedKindsRemainDistinct(t *testing.T) {
	builder := metrics.NewBuilder()
	require.NoError(t, builder.ObserveNode(entity.Node{SourceID: "1", Kinds: []string{"A", "B", "A"}}))
	require.NoError(t, builder.ObserveNode(entity.Node{SourceID: "2", Kinds: []string{"A", "A", "B"}}))

	got := builder.Finalize()

	require.EqualValues(t, 1, got.NodeKindSequences[metrics.OrderedKindsKey([]string{"A", "B", "A"})])
	require.EqualValues(t, 1, got.NodeKindSequences[metrics.OrderedKindsKey([]string{"A", "A", "B"})])
}

func TestBuilderAggregatesGraphShapeIncludingIsolatedNodeDegrees(t *testing.T) {
	builder := metrics.NewBuilder()
	for _, node := range []entity.Node{
		{SourceID: "user", Kinds: []string{"User", "Person"}},
		{SourceID: "group", Kinds: []string{"Group"}},
		{SourceID: "isolated"},
	} {
		require.NoError(t, builder.ObserveNode(node))
	}
	for _, relationship := range []entity.Relationship{
		{SourceID: "member", StartID: "user", EndID: "group", Kind: "MEMBER_OF"},
		{SourceID: "admin", StartID: "group", EndID: "user", Kind: "ADMIN_TO"},
	} {
		require.NoError(t, builder.ObserveRelationship(relationship))
	}

	got := builder.Finalize()

	user := metrics.OrderedKindsKey([]string{"User", "Person"})
	group := metrics.OrderedKindsKey([]string{"Group"})
	empty := metrics.OrderedKindsKey(nil)
	require.EqualValues(t, 3, got.NodeCount)
	require.EqualValues(t, 2, got.RelationshipCount)
	require.Equal(t, map[string]int64{user: 1, group: 1, empty: 1}, got.NodeKindSequences)
	require.Equal(t, map[string]int64{"MEMBER_OF": 1, "ADMIN_TO": 1}, got.RelationshipKinds)
	require.Equal(t, map[string]int64{"0": 1, "1": 2}, got.InboundDegreeHistogram)
	require.Equal(t, map[string]int64{"0": 1, "1": 2}, got.OutboundDegreeHistogram)
	require.Equal(t, map[string]int64{
		metrics.OrderedKindsKey([]string{user, "MEMBER_OF", group}): 1,
		metrics.OrderedKindsKey([]string{group, "ADMIN_TO", user}):  1,
	}, got.EndpointShapeHistogram)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.Fingerprint)
}

func TestBuilderRejectsInvalidNodesDuplicateIDsAndMissingRelationshipEndpoints(t *testing.T) {
	builder := metrics.NewBuilder()
	require.Error(t, builder.ObserveNode(entity.Node{}))
	require.NoError(t, builder.ObserveNode(entity.Node{SourceID: "present"}))
	require.ErrorContains(t, builder.ObserveNode(entity.Node{SourceID: "present"}), "duplicate")
	require.ErrorContains(t, builder.ObserveRelationship(entity.Relationship{StartID: "present", EndID: "missing", Kind: "KNOWS"}), "missing endpoint")
	require.Error(t, builder.ObserveRelationship(entity.Relationship{StartID: "present", EndID: "present"}))
}

func TestFingerprintIsStableForEquivalentGraphsRegardlessOfObservationOrder(t *testing.T) {
	first := buildGraph(t, []entity.Node{
		{SourceID: "one", Kinds: []string{"User", "Person"}},
		{SourceID: "two", Kinds: []string{"Group"}},
	}, []entity.Relationship{
		{StartID: "one", EndID: "two", Kind: "MEMBER_OF"},
		{StartID: "two", EndID: "one", Kind: "ADMIN_TO"},
	})
	second := buildGraph(t, []entity.Node{
		{SourceID: "two", Kinds: []string{"Group"}},
		{SourceID: "one", Kinds: []string{"User", "Person"}},
	}, []entity.Relationship{
		{StartID: "two", EndID: "one", Kind: "ADMIN_TO"},
		{StartID: "one", EndID: "two", Kind: "MEMBER_OF"},
	})

	require.Equal(t, first, second)
}

func TestMetricsExcludeSourceIDsAndProperties(t *testing.T) {
	first := buildGraph(t, []entity.Node{{
		SourceID:   "node-secret-one",
		Kinds:      []string{"User"},
		Properties: map[string]any{"email": "ada@example.test"},
	}}, nil)
	second := buildGraph(t, []entity.Node{{
		SourceID:   "node-secret-two",
		Kinds:      []string{"User"},
		Properties: map[string]any{"email": "grace@example.test"},
	}}, nil)

	payload, err := json.Marshal(first)
	require.NoError(t, err)
	require.Equal(t, first.Fingerprint, second.Fingerprint)
	require.NotContains(t, string(payload), "node-secret-one")
	require.NotContains(t, string(payload), "ada@example.test")
}

func TestFinalizeReturnsIndependentMetrics(t *testing.T) {
	builder := metrics.NewBuilder()
	require.NoError(t, builder.ObserveNode(entity.Node{SourceID: "one", Kinds: []string{"User"}}))

	first := builder.Finalize()
	originalFingerprint := first.Fingerprint
	first.NodeKindSequences[metrics.OrderedKindsKey([]string{"User"})] = 99
	second := builder.Finalize()

	require.EqualValues(t, 1, second.NodeKindSequences[metrics.OrderedKindsKey([]string{"User"})])
	require.Equal(t, originalFingerprint, second.Fingerprint)
}

func buildGraph(t *testing.T, nodes []entity.Node, relationships []entity.Relationship) metrics.GraphMetrics {
	t.Helper()
	builder := metrics.NewBuilder()
	for _, node := range nodes {
		require.NoError(t, builder.ObserveNode(node))
	}
	for _, relationship := range relationships {
		require.NoError(t, builder.ObserveRelationship(relationship))
	}
	return builder.Finalize()
}
