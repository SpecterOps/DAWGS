package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMetricsBuilderFinalizesGraphShape(t *testing.T) {
	builder := newMetricsBuilder("default", 3)
	for _, node := range []fragmentNode{
		{
			ID:    "node-secret-1",
			Kinds: []string{"User"},
			Properties: map[string]any{
				"objectid": "S-1-5-21-secret",
				"name":     "alice",
			},
		},
		{
			ID:    "node-secret-2",
			Kinds: []string{"Group"},
		},
		{
			ID: "node-secret-3",
		},
	} {
		if err := builder.observeFragmentNode(node); err != nil {
			t.Fatalf("observe node: %v", err)
		}
	}

	for _, edge := range []fragmentEdge{
		{
			StartID: "node-secret-1",
			EndID:   "node-secret-2",
			Kind:    "MemberOf",
			Properties: map[string]any{
				"source": "secret-source",
			},
		},
		{
			StartID: "node-secret-2",
			EndID:   "node-secret-1",
			Kind:    "AdminTo",
		},
	} {
		if err := builder.observeFragmentEdge(edge); err != nil {
			t.Fatalf("observe edge: %v", err)
		}
	}

	metrics := builder.finalize()
	if metrics.Name != "default" {
		t.Fatalf("graph name = %q", metrics.Name)
	}
	if metrics.NodeCount != 3 {
		t.Fatalf("node count = %d", metrics.NodeCount)
	}
	if metrics.EdgeCount != 2 {
		t.Fatalf("edge count = %d", metrics.EdgeCount)
	}
	if got := metrics.NodeKindHistogram[metricKindSetKey([]string{"User"})]; got != 1 {
		t.Fatalf("user node kind count = %d", got)
	}
	if got := metrics.NodeKindHistogram[metricKindSetKey([]string{"Group"})]; got != 1 {
		t.Fatalf("group node kind count = %d", got)
	}
	if got := metrics.NodeKindHistogram[metricKindSetKey(nil)]; got != 1 {
		t.Fatalf("empty node kind count = %d", got)
	}
	if got := metrics.EdgeKindHistogram[metricKindKey("MemberOf")]; got != 1 {
		t.Fatalf("member edge kind count = %d", got)
	}
	if got := metrics.EdgeKindHistogram[metricKindKey("AdminTo")]; got != 1 {
		t.Fatalf("admin edge kind count = %d", got)
	}
	if got := metrics.InDegreeHistogram[metricDegreeKey(0)]; got != 1 {
		t.Fatalf("zero in-degree count = %d", got)
	}
	if got := metrics.OutDegreeHistogram[metricDegreeKey(0)]; got != 1 {
		t.Fatalf("zero out-degree count = %d", got)
	}
	if got := metrics.TotalDegreeHistogram[metricDegreeKey(0)]; got != 1 {
		t.Fatalf("zero total-degree count = %d", got)
	}
	if got := metrics.TotalDegreeHistogram[metricDegreeKey(2)]; got != 2 {
		t.Fatalf("total-degree two count = %d", got)
	}

	endpointKey := metricEndpointKindKey(metricKindSetKey([]string{"User"}), metricKindKey("MemberOf"), metricKindSetKey([]string{"Group"}))
	if got := metrics.EndpointKindHistogram[endpointKey]; got != 1 {
		t.Fatalf("endpoint kind count = %d", got)
	}
	if metrics.Fingerprint == "" || !strings.HasPrefix(metrics.Fingerprint, "sha256:") {
		t.Fatalf("missing fingerprint: %q", metrics.Fingerprint)
	}
}

func TestMetricsFingerprintStableAcrossObservationOrder(t *testing.T) {
	first := buildFingerprintFixture(t, []fragmentNode{
		{ID: "a", Kinds: []string{"User", "Person"}},
		{ID: "b", Kinds: []string{"Group"}},
	}, []fragmentEdge{
		{StartID: "a", EndID: "b", Kind: "MemberOf"},
	})
	second := buildFingerprintFixture(t, []fragmentNode{
		{ID: "b", Kinds: []string{"Group"}},
		{ID: "a", Kinds: []string{"Person", "User"}},
	}, []fragmentEdge{
		{StartID: "a", EndID: "b", Kind: "MemberOf"},
	})

	if first.Fingerprint != second.Fingerprint {
		t.Fatalf("fingerprint changed with observation order: %q != %q", first.Fingerprint, second.Fingerprint)
	}
}

func TestMetricsIgnorePropertiesAndIDsInFinalForm(t *testing.T) {
	builder := newMetricsBuilder("default", 1)
	if err := builder.observeFragmentNode(fragmentNode{
		ID:    "node-secret-id",
		Kinds: []string{"User"},
		Properties: map[string]any{
			"objectid": "S-1-5-21-secret",
			"name":     "alice@example.test",
		},
	}); err != nil {
		t.Fatalf("observe node: %v", err)
	}

	metrics := builder.finalize()
	payload, err := json.Marshal(metrics)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}
	serialized := string(payload)
	for _, forbidden := range []string{"node-secret-id", "objectid", "S-1-5-21-secret", "alice@example.test"} {
		if strings.Contains(serialized, forbidden) {
			t.Fatalf("serialized metrics contains attribution %q: %s", forbidden, serialized)
		}
	}
}

func TestMetricsComparisonReportsDeterministicDiffs(t *testing.T) {
	expected := buildFingerprintFixture(t, []fragmentNode{
		{ID: "a", Kinds: []string{"User"}},
	}, nil)
	actual := expected
	actual.NodeCount = 2
	actual.NodeKindHistogram = map[string]int64{
		metricKindSetKey([]string{"Group"}): 1,
		metricKindSetKey([]string{"User"}):  1,
	}
	actual.Fingerprint = fingerprintGraphMetrics(actual)

	differences := compareGraphMetrics(expected, actual)
	if len(differences) != 2 {
		t.Fatalf("diff count = %d: %v", len(differences), differences)
	}
	if !strings.Contains(differences[0], "node_count") {
		t.Fatalf("first diff should be node count, got %q", differences[0])
	}
	if !strings.Contains(differences[1], "node_kind_histogram") {
		t.Fatalf("second diff should be kind histogram, got %q", differences[1])
	}
}

func TestMetricsValidationRejectsMismatchedFingerprint(t *testing.T) {
	metrics := buildFingerprintFixture(t, []fragmentNode{
		{ID: "a", Kinds: []string{"User"}},
	}, nil)
	metrics.Fingerprint = "sha256:bad"

	err := validateMetricsManifest(metricsManifest{
		Version: metricsVersion,
		Graphs:  []graphMetrics{metrics},
	}, []graphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 0,
	}})
	if err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("expected fingerprint validation error, got %v", err)
	}
}

func buildFingerprintFixture(t *testing.T, nodes []fragmentNode, edges []fragmentEdge) graphMetrics {
	t.Helper()

	builder := newMetricsBuilder("default", int64(len(nodes)))
	for _, node := range nodes {
		if err := builder.observeFragmentNode(node); err != nil {
			t.Fatalf("observe node: %v", err)
		}
	}
	for _, edge := range edges {
		if err := builder.observeFragmentEdge(edge); err != nil {
			t.Fatalf("observe edge: %v", err)
		}
	}
	return builder.finalize()
}
