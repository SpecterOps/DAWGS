package retriever

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMetricsBuilderFinalizesGraphShape(t *testing.T) {
	builder := newMetricsBuilder("default", 3)
	for _, node := range []FragmentNode{
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

	for _, edge := range []FragmentEdge{
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
	first := buildNamedFingerprintFixture(t, "default", []FragmentNode{
		{ID: "a", Kinds: []string{"User", "Person"}},
		{ID: "b", Kinds: []string{"Group"}},
	}, []FragmentEdge{
		{StartID: "a", EndID: "b", Kind: "MemberOf"},
	})
	second := buildNamedFingerprintFixture(t, "default", []FragmentNode{
		{ID: "b", Kinds: []string{"Group"}},
		{ID: "a", Kinds: []string{"Person", "User"}},
	}, []FragmentEdge{
		{StartID: "a", EndID: "b", Kind: "MemberOf"},
	})

	if first.Fingerprint != second.Fingerprint {
		t.Fatalf("fingerprint changed with observation order: %q != %q", first.Fingerprint, second.Fingerprint)
	}
}

func TestMetricKindSetKeyDeduplicatesKinds(t *testing.T) {
	once := metricKindSetKey([]string{"User", "Person"})
	duplicated := metricKindSetKey([]string{"Person", "User", "User"})
	if once != duplicated {
		t.Fatalf("duplicate kind changed kind set key: %q != %q", once, duplicated)
	}
}

func TestMetricsIgnorePropertiesAndIDsInFinalForm(t *testing.T) {
	builder := newMetricsBuilder("default", 1)
	if err := builder.observeFragmentNode(FragmentNode{
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
	expected := buildNamedFingerprintFixture(t, "default", []FragmentNode{
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

func TestMetricsManifestComparisonReportsGraphSetDiffs(t *testing.T) {
	expected := MetricsManifest{
		Version: metricsVersion,
		Graphs: []GraphMetrics{
			buildNamedFingerprintFixture(t, "b", []FragmentNode{{ID: "b", Kinds: []string{"User"}}}, nil),
			buildNamedFingerprintFixture(t, "a", []FragmentNode{{ID: "a", Kinds: []string{"User"}}}, nil),
		},
	}
	actual := MetricsManifest{
		Version: "future",
		Graphs: []GraphMetrics{
			buildNamedFingerprintFixture(t, "c", []FragmentNode{{ID: "c", Kinds: []string{"User"}}}, nil),
		},
	}

	differences := compareMetricsManifest(expected, actual)

	for _, expectedDiff := range []string{
		`metrics.version: expected "retriever-metrics-v1", actual "future"`,
		`graph "a" missing from actual metrics`,
		`graph "b" missing from actual metrics`,
		`graph "c" only present in actual metrics`,
	} {
		if !containsExact(differences, expectedDiff) {
			t.Fatalf("missing diff %q in %v", expectedDiff, differences)
		}
	}
}

func TestPublicMetricsHelpers(t *testing.T) {
	expectedGraph := buildNamedFingerprintFixture(t, "default", []FragmentNode{{ID: "a", Kinds: []string{"User"}}}, nil)
	expected := MetricsManifest{
		Version: metricsVersion,
		Graphs:  []GraphMetrics{expectedGraph},
	}
	actualGraph := expectedGraph
	actualGraph.NodeCount = 2
	actualGraph.Fingerprint = FingerprintGraphMetrics(actualGraph)
	actual := MetricsManifest{
		Version: metricsVersion,
		Graphs:  []GraphMetrics{actualGraph},
	}

	differences := CompareMetricsManifest(expected, actual)
	if len(differences) == 0 {
		t.Fatalf("expected public compare helper to report differences")
	}

	if err := ValidateMetricsManifest(expected, []GraphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 0,
	}}); err != nil {
		t.Fatalf("validate metrics through public helper: %v", err)
	}
}

func TestAppendMetricDiffReportsTruncation(t *testing.T) {
	var differences []string
	for i := 0; i < maxMetricDiffs+5; i++ {
		differences = appendMetricDiff(differences, "diff")
	}

	if len(differences) != maxMetricDiffs+1 {
		t.Fatalf("diff count = %d", len(differences))
	}

	if differences[len(differences)-1] != metricDiffsOmittedMessage {
		t.Fatalf("missing omitted marker: %v", differences)
	}
}

func TestMetricsValidationRejectsMismatchedFingerprint(t *testing.T) {
	metrics := buildNamedFingerprintFixture(t, "default", []FragmentNode{
		{ID: "a", Kinds: []string{"User"}},
	}, nil)
	metrics.Fingerprint = "sha256:bad"

	err := validateMetricsManifest(MetricsManifest{
		Version: metricsVersion,
		Graphs:  []GraphMetrics{metrics},
	}, []GraphManifest{{
		Name:      "default",
		NodeCount: 1,
		EdgeCount: 0,
	}})
	if err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("expected fingerprint validation error, got %v", err)
	}
}

func TestMetricsValidationRejectsHistogramInvariantViolations(t *testing.T) {
	base := buildNamedFingerprintFixture(t, "default", []FragmentNode{
		{ID: "a", Kinds: []string{"User"}},
		{ID: "b", Kinds: []string{"Group"}},
	}, []FragmentEdge{
		{StartID: "a", EndID: "b", Kind: "MemberOf"},
	})

	cases := map[string]func(GraphMetrics) GraphMetrics{
		"node kind sum mismatch": func(value GraphMetrics) GraphMetrics {
			value.NodeKindHistogram = map[string]int64{
				metricKindSetKey([]string{"User"}): 1,
			}
			value.Fingerprint = fingerprintGraphMetrics(value)
			return value
		},
		"edge kind sum mismatch": func(value GraphMetrics) GraphMetrics {
			value.EdgeKindHistogram = map[string]int64{}
			value.Fingerprint = fingerprintGraphMetrics(value)
			return value
		},
		"negative count": func(value GraphMetrics) GraphMetrics {
			value.OutDegreeHistogram = map[string]int64{
				metricDegreeKey(0): -1,
				metricDegreeKey(1): 3,
			}
			value.Fingerprint = fingerprintGraphMetrics(value)
			return value
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateMetricsManifest(MetricsManifest{
				Version: metricsVersion,
				Graphs:  []GraphMetrics{mutate(base)},
			}, []GraphManifest{{
				Name:      "default",
				NodeCount: 2,
				EdgeCount: 1,
			}})
			if err == nil {
				t.Fatalf("expected invariant validation error")
			}
		})
	}
}

func buildFingerprintFixture(t *testing.T, nodes []FragmentNode, edges []FragmentEdge) GraphMetrics {
	t.Helper()
	return buildNamedFingerprintFixture(t, "default", nodes, edges)
}

func buildNamedFingerprintFixture(t *testing.T, graphName string, nodes []FragmentNode, edges []FragmentEdge) GraphMetrics {
	t.Helper()

	builder := newMetricsBuilder(graphName, int64(len(nodes)))
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

func containsExact(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}

	return false
}
