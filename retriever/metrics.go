package retriever

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	metricsVersion            = "retriever-metrics-v1"
	maxMetricDiffs            = 20
	metricDiffsOmittedMessage = "... additional differences omitted"
	metricsNoneKind           = "0:"
)

type MetricsManifest struct {
	Version string         `json:"version"`
	Graphs  []GraphMetrics `json:"graphs"`
}

type GraphMetrics struct {
	Name                  string           `json:"name"`
	NodeCount             int64            `json:"node_count"`
	EdgeCount             int64            `json:"edge_count"`
	NodeKindHistogram     map[string]int64 `json:"node_kind_histogram"`
	EdgeKindHistogram     map[string]int64 `json:"edge_kind_histogram"`
	InDegreeHistogram     map[string]int64 `json:"in_degree_histogram"`
	OutDegreeHistogram    map[string]int64 `json:"out_degree_histogram"`
	TotalDegreeHistogram  map[string]int64 `json:"total_degree_histogram"`
	EndpointKindHistogram map[string]int64 `json:"endpoint_kind_histogram"`
	Fingerprint           string           `json:"fingerprint"`
}

type MetricHistogramEntry struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type canonicalGraphMetrics struct {
	Name                  string                 `json:"name"`
	NodeCount             int64                  `json:"node_count"`
	EdgeCount             int64                  `json:"edge_count"`
	NodeKindHistogram     []MetricHistogramEntry `json:"node_kind_histogram"`
	EdgeKindHistogram     []MetricHistogramEntry `json:"edge_kind_histogram"`
	InDegreeHistogram     []MetricHistogramEntry `json:"in_degree_histogram"`
	OutDegreeHistogram    []MetricHistogramEntry `json:"out_degree_histogram"`
	TotalDegreeHistogram  []MetricHistogramEntry `json:"total_degree_histogram"`
	EndpointKindHistogram []MetricHistogramEntry `json:"endpoint_kind_histogram"`
}

type metricsBuilder struct {
	graphName             string
	nodeCount             int64
	edgeCount             int64
	nodeKindByID          map[string]string
	inDegreeByID          map[string]uint64
	outDegreeByID         map[string]uint64
	nodeKindHistogram     map[string]int64
	edgeKindHistogram     map[string]int64
	endpointKindHistogram map[string]int64
}

func newMetricsManifest(graphCount int) MetricsManifest {
	return MetricsManifest{
		Version: metricsVersion,
		Graphs:  make([]GraphMetrics, 0, graphCount),
	}
}

func newMetricsBuilder(graphName string, expectedNodeCount int64) *metricsBuilder {
	if expectedNodeCount < 0 {
		expectedNodeCount = 0
	}

	return &metricsBuilder{
		graphName:             graphName,
		nodeKindByID:          make(map[string]string, expectedNodeCount),
		inDegreeByID:          make(map[string]uint64, expectedNodeCount),
		outDegreeByID:         make(map[string]uint64, expectedNodeCount),
		nodeKindHistogram:     map[string]int64{},
		edgeKindHistogram:     map[string]int64{},
		endpointKindHistogram: map[string]int64{},
	}
}

func (s *metricsBuilder) observeFragmentNode(node FragmentNode) error {
	return s.observeNode(node.ID, node.Kinds)
}

func (s *metricsBuilder) observeNode(id string, kinds []string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("metrics node observation has empty ID")
	}

	if _, seen := s.nodeKindByID[id]; seen {
		return fmt.Errorf("metrics node observation has duplicate ID %q", id)
	}

	kindKey := metricKindSetKey(kinds)
	s.nodeKindByID[id] = kindKey
	s.nodeKindHistogram[kindKey]++
	s.nodeCount++

	return nil
}

func (s *metricsBuilder) observeFragmentEdge(edge FragmentEdge) error {
	return s.observeRelationship(edge.StartID, edge.EndID, edge.Kind)
}

func (s *metricsBuilder) observeRelationship(startID, endID, kind string) error {
	startKindKey, startFound := s.nodeKindByID[startID]
	endKindKey, endFound := s.nodeKindByID[endID]
	if !startFound || !endFound {
		return fmt.Errorf("metrics relationship observation references an endpoint missing from the node scan")
	}

	edgeKindKey := metricKindKey(kind)
	s.edgeKindHistogram[edgeKindKey]++
	s.endpointKindHistogram[metricEndpointKindKey(startKindKey, edgeKindKey, endKindKey)]++
	s.outDegreeByID[startID]++
	s.inDegreeByID[endID]++
	s.edgeCount++

	return nil
}

func (s *metricsBuilder) finalize() GraphMetrics {
	value := GraphMetrics{
		Name:                  s.graphName,
		NodeCount:             s.nodeCount,
		EdgeCount:             s.edgeCount,
		NodeKindHistogram:     cloneMetricHistogram(s.nodeKindHistogram),
		EdgeKindHistogram:     cloneMetricHistogram(s.edgeKindHistogram),
		InDegreeHistogram:     map[string]int64{},
		OutDegreeHistogram:    map[string]int64{},
		TotalDegreeHistogram:  map[string]int64{},
		EndpointKindHistogram: cloneMetricHistogram(s.endpointKindHistogram),
	}

	for id := range s.nodeKindByID {
		inDegree := s.inDegreeByID[id]
		outDegree := s.outDegreeByID[id]

		value.InDegreeHistogram[metricDegreeKey(inDegree)]++
		value.OutDegreeHistogram[metricDegreeKey(outDegree)]++
		value.TotalDegreeHistogram[metricDegreeKey(inDegree+outDegree)]++
	}

	value.Fingerprint = fingerprintGraphMetrics(value)

	return value
}

func fingerprintGraphMetrics(value GraphMetrics) string {
	payload, err := json.Marshal(canonicalizeGraphMetrics(value))
	if err != nil {
		panic(fmt.Sprintf("canonical graph metrics cannot be marshaled: %v", err))
	}

	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func FingerprintGraphMetrics(value GraphMetrics) string {
	return fingerprintGraphMetrics(value)
}

func canonicalizeGraphMetrics(value GraphMetrics) canonicalGraphMetrics {
	return canonicalGraphMetrics{
		Name:                  value.Name,
		NodeCount:             value.NodeCount,
		EdgeCount:             value.EdgeCount,
		NodeKindHistogram:     canonicalMetricHistogram(value.NodeKindHistogram),
		EdgeKindHistogram:     canonicalMetricHistogram(value.EdgeKindHistogram),
		InDegreeHistogram:     canonicalMetricHistogram(value.InDegreeHistogram),
		OutDegreeHistogram:    canonicalMetricHistogram(value.OutDegreeHistogram),
		TotalDegreeHistogram:  canonicalMetricHistogram(value.TotalDegreeHistogram),
		EndpointKindHistogram: canonicalMetricHistogram(value.EndpointKindHistogram),
	}
}

func canonicalMetricHistogram(histogram map[string]int64) []MetricHistogramEntry {
	keys := make([]string, 0, len(histogram))
	for key := range histogram {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	entries := make([]MetricHistogramEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, MetricHistogramEntry{
			Key:   key,
			Count: histogram[key],
		})
	}

	return entries
}

func compareMetricsManifest(expected, actual MetricsManifest) []string {
	var differences []string
	if expected.Version != actual.Version {
		differences = appendMetricDiff(differences, fmt.Sprintf("metrics.version: expected %q, actual %q", expected.Version, actual.Version))
	}

	actualByGraph := map[string]GraphMetrics{}
	for _, actualGraph := range actual.Graphs {
		actualByGraph[actualGraph.Name] = actualGraph
	}

	expectedGraphNames := make([]string, 0, len(expected.Graphs))
	expectedByGraph := map[string]struct{}{}
	for _, expectedGraph := range expected.Graphs {
		expectedGraphNames = append(expectedGraphNames, expectedGraph.Name)
		expectedByGraph[expectedGraph.Name] = struct{}{}
	}

	sort.Strings(expectedGraphNames)

	for _, graphName := range expectedGraphNames {
		expectedGraph := findGraphMetrics(expected.Graphs, graphName)
		actualGraph, ok := actualByGraph[graphName]
		if !ok {
			differences = appendMetricDiff(differences, fmt.Sprintf("graph %q missing from actual metrics", graphName))
			continue
		}

		differences = appendMetricDiffs(differences, compareGraphMetrics(expectedGraph, actualGraph)...)
	}

	actualGraphNames := make([]string, 0, len(actual.Graphs))
	for _, actualGraph := range actual.Graphs {
		if _, ok := expectedByGraph[actualGraph.Name]; !ok {
			actualGraphNames = append(actualGraphNames, actualGraph.Name)
		}
	}

	sort.Strings(actualGraphNames)

	for _, graphName := range actualGraphNames {
		differences = appendMetricDiff(differences, fmt.Sprintf("graph %q only present in actual metrics", graphName))
	}

	return differences
}

func CompareMetricsManifest(expected, actual MetricsManifest) []string {
	return compareMetricsManifest(expected, actual)
}

func compareGraphMetrics(expected, actual GraphMetrics) []string {
	var differences []string
	graphPrefix := fmt.Sprintf("graph %q", expected.Name)

	if expected.Name != actual.Name {
		differences = appendMetricDiff(differences, fmt.Sprintf("graph name: expected %q, actual %q", expected.Name, actual.Name))
	}

	if expected.NodeCount != actual.NodeCount {
		differences = appendMetricDiff(differences, fmt.Sprintf("%s node_count: expected %d, actual %d", graphPrefix, expected.NodeCount, actual.NodeCount))
	}

	if expected.EdgeCount != actual.EdgeCount {
		differences = appendMetricDiff(differences, fmt.Sprintf("%s edge_count: expected %d, actual %d", graphPrefix, expected.EdgeCount, actual.EdgeCount))
	}

	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "node_kind_histogram", expected.NodeKindHistogram, actual.NodeKindHistogram)...)
	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "edge_kind_histogram", expected.EdgeKindHistogram, actual.EdgeKindHistogram)...)
	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "in_degree_histogram", expected.InDegreeHistogram, actual.InDegreeHistogram)...)
	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "out_degree_histogram", expected.OutDegreeHistogram, actual.OutDegreeHistogram)...)
	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "total_degree_histogram", expected.TotalDegreeHistogram, actual.TotalDegreeHistogram)...)
	differences = appendMetricDiffs(differences, compareMetricHistogram(graphPrefix, "endpoint_kind_histogram", expected.EndpointKindHistogram, actual.EndpointKindHistogram)...)

	if len(differences) == 0 && expected.Fingerprint != actual.Fingerprint {
		differences = appendMetricDiff(differences, fmt.Sprintf("%s fingerprint: expected %q, actual %q", graphPrefix, expected.Fingerprint, actual.Fingerprint))
	}

	return differences
}

func compareMetricHistogram(prefix, field string, expected, actual map[string]int64) []string {
	keys := map[string]struct{}{}
	for key := range expected {
		keys[key] = struct{}{}
	}

	for key := range actual {
		keys[key] = struct{}{}
	}

	sortedKeys := make([]string, 0, len(keys))
	for key := range keys {
		sortedKeys = append(sortedKeys, key)
	}

	sort.Strings(sortedKeys)

	var differences []string
	for _, key := range sortedKeys {
		expectedCount := expected[key]
		actualCount := actual[key]

		if expectedCount != actualCount {
			differences = appendMetricDiff(differences, fmt.Sprintf("%s %s[%q]: expected %d, actual %d", prefix, field, key, expectedCount, actualCount))
		}
	}

	return differences
}

func appendMetricDiffs(target []string, values ...string) []string {
	for _, value := range values {
		target = appendMetricDiff(target, value)
	}

	return target
}

func appendMetricDiff(target []string, value string) []string {
	if len(target) > maxMetricDiffs {
		return target
	}

	if len(target) == maxMetricDiffs {
		return append(target, metricDiffsOmittedMessage)
	}

	return append(target, value)
}

func findGraphMetrics(values []GraphMetrics, graphName string) GraphMetrics {
	for _, value := range values {
		if value.Name == graphName {
			return value
		}
	}

	return GraphMetrics{}
}

func validateMetricsManifest(value MetricsManifest, graphEntries []GraphManifest) error {
	if value.Version != metricsVersion {
		return fmt.Errorf("unsupported metrics version %q", value.Version)
	}

	if len(value.Graphs) != len(graphEntries) {
		return fmt.Errorf("metrics graph count %d does not match %d manifest graph entries", len(value.Graphs), len(graphEntries))
	}

	expectedGraphs := map[string]GraphManifest{}
	for _, graphEntry := range graphEntries {
		expectedGraphs[graphEntry.Name] = graphEntry
	}

	seenGraphs := map[string]struct{}{}
	for _, graphEntry := range value.Graphs {
		if graphEntry.Name == "" {
			return fmt.Errorf("metrics graph entry has empty name")
		}

		if _, seen := seenGraphs[graphEntry.Name]; seen {
			return fmt.Errorf("metrics contains duplicate graph %q", graphEntry.Name)
		}

		seenGraphs[graphEntry.Name] = struct{}{}

		manifestGraph, expected := expectedGraphs[graphEntry.Name]
		if !expected {
			return fmt.Errorf("metrics contains graph %q not present in manifest graphs", graphEntry.Name)
		}

		if graphEntry.NodeCount != manifestGraph.NodeCount {
			return fmt.Errorf("metrics graph %q node_count %d does not match manifest node_count %d", graphEntry.Name, graphEntry.NodeCount, manifestGraph.NodeCount)
		}

		if graphEntry.EdgeCount != manifestGraph.EdgeCount {
			return fmt.Errorf("metrics graph %q edge_count %d does not match manifest edge_count %d", graphEntry.Name, graphEntry.EdgeCount, manifestGraph.EdgeCount)
		}

		if err := validateGraphMetricHistograms(graphEntry); err != nil {
			return err
		}

		if expectedFingerprint := fingerprintGraphMetrics(graphEntry); graphEntry.Fingerprint != expectedFingerprint {
			return fmt.Errorf("metrics graph %q fingerprint %q does not match computed fingerprint %q", graphEntry.Name, graphEntry.Fingerprint, expectedFingerprint)
		}
	}

	return nil
}

func ValidateMetricsManifest(value MetricsManifest, graphEntries []GraphManifest) error {
	return validateMetricsManifest(value, graphEntries)
}

func validateGraphMetricHistograms(value GraphMetrics) error {
	if value.NodeCount < 0 {
		return fmt.Errorf("metrics graph %q has negative node_count %d", value.Name, value.NodeCount)
	}

	if value.EdgeCount < 0 {
		return fmt.Errorf("metrics graph %q has negative edge_count %d", value.Name, value.EdgeCount)
	}

	for _, histogram := range []struct {
		name     string
		values   map[string]int64
		expected int64
	}{
		{
			name:     "node_kind_histogram",
			values:   value.NodeKindHistogram,
			expected: value.NodeCount,
		},
		{
			name:     "in_degree_histogram",
			values:   value.InDegreeHistogram,
			expected: value.NodeCount,
		},
		{
			name:     "out_degree_histogram",
			values:   value.OutDegreeHistogram,
			expected: value.NodeCount,
		},
		{
			name:     "total_degree_histogram",
			values:   value.TotalDegreeHistogram,
			expected: value.NodeCount,
		},
		{
			name:     "edge_kind_histogram",
			values:   value.EdgeKindHistogram,
			expected: value.EdgeCount,
		},
		{
			name:     "endpoint_kind_histogram",
			values:   value.EndpointKindHistogram,
			expected: value.EdgeCount,
		},
	} {
		if err := validateMetricHistogramSum(value.Name, histogram.name, histogram.values, histogram.expected); err != nil {
			return err
		}
	}

	return nil
}

func validateMetricHistogramSum(graphName string, histogramName string, histogram map[string]int64, expected int64) error {
	var total int64
	for key, count := range histogram {
		if count < 0 {
			return fmt.Errorf("metrics graph %q %s[%q] has negative count %d", graphName, histogramName, key, count)
		}

		total += count
	}

	if total != expected {
		return fmt.Errorf("metrics graph %q %s total %d does not match expected count %d", graphName, histogramName, total, expected)
	}

	return nil
}

func cloneMetricHistogram(source map[string]int64) map[string]int64 {
	target := make(map[string]int64, len(source))
	for key, count := range source {
		target[key] = count
	}

	return target
}

func metricKindSetKey(kinds []string) string {
	seen := map[string]struct{}{}
	for _, kind := range kinds {
		if kind != "" {
			seen[kind] = struct{}{}
		}
	}

	values := make([]string, 0, len(seen))
	for kind := range seen {
		values = append(values, kind)
	}

	if len(values) == 0 {
		return metricsNoneKind
	}

	sort.Strings(values)

	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, metricKeyPart(value))
	}

	return strings.Join(parts, "+")
}

func metricKindKey(kind string) string {
	if kind == "" {
		return metricsNoneKind
	}

	return metricKeyPart(kind)
}

func metricEndpointKindKey(startKindKey, edgeKindKey, endKindKey string) string {
	return strings.Join([]string{
		metricKeyPart(startKindKey),
		metricKeyPart(edgeKindKey),
		metricKeyPart(endKindKey),
	}, "|")
}

func metricDegreeKey(degree uint64) string {
	return strconv.FormatUint(degree, 10)
}

func metricKeyPart(value string) string {
	return strconv.Itoa(len(value)) + ":" + value
}
