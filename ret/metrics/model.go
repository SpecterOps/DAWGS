// Package metrics aggregates format-neutral graph shape metrics.
package metrics

// GraphMetrics describes the observable graph shape without source identifiers
// or entity properties.
type GraphMetrics struct {
	NodeCount               int64            `json:"node_count"`
	RelationshipCount       int64            `json:"relationship_count"`
	NodeKindSequences       map[string]int64 `json:"node_kind_sequences"`
	RelationshipKinds       map[string]int64 `json:"relationship_kinds"`
	InboundDegreeHistogram  map[string]int64 `json:"inbound_degree_histogram"`
	OutboundDegreeHistogram map[string]int64 `json:"outbound_degree_histogram"`
	EndpointShapeHistogram  map[string]int64 `json:"endpoint_shape_histogram"`
	Fingerprint             string           `json:"fingerprint"`
}

type metricHistogramEntry struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type canonicalGraphMetrics struct {
	NodeCount               int64                  `json:"node_count"`
	RelationshipCount       int64                  `json:"relationship_count"`
	NodeKindSequences       []metricHistogramEntry `json:"node_kind_sequences"`
	RelationshipKinds       []metricHistogramEntry `json:"relationship_kinds"`
	InboundDegreeHistogram  []metricHistogramEntry `json:"inbound_degree_histogram"`
	OutboundDegreeHistogram []metricHistogramEntry `json:"outbound_degree_histogram"`
	EndpointShapeHistogram  []metricHistogramEntry `json:"endpoint_shape_histogram"`
}
