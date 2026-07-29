package metrics

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

func fingerprint(value GraphMetrics) string {
	payload, err := json.Marshal(canonicalize(value))
	if err != nil {
		panic(fmt.Sprintf("canonical graph metrics cannot be marshaled: %v", err))
	}

	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func canonicalize(value GraphMetrics) canonicalGraphMetrics {
	return canonicalGraphMetrics{
		NodeCount:               value.NodeCount,
		RelationshipCount:       value.RelationshipCount,
		NodeKindSequences:       canonicalHistogram(value.NodeKindSequences),
		RelationshipKinds:       canonicalHistogram(value.RelationshipKinds),
		InboundDegreeHistogram:  canonicalHistogram(value.InboundDegreeHistogram),
		OutboundDegreeHistogram: canonicalHistogram(value.OutboundDegreeHistogram),
		EndpointShapeHistogram:  canonicalHistogram(value.EndpointShapeHistogram),
	}
}

func canonicalHistogram(histogram map[string]int64) []metricHistogramEntry {
	keys := make([]string, 0, len(histogram))
	for key := range histogram {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]metricHistogramEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, metricHistogramEntry{Key: key, Count: histogram[key]})
	}

	return entries
}
