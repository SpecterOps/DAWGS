package metrics

import (
	"fmt"
	"sort"
	"strings"
)

// Compare returns all deterministic differences between two graph metrics.
func Compare(expected, actual GraphMetrics) error {
	differences := make([]string, 0)
	if expected.NodeCount != actual.NodeCount {
		differences = append(differences, fmt.Sprintf("node count: expected %d, actual %d", expected.NodeCount, actual.NodeCount))
	}
	if expected.RelationshipCount != actual.RelationshipCount {
		differences = append(differences, fmt.Sprintf("relationship count: expected %d, actual %d", expected.RelationshipCount, actual.RelationshipCount))
	}

	differences = append(differences, compareHistogram("node kind sequences", expected.NodeKindSequences, actual.NodeKindSequences)...)
	differences = append(differences, compareHistogram("relationship kinds", expected.RelationshipKinds, actual.RelationshipKinds)...)
	differences = append(differences, compareHistogram("inbound degree histogram", expected.InboundDegreeHistogram, actual.InboundDegreeHistogram)...)
	differences = append(differences, compareHistogram("outbound degree histogram", expected.OutboundDegreeHistogram, actual.OutboundDegreeHistogram)...)
	differences = append(differences, compareHistogram("endpoint shape histogram", expected.EndpointShapeHistogram, actual.EndpointShapeHistogram)...)

	if expected.Fingerprint != actual.Fingerprint {
		differences = append(differences, fmt.Sprintf("fingerprint: expected %q, actual %q", expected.Fingerprint, actual.Fingerprint))
	}
	if len(differences) == 0 {
		return nil
	}

	return fmt.Errorf("graph metrics differ:\n%s", strings.Join(differences, "\n"))
}

func compareHistogram(name string, expected, actual map[string]int64) []string {
	keys := make(map[string]struct{}, len(expected)+len(actual))
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

	differences := make([]string, 0)
	for _, key := range sortedKeys {
		if expected[key] != actual[key] {
			differences = append(differences, fmt.Sprintf("%s[%q]: expected %d, actual %d", name, key, expected[key], actual[key]))
		}
	}

	return differences
}
