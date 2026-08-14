package metrics_test

import (
	"testing"

	"github.com/specterops/dawgs/ret/metrics"
	"github.com/stretchr/testify/require"
)

func TestCompareReportsEveryChangedCategoryDeterministically(t *testing.T) {
	expected := metrics.GraphMetrics{
		NodeCount:               1,
		RelationshipCount:       2,
		NodeKindSequences:       map[string]int64{"a": 1},
		RelationshipKinds:       map[string]int64{"A": 2},
		InboundDegreeHistogram:  map[string]int64{"0": 1},
		OutboundDegreeHistogram: map[string]int64{"0": 1},
		EndpointShapeHistogram:  map[string]int64{"shape-a": 2},
		Fingerprint:             "a",
	}
	actual := metrics.GraphMetrics{
		NodeCount:               3,
		RelationshipCount:       4,
		NodeKindSequences:       map[string]int64{"z": 3},
		RelationshipKinds:       map[string]int64{"Z": 4},
		InboundDegreeHistogram:  map[string]int64{"1": 3},
		OutboundDegreeHistogram: map[string]int64{"1": 3},
		EndpointShapeHistogram:  map[string]int64{"shape-z": 4},
		Fingerprint:             "b",
	}

	err := metrics.Compare(expected, actual)

	require.EqualError(t, err, "graph metrics differ:\n"+
		"node count: expected 1, actual 3\n"+
		"relationship count: expected 2, actual 4\n"+
		"node kind sequences[\"a\"]: expected 1, actual 0\n"+
		"node kind sequences[\"z\"]: expected 0, actual 3\n"+
		"relationship kinds[\"A\"]: expected 2, actual 0\n"+
		"relationship kinds[\"Z\"]: expected 0, actual 4\n"+
		"inbound degree histogram[\"0\"]: expected 1, actual 0\n"+
		"inbound degree histogram[\"1\"]: expected 0, actual 3\n"+
		"outbound degree histogram[\"0\"]: expected 1, actual 0\n"+
		"outbound degree histogram[\"1\"]: expected 0, actual 3\n"+
		"endpoint shape histogram[\"shape-a\"]: expected 2, actual 0\n"+
		"endpoint shape histogram[\"shape-z\"]: expected 0, actual 4\n"+
		"fingerprint: expected \"a\", actual \"b\"")
}

func TestCompareReturnsNilForEqualMetrics(t *testing.T) {
	value := metrics.GraphMetrics{NodeCount: 1, Fingerprint: "sha256:abc"}

	require.NoError(t, metrics.Compare(value, value))
}
