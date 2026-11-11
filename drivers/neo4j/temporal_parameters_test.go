package neo4j

import (
	"reflect"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

func TestNormalizeTemporalParameters(t *testing.T) {
	for _, input := range []string{"2026-01-02T03:04:05.123456789+00:00", "2026-01-02T04:04:05.123456789+01:00", "2026-01-01T21:34:05.123456789-05:30"} {
		t.Run(input, func(t *testing.T) {
			timestamp, err := time.Parse(time.RFC3339Nano, input)
			require.NoError(t, err)
			// Parsing can reuse time.Local when its offset matches the input.
			// Construct an unnamed location explicitly to isolate this regression.
			_, inputOffset := timestamp.Zone()
			timestamp = timestamp.In(time.FixedZone("", inputOffset))
			parameters := map[string]any{
				"scalar": timestamp,
				"nested": []any{map[string]any{"time": timestamp}, nil},
				"typed":  []time.Time{timestamp},
				"maps":   []map[string]time.Time{{"time": timestamp}},
				"array":  [1]time.Time{timestamp},
			}
			normalized := normalizeTemporalParameters(parameters)
			for _, actual := range []time.Time{
				normalized["scalar"].(time.Time),
				normalized["nested"].([]any)[0].(map[string]any)["time"].(time.Time),
				normalized["typed"].([]time.Time)[0],
				normalized["maps"].([]map[string]time.Time)[0]["time"],
				normalized["array"].([1]time.Time)[0],
			} {
				require.True(t, timestamp.Equal(actual))
				require.Equal(t, timestamp.Nanosecond(), actual.Nanosecond())
				_, expectedOffset := timestamp.Zone()
				zone, offset := actual.Zone()
				require.Equal(t, "Offset", zone)
				require.Equal(t, expectedOffset, offset)
			}
			require.Nil(t, normalized["nested"].([]any)[1])
			require.Equal(t, timestamp, parameters["scalar"])
			require.Equal(t, timestamp, parameters["nested"].([]any)[0].(map[string]any)["time"])
			require.Equal(t, timestamp, parameters["typed"].([]time.Time)[0])
			require.Equal(t, timestamp, parameters["maps"].([]map[string]time.Time)[0]["time"])
		})
	}
}

func TestNormalizeTemporalParametersPreservesOtherValues(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	parameters := map[string]any{
		"utc": now, "named": now.In(time.FixedZone("Europe/Paris", 3600)),
		"offset": now.In(time.FixedZone("Offset", 3600)),
		"date":   dbtype.Date(now), "local": dbtype.LocalDateTime(now),
		"null": nil, "nil slice": []time.Time(nil), "nil map": map[string]any(nil),
		"bytes": []byte("unchanged"), "ids": []int64{1, 2}, "strings": map[string]string{"key": "value"}, "list": []any{1, "text", true},
	}
	normalized := normalizeTemporalParameters(parameters)
	require.Equal(t, parameters, normalized)
	require.Equal(t, reflect.ValueOf(parameters).Pointer(), reflect.ValueOf(normalized).Pointer())
	require.Nil(t, normalizeTemporalParameters(nil))
}
