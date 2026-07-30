package scrub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActionCountsAddAndCombine(t *testing.T) {
	left := ActionCounts{
		Preserve:     1,
		Pseudonymize: 2,
	}
	right := ActionCounts{
		Preserve:       3,
		Redact:         4,
		ShiftTimestamp: 5,
	}

	combined := left.Combine(right)

	require.Equal(t, ActionCounts{
		Preserve:       4,
		Pseudonymize:   2,
		Redact:         4,
		ShiftTimestamp: 5,
	}, combined)
	require.Equal(t, ActionCounts{
		Preserve:     1,
		Pseudonymize: 2,
	}, left)

	left.Add(right)
	require.Equal(t, combined, left)
}

func TestActionCountsTotalAndIsZero(t *testing.T) {
	require.True(t, (ActionCounts{}).IsZero())
	require.Zero(t, (ActionCounts{}).Total())

	counts := ActionCounts{
		Preserve:       1,
		Pseudonymize:   2,
		Redact:         3,
		ShiftTimestamp: 4,
	}
	require.False(t, counts.IsZero())
	require.EqualValues(t, 10, counts.Total())
}

func TestActionCountsJSONUsesNamedFieldsAndOmitsZeros(t *testing.T) {
	payload, err := json.Marshal(ActionCounts{
		Pseudonymize:   2,
		ShiftTimestamp: 1,
	})

	require.NoError(t, err)
	require.JSONEq(t, `{"pseudonymize":2,"shift_timestamp":1}`, string(payload))

	var decoded ActionCounts
	require.NoError(t, json.Unmarshal(
		[]byte(`{"preserve":3,"pseudonymize":4,"redact":5,"shift_timestamp":6}`),
		&decoded,
	))
	require.Equal(t, ActionCounts{
		Preserve:       3,
		Pseudonymize:   4,
		Redact:         5,
		ShiftTimestamp: 6,
	}, decoded)
}
