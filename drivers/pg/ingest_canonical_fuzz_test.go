package pg

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func FuzzCanonicalIngest(f *testing.F) {
	for index, seed := range []string{
		`{}`,
		`{"nested":{"b":[null,true,"value"],"a":1}}`,
		`{"unicode":{"z":"escape\n\"\\","\u00e9":"東京"}}`,
		`{"array":[3,2,1,{"b":2,"a":1}]}`,
		`{"zero":[0,-0,0.0,-0.000e99]}`,
		`{"exponents":[1e131071,1e-16383,10e-1]}`,
	} {
		f.Add(seed, uint64(index+1))
	}

	f.Fuzz(func(t *testing.T, document string, orderSeed uint64) {
		decoded, ok := decodeFuzzJSONObject(document)
		if !ok {
			t.Skip()
		}

		normalized, err := normalizeIngestProperties(graph.AsProperties(decoded))
		if err != nil {
			t.Skip()
		}
		originalHash, err := hashIngestEdgeContent(normalized)
		require.NoError(t, err)

		marshaled, err := json.Marshal(normalized)
		require.NoError(t, err)
		roundTripped, ok := decodeFuzzJSONObject(string(marshaled))
		require.True(t, ok)
		roundTripHash, err := hashIngestEdgeContent(roundTripped)
		require.NoError(t, err)
		require.Equal(t, originalHash, roundTripHash)

		reordered := rebuildFuzzMapsInRandomOrder(normalized, &orderSeed).(map[string]any)
		reorderedHash, err := hashIngestEdgeContent(reordered)
		require.NoError(t, err)
		require.Equal(t, originalHash, reorderedHash)
	})
}

func decodeFuzzJSONObject(document string) (map[string]any, bool) {
	decoder := json.NewDecoder(bytes.NewBufferString(document))
	decoder.UseNumber()

	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil || decoded == nil {
		return nil, false
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}

	return decoded, true
}

func rebuildFuzzMapsInRandomOrder(value any, orderSeed *uint64) any {
	switch typedValue := value.(type) {
	case []any:
		rebuilt := make([]any, len(typedValue))
		for index, element := range typedValue {
			rebuilt[index] = rebuildFuzzMapsInRandomOrder(element, orderSeed)
		}
		return rebuilt

	case map[string]any:
		keys := make([]string, 0, len(typedValue))
		for key := range typedValue {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for index := len(keys) - 1; index > 0; index-- {
			*orderSeed ^= *orderSeed << 13
			*orderSeed ^= *orderSeed >> 7
			*orderSeed ^= *orderSeed << 17
			swapIndex := int(*orderSeed % uint64(index+1))
			keys[index], keys[swapIndex] = keys[swapIndex], keys[index]
		}

		rebuilt := make(map[string]any, len(typedValue))
		for _, key := range keys {
			rebuilt[key] = rebuildFuzzMapsInRandomOrder(typedValue[key], orderSeed)
		}
		return rebuilt

	default:
		return value
	}
}
