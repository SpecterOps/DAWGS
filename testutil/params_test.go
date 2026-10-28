// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParamsDecodesTaggedDatetime verifies tagged datetime values are parsed
// recursively with nanosecond precision.
func TestParamsDecodesTaggedDatetime(t *testing.T) {
	var values Params
	require.NoError(t, json.Unmarshal([]byte(`{
		"threshold": {"$type": "datetime", "value": "2026-01-02T03:04:05.123456789Z"},
		"nested": [{"$type": "datetime", "value": "2025-02-03T04:05:06Z"}]
	}`), &values))

	offset := time.FixedZone("Offset", 0)
	require.Equal(t, time.Date(2026, time.January, 2, 3, 4, 5, 123456789, offset), values["threshold"])
	require.Equal(t, []any{time.Date(2025, time.February, 3, 4, 5, 6, 0, offset)}, values["nested"])
}

func TestParamsDatetimeNormalizesOffsetsToUTC(t *testing.T) {
	for _, text := range []string{
		"2026-01-02T03:04:05.123456789Z",
		"2026-01-02T04:04:05.123456789+01:00",
		"2026-01-01T21:34:05.123456789-05:30",
	} {
		t.Run(text, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"threshold": map[string]any{"$type": "datetime", "value": text}})
			require.NoError(t, err)
			var values Params
			require.NoError(t, json.Unmarshal(raw, &values))
			actual := values["threshold"].(time.Time)
			want, err := time.Parse(time.RFC3339Nano, text)
			require.NoError(t, err)
			require.True(t, want.Equal(actual))
			require.Equal(t, want.UTC().Format(time.RFC3339Nano), actual.Format(time.RFC3339Nano))
			zone, offset := actual.Zone()
			require.Equal(t, "Offset", zone)
			require.Zero(t, offset)
		})
	}
}

// TestParamsDecodesNestedObjectsAsStandardMaps verifies untagged objects remain
// ordinary nested parameter maps.
func TestParamsDecodesNestedObjectsAsStandardMaps(t *testing.T) {
	var values Params
	require.NoError(t, json.Unmarshal([]byte(`{
		"properties": {"name": "node", "nested": {"enabled": true}}
	}`), &values))

	properties, ok := values["properties"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "node", properties["name"])

	nested, ok := properties["nested"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, nested["enabled"])
}

// TestParamsRejectsUnknownTaggedType verifies unsupported tagged parameter
// discriminators fail decoding.
func TestParamsRejectsUnknownTaggedType(t *testing.T) {
	var values Params
	err := json.Unmarshal([]byte(`{"threshold":{"$type":"timestamp","value":"2026-01-02T03:04:05Z"}}`), &values)
	require.ErrorContains(t, err, `unsupported tagged parameter type "timestamp"`)
}

// TestParamsDecodesDeterministicStringList verifies literal inclusions precede
// deterministically numbered generated values.
func TestParamsDecodesDeterministicStringList(t *testing.T) {
	var values Params
	require.NoError(t, json.Unmarshal([]byte(`{
		"object_ids": {"$type": "string_list", "prefix": "missing", "count": 3, "include": ["target-a", "target-b"]}
	}`), &values))

	require.Equal(t, []string{"target-a", "target-b", "missing-00", "missing-01", "missing-02"}, values["object_ids"])
}

// TestParamsRejectsInvalidStringList verifies malformed string-list
// specifications fail decoding.
func TestParamsRejectsInvalidStringList(t *testing.T) {
	testCases := []string{
		`{"ids":{"$type":"string_list","count":1}}`,
		`{"ids":{"$type":"string_list","prefix":"x","count":-1}}`,
		`{"ids":{"$type":"string_list","prefix":"x","count":1.5}}`,
		`{"ids":{"$type":"string_list","prefix":"x","count":1,"include":[1]}}`,
		`{"ids":{"$type":"string_list","prefix":"x","count":1,"extra":true}}`,
	}

	for _, raw := range testCases {
		t.Run(raw, func(t *testing.T) {
			var values Params
			require.Error(t, json.Unmarshal([]byte(raw), &values))
		})
	}
}
