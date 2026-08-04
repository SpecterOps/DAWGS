// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
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

package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

func TestResolveCaseParams(t *testing.T) {
	params, err := resolveCaseParams(ScaleCase{
		Params: map[string]any{
			"name": "value",
		},
		NodeParams: map[string]string{
			"start_id": "n1",
		},
		NodeListParams: map[string][]string{
			"end_ids": {"n2", "n1"},
		},
		GeneratedNodeListParams: map[string]testutil.GeneratedNodeListParam{
			"generated_ids": {Prefix: "generated", Count: 2, Include: []string{"n2"}},
		},
	}, opengraph.IDMap{
		"n1":           graph.ID(42),
		"n2":           graph.ID(84),
		"generated-00": graph.ID(126),
		"generated-01": graph.ID(168),
	})

	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"name":          "value",
		"start_id":      int64(42),
		"end_ids":       []int64{84, 42},
		"generated_ids": []int64{84, 126, 168},
	}, params)
}

func TestScaleCaseDecodesTypedDatetimeParameter(t *testing.T) {
	var testCase ScaleCase
	require.NoError(t, json.Unmarshal([]byte(`{
		"name":"typed-time",
		"dataset":"base",
		"category":"lookup",
		"cypher":"MATCH (n) WHERE n.lastseen < $threshold RETURN n",
		"params":{"threshold":{"$type":"datetime","value":"2026-01-02T03:04:05Z"}},
		"candidate_modes":["postgres_sql"]
	}`), &testCase))

	require.Equal(t, time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC), testCase.Params["threshold"])
}

func TestParsePostgresPlanMetrics(t *testing.T) {
	metrics := parsePostgresPlanMetrics([]string{
		"Nested Loop  (actual rows=1 loops=1)",
		"  Buffers: shared hit=12 read=3 dirtied=2, temp read=4 written=5",
		"Planning Time: 1.250 ms",
		"Execution Time: 9.750 ms",
	})

	require.NotNil(t, metrics.PlanningMS)
	require.Equal(t, 1.25, *metrics.PlanningMS)
	require.NotNil(t, metrics.ExecutionMS)
	require.Equal(t, 9.75, *metrics.ExecutionMS)
	require.Equal(t, Buffers{
		SharedHit:     12,
		SharedRead:    3,
		SharedDirtied: 2,
		TempRead:      4,
		TempWritten:   5,
	}, metrics.Buffers)
}
