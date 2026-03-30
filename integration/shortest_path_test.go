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

//go:build manual_integration

package integration

import (
	"fmt"
	"testing"
)

func TestAllShortestPaths(t *testing.T) {
	db, ctx := SetupDB(t, "diamond", "direct_shortcut", "linear", "dead_end", "disconnected", "wide_diamond")

	tests := []struct {
		dataset    string
		start, end string
		expected   [][]string
	}{
		// A -> B -> D, A -> C -> D: two shortest paths of depth 2
		{"diamond", "a", "d", [][]string{{"a", "b", "d"}, {"a", "c", "d"}}},

		// A -> B -> C -> D, A -> D: direct edge wins
		{"direct_shortcut", "a", "d", [][]string{{"a", "d"}}},

		// A -> B -> C: single path
		{"linear", "a", "c", [][]string{{"a", "b", "c"}}},

		// A -> B -> C, A -> D (dead end): dead end doesn't affect result
		{"dead_end", "a", "c", [][]string{{"a", "b", "c"}}},

		// A, B disconnected: no paths
		{"disconnected", "a", "b", nil},

		// A -> {B,C,D} -> E: three shortest paths of depth 2
		{"wide_diamond", "a", "e", [][]string{{"a", "b", "e"}, {"a", "c", "e"}, {"a", "d", "e"}}},
	}

	for _, tt := range tests {
		t.Run(tt.dataset, func(t *testing.T) {
			idMap := LoadDataset(t, db, ctx, tt.dataset)

			cypher := fmt.Sprintf(
				"MATCH p = allShortestPaths((s)-[:EdgeKind1*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
				idMap[tt.start], idMap[tt.end],
			)

			paths := QueryPaths(t, ctx, db, cypher)
			AssertPaths(t, paths, idMap, tt.expected)
		})
	}
}
