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

func TestVariableLengthTraversal(t *testing.T) {
	db, ctx := SetupDB(t, "linear", "diamond", "wide_diamond", "dead_end", "disconnected")

	tests := []struct {
		dataset  string
		start    string
		expected []string // all reachable nodes (not including start)
	}{
		// A -> B -> C: from A, reach B and C
		{"linear", "a", []string{"b", "c"}},
		// A -> B -> D, A -> C -> D: from A, reach B, C, D
		{"diamond", "a", []string{"b", "c", "d"}},
		// A -> {B,C,D} -> E: from A, reach B, C, D, E
		{"wide_diamond", "a", []string{"b", "c", "d", "e"}},
		// A -> B -> C, A -> D: from A, reach B, C, D
		{"dead_end", "a", []string{"b", "c", "d"}},
		// disconnected: from A, reach nothing
		{"disconnected", "a", nil},
	}

	for _, tt := range tests {
		t.Run(tt.dataset, func(t *testing.T) {
			idMap := LoadDataset(t, db, ctx, tt.dataset)

			cypher := fmt.Sprintf(
				"MATCH (s)-[:EdgeKind1*1..]->(e) WHERE id(s) = %d RETURN e",
				idMap[tt.start],
			)

			AssertIDSet(t, QueryNodeIDs(t, ctx, db, cypher, idMap), tt.expected)
		})
	}
}
