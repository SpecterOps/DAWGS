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

	"github.com/specterops/dawgs/graph"
)

func TestMatchNodesByKind(t *testing.T) {
	db, ctx := SetupDB(t, "diamond", "linear", "wide_diamond", "disconnected", "dead_end", "direct_shortcut")

	tests := []struct {
		dataset       string
		expectedNodes int
	}{
		{"diamond", 4},
		{"linear", 3},
		{"wide_diamond", 5},
		{"disconnected", 2},
		{"dead_end", 4},
		{"direct_shortcut", 4},
	}

	for _, tt := range tests {
		t.Run(tt.dataset, func(t *testing.T) {
			ClearGraph(t, db, ctx)
			LoadDataset(t, db, ctx, tt.dataset)

			var count int64
			err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var countErr error
				count, countErr = tx.Nodes().Count()
				return countErr
			})
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if int(count) != tt.expectedNodes {
				t.Fatalf("node count: got %d, want %d", count, tt.expectedNodes)
			}
		})
	}
}

func TestMatchEdgesByKind(t *testing.T) {
	db, ctx := SetupDB(t, "diamond", "linear", "wide_diamond", "disconnected", "dead_end", "direct_shortcut")

	tests := []struct {
		dataset       string
		expectedEdges int
	}{
		{"diamond", 4},
		{"linear", 2},
		{"wide_diamond", 6},
		{"disconnected", 0},
		{"dead_end", 3},
		{"direct_shortcut", 4},
	}

	for _, tt := range tests {
		t.Run(tt.dataset, func(t *testing.T) {
			ClearGraph(t, db, ctx)
			LoadDataset(t, db, ctx, tt.dataset)

			var count int64
			err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var countErr error
				count, countErr = tx.Relationships().Count()
				return countErr
			})
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if int(count) != tt.expectedEdges {
				t.Fatalf("edge count: got %d, want %d", count, tt.expectedEdges)
			}
		})
	}
}

func TestMatchReturnNodes(t *testing.T) {
	db, ctx := SetupDB(t, "diamond", "linear", "wide_diamond", "disconnected", "dead_end", "direct_shortcut")

	tests := []struct {
		dataset  string
		queryID  string
		expected []string
	}{
		// diamond: A connects to B and C
		{"diamond", "a", []string{"b", "c"}},
		// linear: A connects to B only
		{"linear", "a", []string{"b"}},
		// wide_diamond: A connects to B, C, D
		{"wide_diamond", "a", []string{"b", "c", "d"}},
	}

	for _, tt := range tests {
		t.Run(tt.dataset, func(t *testing.T) {
			idMap := LoadDataset(t, db, ctx, tt.dataset)

			cypher := fmt.Sprintf(
				"MATCH (s)-[:EdgeKind1]->(e) WHERE id(s) = %d RETURN e",
				idMap[tt.queryID],
			)

			AssertIDSet(t, QueryNodeIDs(t, ctx, db, cypher, idMap), tt.expected)
		})
	}
}
