// Copyright 2025 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package query

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeUpdateBatch was created to reproduce a specific scenario where a
// node's identity property is updated around the time that the batch processes
// it.
func TestNodeUpdateBatch(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T) *NodeUpdateBatch
		assert func(t *testing.T, updates *NodeUpdateBatch)
	}{
		{
			name: "Success: Conflicting key successfully serializes — batch keeps both identities",
			setup: func(t *testing.T) *NodeUpdateBatch {
				updates := NewNodeUpdateBatch()

				// Add node A to the batch
				firstNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-A"), graph.StringKind("User"))
				_, err := updates.Add(graph.NodeUpdate{
					Node:               firstNode,
					IdentityProperties: []string{"objectid"},
				})
				require.NoError(t, err, "unexpected error occurred while adding first node to batch")

				// The value of the unique key (objectid) is updated AFTER the node
				// has already been added to the batch. 
				// Without a snapshot, this causes both the stored entry
				// (keyed as "OID-A") and the upcoming second entry to serialize with
				// objectid="OID-B" — producing a duplicate key in the INSERT statement.
				firstNode.Properties.Set("objectid", "OID-B")

				// Add node B to the batch
				secondNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-B"), graph.StringKind("User"))
				_, err = updates.Add(graph.NodeUpdate{
					Node:               secondNode,
					IdentityProperties: []string{"objectid"},
				})
				require.NoError(t, err, "unexpected error occurred while adding second node to batch")

				return updates
			},
			assert: func(t *testing.T, updates *NodeUpdateBatch) {
				// The snapshot must preserve OID-A so two distinct keys exist.
				assert.Len(t, updates.Updates, 2, "batch must hold two distinct entries")

				// The first entry must retain its original identity property value.
				oidAUpdate, hasOIDA := updates.Updates["OID-A"]
				assert.True(t, hasOIDA, "OID-A entry must still exist in the batch")

				// The stored node must retain its original identity property value.
				storedOIDA, err := oidAUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-A", storedOIDA, "stored node must retain its original identity")

				// The second entry must exist and retain its original identity property value.
				oidBUpdate, hasOIDB := updates.Updates["OID-B"]
				assert.True(t, hasOIDB, "OID-B entry must exist in the batch")

				// The stored node must retain its original identity property value.
				storedOIDB, err := oidBUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-B", storedOIDB)
			},
		},
		{
			name: "Success: Conflicting key successfully serializes — both entries merge into one row",
			setup: func(t *testing.T) *NodeUpdateBatch {
				// Add node A to the batch
				firstNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-A"), graph.StringKind("User"))
				queuedUpdates := []graph.NodeUpdate{{
					Node:               firstNode,
					IdentityProperties: []string{"objectid"},
				}}

				// The value of the unique key (objectid) is updated BEFORE the node
				// is added to the batch. 
				// At validation time, Key() reads "OID-B" for both entries,
				// so they naturally collapse into one row. This results in no possible conflict.
				firstNode.Properties.Set("objectid", "OID-B")

				// Add node B to the batch
				secondNode := graph.NewNode(0, graph.NewProperties().Set("objectid", "OID-B"), graph.StringKind("User"))
				queuedUpdates = append(queuedUpdates, graph.NodeUpdate{
					Node:               secondNode,
					IdentityProperties: []string{"objectid"},
				})

				// Validate the batch
				updates, err := ValidateNodeUpdateByBatch(queuedUpdates)
				require.NoError(t, err, "setup: ValidateNodeUpdateByBatch failed")

				return updates
			},
			assert: func(t *testing.T, updates *NodeUpdateBatch) {
				// Both entries share key "OID-B" at validation time, so they merge.
				assert.Len(t, updates.Updates, 1, "both entries share OID-B so they merge")

				// The merged entry is keyed as "OID-B" and contains the properties of both nodes.
				collapsedUpdate, hasKey := updates.Updates["OID-B"]
				assert.True(t, hasKey, "merged entry must be keyed as OID-B")

				// The collapsed node must retain the original identity property value.
				collapsedObjectID, err := collapsedUpdate.Node.Properties.Get("objectid").String()
				assert.NoError(t, err, "unexpected error occurred while fetching objectid")
				assert.Equal(t, "OID-B", collapsedObjectID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			updates := tt.setup(t)
			tt.assert(t, updates)
		})
	}
}
