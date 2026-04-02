// Copyright 2025 Specter Ops, Inc.
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

package opengraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/specterops/dawgs/graph"
)

// Export reads all nodes and edges from db and writes them as an indented JSON Document to w.
func Export(ctx context.Context, db graph.Database, w io.Writer) error {
	var doc Document

	idToString := make(map[graph.ID]string)

	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		// Export nodes
		if err := tx.Nodes().Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				stringID := strconv.FormatInt(int64(node.ID), 10)
				idToString[node.ID] = stringID

				doc.Graph.Nodes = append(doc.Graph.Nodes, Node{
					ID:         stringID,
					Kinds:      node.Kinds.Strings(),
					Properties: node.Properties.MapOrEmpty(),
				})
			}

			return cursor.Error()
		}); err != nil {
			return fmt.Errorf("failed to fetch nodes: %w", err)
		}

		// Export edges
		if err := tx.Relationships().Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for rel := range cursor.Chan() {
				startStr, ok := idToString[rel.StartID]
				if !ok {
					startStr = strconv.FormatInt(int64(rel.StartID), 10)
				}

				endStr, ok := idToString[rel.EndID]
				if !ok {
					endStr = strconv.FormatInt(int64(rel.EndID), 10)
				}

				doc.Graph.Edges = append(doc.Graph.Edges, Edge{
					StartID:    startStr,
					EndID:      endStr,
					Kind:       rel.Kind.String(),
					Properties: rel.Properties.MapOrEmpty(),
				})
			}

			return cursor.Error()
		}); err != nil {
			return fmt.Errorf("failed to fetch relationships: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("opengraph: export error: %w", err)
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(&doc); err != nil {
		return fmt.Errorf("opengraph: encode error: %w", err)
	}

	return nil
}
