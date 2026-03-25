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

	"github.com/specterops/dawgs/graph"
)

// IDMap maps document string node IDs to their database-assigned IDs.
type IDMap map[string]graph.ID

// Load reads a Document from r, validates it, and writes the graph into db.
// Returns a mapping from document node IDs to database IDs.
func Load(ctx context.Context, db graph.Database, r io.Reader) (IDMap, error) {
	var doc Document

	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("opengraph: decode error: %w", err)
	}

	if err := Validate(doc); err != nil {
		return nil, fmt.Errorf("opengraph: validation error: %w", err)
	}

	return WriteGraph(ctx, db, &doc.Graph)
}

// WriteGraph writes the nodes and edges of g into db.
// Returns a mapping from document node IDs to database IDs.
func WriteGraph(ctx context.Context, db graph.Database, g *Graph) (IDMap, error) {
	nodeMap := make(IDMap, len(g.Nodes))

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, node := range g.Nodes {
			dbNode, err := tx.CreateNode(graph.AsProperties(node.Properties), graph.StringsToKinds(node.Kinds)...)
			if err != nil {
				return fmt.Errorf("could not create node %q: %w", node.ID, err)
			}

			nodeMap[node.ID] = dbNode.ID
		}

		for _, edge := range g.Edges {
			startID, ok := nodeMap[edge.StartID]
			if !ok {
				return fmt.Errorf("could not find start node %q", edge.StartID)
			}

			endID, ok := nodeMap[edge.EndID]
			if !ok {
				return fmt.Errorf("could not find end node %q", edge.EndID)
			}

			if _, err := tx.CreateRelationshipByIDs(startID, endID, graph.StringKind(edge.Kind), graph.AsProperties(edge.Properties)); err != nil {
				return fmt.Errorf("could not create edge (%s)-[%s]->(%s): %w", edge.StartID, edge.Kind, edge.EndID, err)
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("opengraph: write error: %w", err)
	}

	return nodeMap, nil
}
