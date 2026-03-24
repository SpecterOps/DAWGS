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

package opengraph

import (
	"errors"
	"fmt"
)

// Validate checks a Document for structural errors. It returns nil for an empty graph.
func Validate(doc Document) error {
	nodeIDs := make(map[string]struct{}, len(doc.Graph.Nodes))

	for i, node := range doc.Graph.Nodes {
		if node.ID == "" {
			return fmt.Errorf("node at index %d has an empty ID", i)
		}

		if _, exists := nodeIDs[node.ID]; exists {
			return fmt.Errorf("duplicate node ID %q", node.ID)
		}

		nodeIDs[node.ID] = struct{}{}
	}

	var errs []error

	for i, edge := range doc.Graph.Edges {
		if edge.Kind == "" {
			errs = append(errs, fmt.Errorf("edge at index %d has an empty kind", i))
		}

		if edge.StartID == "" {
			errs = append(errs, fmt.Errorf("edge at index %d has an empty start_id", i))
		} else if _, ok := nodeIDs[edge.StartID]; !ok {
			errs = append(errs, fmt.Errorf("edge at index %d references unknown start node %q", i, edge.StartID))
		}

		if edge.EndID == "" {
			errs = append(errs, fmt.Errorf("edge at index %d has an empty end_id", i))
		} else if _, ok := nodeIDs[edge.EndID]; !ok {
			errs = append(errs, fmt.Errorf("edge at index %d references unknown end node %q", i, edge.EndID))
		}
	}

	return errors.Join(errs...)
}
