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

// Document is the top-level container for an OpenGraph JSON file.
type Document struct {
	Graph Graph `json:"graph"`
}

// Graph contains the nodes and edges of the graph.
type Graph struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// Node represents a graph node with a string ID, one or more kind labels, and arbitrary properties.
type Node struct {
	ID         string         `json:"id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Edge represents a directed relationship between two nodes.
type Edge struct {
	StartID    string         `json:"start_id"`
	EndID      string         `json:"end_id"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}
