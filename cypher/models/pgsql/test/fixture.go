// Copyright 2026 Specter Ops, Inc.
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

package test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
)

var (
	// Node and edge kinds to keep queries consistent
	NodeKind1 = graph.StringKind("NodeKind1")
	NodeKind2 = graph.StringKind("NodeKind2")
	EdgeKind1 = graph.StringKind("EdgeKind1")
	EdgeKind2 = graph.StringKind("EdgeKind2")
)

// SemanticGraphSchema is the graph schema used by semantic integration tests.
// It must be kept in sync with the schema in validation_integration_test.go so
// that kind IDs are assigned in the same order and match the golden SQL files.
var SemanticGraphSchema = graph.Schema{
	Graphs: []graph.Graph{{
		Name:  "test",
		Nodes: graph.Kinds{NodeKind1, NodeKind2},
		Edges: graph.Kinds{EdgeKind1, EdgeKind2},
	}},
	DefaultGraph: graph.Graph{Name: "test"},
}

// NodeRef is a symbolic name used to wire nodes into edges within a fixture.
type NodeRef = string

// NodeFixture describes a single node to be created in the fixture graph.
type NodeFixture struct {
	Ref   NodeRef
	Kinds graph.Kinds
	Props map[string]any
}

// EdgeFixture describes a directed edge to be created in the fixture graph.
type EdgeFixture struct {
	StartRef NodeRef
	EndRef   NodeRef
	Kind     graph.Kind
	Props    map[string]any
}

// GraphFixture is the complete description of the minimal graph state required
// by a SemanticTestCase. Nodes and edges are created inside a write transaction
// that is always rolled back after the test completes.
type GraphFixture struct {
	Nodes []NodeFixture
	Edges []EdgeFixture
}

// SemanticTestCase pairs a Cypher query with a fixture graph and assertions
// on the result set produced by executing that query against the fixture.
type SemanticTestCase struct {
	// Name is a human-readable label shown by the test runner.
	Name string
	// Cypher is the query passed verbatim to graph.Transaction.Query.
	Cypher string
	// Params are optional Cypher-level parameters forwarded to the query.
	Params map[string]any
	// Fixture is the graph state created before executing Cypher.
	Fixture GraphFixture
	// Assert inspects the query result. Next() has not yet been called.
	Assert ResultAssertion
	// PostAssert runs after Assert while the transaction is still open.
	// Use this for destructive queries (delete/update) that require a
	// follow-up read to verify the mutation took effect.
	PostAssert func(*testing.T, graph.Transaction)
}

// NewNode creates a NodeFixture with no properties.
func NewNode(ref NodeRef, kinds ...graph.Kind) NodeFixture {
	return NodeFixture{Ref: ref, Kinds: kinds}
}

// NewNodeWithProperties creates a NodeFixture with the given properties and kinds.
func NewNodeWithProperties(ref NodeRef, props map[string]any, kinds ...graph.Kind) NodeFixture {
	return NodeFixture{Ref: ref, Kinds: kinds, Props: props}
}

// NewEdge creates an EdgeFixture with no properties.
func NewEdge(start, end NodeRef, kind graph.Kind) EdgeFixture {
	return EdgeFixture{StartRef: start, EndRef: end, Kind: kind}
}

// NewEdgeWithProperties creates an EdgeFixture with properties.
func NewEdgeWithProperties(start, end NodeRef, kind graph.Kind, props map[string]any) EdgeFixture {
	return EdgeFixture{StartRef: start, EndRef: end, Kind: kind, Props: props}
}
