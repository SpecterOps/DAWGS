// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestLegacyBuilderDelegatedEnrollmentDiscovery(t *testing.T) {
	fixture := delegatedEnrollmentFixture()
	nodeKinds, edgeKinds := fixture.Kinds()
	db, ctx := SetupDBWithKindsNoGraphCleanup(t, nodeKinds, edgeKinds)
	ClearGraph(t, db, ctx)

	WithLegacyRelationshipQuery(t, &Session{
		DB:  db,
		Ctx: ctx,
	}, fixture, func(opengraph.IDMap) graph.Criteria {
		return query.And(
			query.In(query.EndProperty("objectid"), []string{"ca-a", "ca-b"}),
			query.Kind(query.Relationship(), graph.StringKind("PublishedTo")),
			query.Kind(query.Start(), graph.StringKind("CertTemplate")),
		)
	}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
		relationships, err := ops.FetchRelationships(relationshipQuery)
		require.NoError(t, err)
		require.Len(t, relationships, 3, "raw relationship results must retain duplicate paths to one template")

		nodes, err := ops.FetchStartNodes(relationshipQuery)
		require.NoError(t, err)
		require.Equal(t, 2, nodes.Len(), "FetchStartNodes must de-duplicate repeated start nodes")
		require.True(t, nodes.ContainsID(idMap["template-a"]))
		require.True(t, nodes.ContainsID(idMap["template-b"]))
		return nil
	})
}

func delegatedEnrollmentFixture() *opengraph.Graph {
	return &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:         "template-a",
				Kinds:      []string{"CertTemplate"},
				Properties: map[string]any{"objectid": "template-a"},
			},
			{
				ID:         "template-b",
				Kinds:      []string{"CertTemplate"},
				Properties: map[string]any{"objectid": "template-b"},
			},
			{
				ID:         "wrong-start",
				Kinds:      []string{"OtherTemplate"},
				Properties: map[string]any{"objectid": "wrong-start"},
			},
			{
				ID:         "ca-a",
				Kinds:      []string{"EnterpriseCA"},
				Properties: map[string]any{"objectid": "ca-a"},
			},
			{
				ID:         "ca-b",
				Kinds:      []string{"EnterpriseCA"},
				Properties: map[string]any{"objectid": "ca-b"},
			},
		},
		Edges: []opengraph.Edge{
			{
				StartID:    "template-a",
				EndID:      "ca-a",
				Kind:       "PublishedTo",
				Properties: map[string]any{"marker": "published-a"},
			},
			{
				StartID:    "template-a",
				EndID:      "ca-b",
				Kind:       "PublishedTo",
				Properties: map[string]any{"marker": "published-b"},
			},
			{
				StartID:    "template-b",
				EndID:      "ca-a",
				Kind:       "PublishedTo",
				Properties: map[string]any{"marker": "published-c"},
			},
			{
				StartID:    "wrong-start",
				EndID:      "ca-a",
				Kind:       "PublishedTo",
				Properties: map[string]any{"marker": "wrong-start"},
			},
			{
				StartID:    "template-a",
				EndID:      "ca-a",
				Kind:       "OtherPublication",
				Properties: map[string]any{"marker": "wrong-edge"},
			},
		},
	}
}
