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

package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// staticKindMapper returns deterministic kind mappings for relationship batch tests.
type staticKindMapper struct {
}

// MapKindID returns the fixed kind associated with every synthetic ID.
func (s staticKindMapper) MapKindID(context.Context, int16) (graph.Kind, error) {
	return graph.StringKind("WriteCreateRelationship"), nil
}

// MapKindIDs returns the fixed kind set used by the batch fixture.
func (s staticKindMapper) MapKindIDs(context.Context, []int16) (graph.Kinds, error) {
	return graph.Kinds{graph.StringKind("WriteCreateRelationship")}, nil
}

// MapKind returns the fixed database ID associated with every synthetic kind.
func (s staticKindMapper) MapKind(context.Context, graph.Kind) (int16, error) {
	return 1, nil
}

// MapKinds returns the fixed database ID set used by the batch fixture.
func (s staticKindMapper) MapKinds(context.Context, graph.Kinds) ([]int16, error) {
	return []int16{1}, nil
}

// AssertKinds accepts every supplied kind and returns the fixture's fixed database ID.
func (s staticKindMapper) AssertKinds(context.Context, graph.Kinds) ([]int16, error) {
	return []int16{1}, nil
}

// TestRelationshipCreateBatchBuilderMergesPropertiesByConflictKey verifies distinct endpoint tuples cannot collide and duplicate tuples merge properties.
func TestRelationshipCreateBatchBuilderMergesPropertiesByConflictKey(t *testing.T) {
	var (
		ctx     = context.Background()
		kind    = graph.StringKind("WriteCreateRelationship")
		builder = newRelationshipCreateBatchBuilder(4)
	)

	updates := []*graph.Relationship{
		// These two endpoint pairs had the same concatenated key ("123...")
		// before the batch builder used a structured conflict key.
		graph.NewRelationship(0, 1, 23, graph.NewProperties().SetAll(map[string]any{"custom": "a-first", "a": true}), kind),
		graph.NewRelationship(0, 1, 23, graph.NewProperties().SetAll(map[string]any{"custom": "a-last", "a-last": true}), kind),
		graph.NewRelationship(0, 12, 3, graph.NewProperties().SetAll(map[string]any{"custom": "b-first", "b": true}), kind),
		graph.NewRelationship(0, 12, 3, graph.NewProperties().SetAll(map[string]any{"custom": "b-last", "b-last": true}), kind),
	}
	for _, update := range updates {
		require.NoError(t, builder.Add(ctx, staticKindMapper{}, update))
	}

	require.Len(t, builder.edgePropertiesBatch, 2)
	require.Equal(t, "a-last", builder.edgePropertiesBatch[0].Get("custom").Any())
	require.Equal(t, true, builder.edgePropertiesBatch[0].Get("a").Any())
	require.Equal(t, true, builder.edgePropertiesBatch[0].Get("a-last").Any())
	require.False(t, builder.edgePropertiesBatch[0].Exists("b-last"))
	require.Equal(t, "b-last", builder.edgePropertiesBatch[1].Get("custom").Any())
	require.Equal(t, true, builder.edgePropertiesBatch[1].Get("b").Any())
	require.Equal(t, true, builder.edgePropertiesBatch[1].Get("b-last").Any())
	require.False(t, builder.edgePropertiesBatch[1].Exists("a-last"))

	batch, err := builder.Build()
	require.NoError(t, err)
	require.Len(t, batch.startIDs, 2)
	require.Len(t, batch.edgePropertyBags, 2)
}
