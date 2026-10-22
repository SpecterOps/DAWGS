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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/specterops/dawgs/drivers/pg/model"
	sql "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// staticKindMapper returns deterministic kind mappings for relationship batch tests.
type staticKindMapper struct {
}

func TestNodeUpsertKindRemovalsUseResolvedIDs(t *testing.T) {
	batch := sql.NewNodeUpdateBatch()
	removedKind := graph.StringKind("Removed")
	for idx, name := range []string{"untouched", "one", "two"} {
		node := graph.PrepareNode(graph.AsProperties(map[string]any{"id": name, "preserved": true}), graph.StringKind("Base"))
		if idx > 0 {
			node.DeleteKinds(removedKind)
		}
		future, err := batch.Add(graph.NodeUpdate{Node: node, IdentityProperties: []string{"id"}})
		require.NoError(t, err)
		future.Value = graph.ID(10 + idx)
	}
	removals := nodeUpsertKindRemovals(batch)
	require.Len(t, removals, 2)
	ids := []graph.ID{}
	for _, node := range removals {
		ids = append(ids, node.ID)
		require.Equal(t, graph.Kinds{removedKind}, node.DeletedKinds)
		require.Empty(t, node.Kinds, "removing a kind must not replace or add any other kinds")
		require.Empty(t, node.Properties.Map, "the second write must leave properties alone")
	}
	require.ElementsMatch(t, []graph.ID{11, 12}, ids)
	require.Empty(t, nodeUpsertKindRemovals(sql.NewNodeUpdateBatch()))
}

func TestNodeUpsertKindRemovalExecution(t *testing.T) {
	for _, fail := range []string{"", "upsert", "removal"} {
		t.Run("failure="+fail, func(t *testing.T) {
			ctx := context.Background()
			manager := NewSchemaManager(nil, 0)
			t.Cleanup(func() { manager.parseCache.Close(); manager.compilationCache.Close() })
			manager.hasDefaultGraph = true
			manager.defaultGraph = model.Graph{ID: 1, Partitions: model.GraphPartitions{Node: model.NewGraphPartition("node_1")}}
			removed := graph.StringKind("Removed")
			manager.kindsByID[removed] = 7
			node := graph.PrepareNode(graph.AsProperties(map[string]any{"id": "one"}))
			node.DeleteKinds(removed)
			updates := sql.NewNodeUpdateBatch()
			future, err := updates.Add(graph.NodeUpdate{Node: node, IdentityProperties: []string{"id"}})
			require.NoError(t, err)
			parameters := NewNodeUpsertParameters(1)
			parameters.IDFutures = append(parameters.IDFutures, future)
			b := &batch{ctx: ctx, schemaManager: manager, innerTransaction: &transaction{schemaManager: manager}, kindIDEncoder: Int2ArrayEncoder{buffer: &bytes.Buffer{}}}
			mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			failure := errors.New("forced write failure")
			upsert := mock.ExpectQuery(sql.FormatNodeUpsert(manager.defaultGraph, []string{"id"})).WithArgs(int32(1), parameters.KindIDSlices, parameters.Properties)
			if fail == "upsert" {
				upsert.WillReturnError(failure)
			} else {
				upsert.WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(uint64(42)))
				removal := mock.ExpectQuery(sql.FormatNodesUpdate(manager.defaultGraph)).WithArgs([]graph.ID{42}, []string{"{}"}, []string{"{7}"}, pgxmock.AnyArg(), pgxmock.AnyArg())
				if fail == "removal" {
					removal.WillReturnError(failure)
				} else {
					removal.WillReturnRows(pgxmock.NewRows([]string{}))
				}
			}
			err = b.flushNodeUpsertBatchWithDriver(updates, parameters, mock)
			if fail == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
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
