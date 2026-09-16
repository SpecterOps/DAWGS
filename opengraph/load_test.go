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

// SPDX-License-Identifier: Apache-2.0

package opengraph

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

type recordingKindMapper struct {
	asserted graph.Kinds
	err      error
}

func (m *recordingKindMapper) MapKindID(context.Context, int16) (graph.Kind, error) { return nil, nil }
func (m *recordingKindMapper) MapKindIDs(context.Context, []int16) (graph.Kinds, error) {
	return nil, nil
}
func (m *recordingKindMapper) MapKind(context.Context, graph.Kind) (int16, error) { return 0, nil }
func (m *recordingKindMapper) MapKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, nil
}
func (m *recordingKindMapper) AssertKinds(_ context.Context, kinds graph.Kinds) ([]int16, error) {
	m.asserted = kinds
	return nil, m.err
}

type kindMappedTestDatabase struct {
	graph.Database
	mapper              pg.KindMapper
	writeTransactionErr error
}

func (d *kindMappedTestDatabase) KindMapper() pg.KindMapper { return d.mapper }

func (d *kindMappedTestDatabase) WriteTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return d.writeTransactionErr
}

func TestWriteGraphAssertsDistinctEdgeKinds(t *testing.T) {
	mapper := &recordingKindMapper{}
	db := &kindMappedTestDatabase{mapper: mapper, writeTransactionErr: errors.New("stop after assertion")}
	g := &Graph{Edges: []Edge{
		{Kind: "KNOWS"},
		{Kind: "FOLLOWS"},
		{Kind: "KNOWS"},
	}}

	_, err := WriteGraph(context.Background(), db, g)
	if err == nil || !errors.Is(err, db.writeTransactionErr) {
		t.Fatalf("expected write transaction error, got %v", err)
	}

	if len(map[graph.Kind]struct{}{mapper.asserted[0]: {}, mapper.asserted[1]: {}}) != 2 {
		t.Fatalf("expected two distinct asserted kinds, got %v", mapper.asserted)
	}
}

func TestWriteGraphReturnsKindAssertionError(t *testing.T) {
	assertErr := errors.New("assert kinds failed")
	mapper := &recordingKindMapper{err: assertErr}
	db := &kindMappedTestDatabase{mapper: mapper}

	_, err := WriteGraph(context.Background(), db, &Graph{Edges: []Edge{{Kind: "KNOWS"}}})
	if !errors.Is(err, assertErr) {
		t.Fatalf("expected kind assertion error, got %v", err)
	}
}

func TestWriteGraphNilGraph(t *testing.T) {
	idMap, err := WriteGraph(context.Background(), &kindMappedTestDatabase{}, nil)
	if err != nil || idMap != nil {
		t.Fatalf("expected nil result for nil graph, got %v, %v", idMap, err)
	}
}
