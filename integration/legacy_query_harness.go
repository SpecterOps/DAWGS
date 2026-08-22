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
)

// WithLegacyNodeQuery executes legacy query-builder criteria directly through
// the selected backend and keeps fixture setup, execution, and assertions in a
// single rollback transaction.
func WithLegacyNodeQuery(
	t *testing.T,
	session *Session,
	fixture *opengraph.Graph,
	criteriaProvider func(idMap opengraph.IDMap) graph.Criteria,
	delegate func(query graph.NodeQuery, idMap opengraph.IDMap) error,
) {
	t.Helper()

	err := session.WithRollbackFixture(t, fixture, false, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		query := tx.Nodes()
		if criteriaProvider != nil {
			query = query.Filter(criteriaProvider(idMap))
		}

		return delegate(query, idMap)
	})
	if err != nil {
		t.Fatalf("legacy node query failed: %v", err)
	}
}

// WithLegacyRelationshipQuery is the relationship-query counterpart to
// WithLegacyNodeQuery.
func WithLegacyRelationshipQuery(
	t *testing.T,
	session *Session,
	fixture *opengraph.Graph,
	criteriaProvider func(idMap opengraph.IDMap) graph.Criteria,
	delegate func(query graph.RelationshipQuery, idMap opengraph.IDMap) error,
) {
	t.Helper()

	err := session.WithRollbackFixture(t, fixture, false, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		query := tx.Relationships()
		if criteriaProvider != nil {
			query = query.Filter(criteriaProvider(idMap))
		}

		return delegate(query, idMap)
	})
	if err != nil {
		t.Fatalf("legacy relationship query failed: %v", err)
	}
}
