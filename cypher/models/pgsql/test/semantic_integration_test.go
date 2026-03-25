//go:build manual_integration

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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// errFixtureRollback is returned from every test transaction delegate to
// unconditionally roll back fixture data after each semantic test case.
var errFixtureRollback = errors.New("semantic test fixture rollback")

func TestSemanticTranslation(t *testing.T) {
	pgConnectionStr := os.Getenv(PGConnectionStringEV)
	if pgConnectionStr == "" {
		t.Fatalf("%s environment variable is not set", PGConnectionStringEV)
	}

	testCtx, done := context.WithCancel(context.Background())
	defer done()

	pgxPool, err := pg.NewPool(pgConnectionStr)
	if err != nil {
		t.Fatalf("Failed creating pgx pool: %v", err)
	}

	db, err := dawgs.Open(context.TODO(), pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pgxPool,
	})
	if err != nil {
		t.Fatalf("Failed opening database connection: %v", err)
	}
	defer db.Close(testCtx)

	var (
		numCasesRun = 0
	)

	// Each test case calls AssertSchema independently so that the kind mapper
	// is always in a known state regardless of test ordering.
	for _, testCase := range semanticTestCases {
		t.Run(testCase.Name, func(t *testing.T) {
			if err := db.AssertSchema(testCtx, SemanticGraphSchema); err != nil {
				t.Fatalf("Failed asserting graph schema: %v", err)
			}

			numCasesRun += 1
			slog.Info("Semantic Test", slog.String("name", testCase.Name), slog.Int("num_cases_run", numCasesRun))

			runSemanticCase(testCtx, t, db, testCase)
		})
	}

	slog.Info("Semantic Tests Finished", slog.Int("num_cases_run", numCasesRun))
}

// runSemanticCase creates tc.Fixture inside a write transaction, executes
// tc.Cypher, runs tc.Assert against the live result, optionally runs
// tc.PostAssert, then rolls back all fixture data.
func runSemanticCase(ctx context.Context, t *testing.T, db graph.Database, tc SemanticTestCase) {
	t.Helper()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodeMap, err := createFixtureNodes(tx, tc.Fixture)
		if err != nil {
			return fmt.Errorf("creating fixture nodes: %w", err)
		}

		if err := createFixtureEdges(tx, nodeMap, tc.Fixture); err != nil {
			return fmt.Errorf("creating fixture edges: %w", err)
		}

		result := tx.Query(tc.Cypher, tc.Params)
		defer result.Close()

		if tc.Assert != nil {
			tc.Assert(t, result)
		}

		if tc.PostAssert != nil {
			tc.PostAssert(t, tx)
		}

		return errFixtureRollback
	})

	if !errors.Is(err, errFixtureRollback) {
		t.Errorf("unexpected transaction error in %q: %v", tc.Name, err)
	}
}

// createFixtureNodes creates all nodes described by fixture and returns a
// map from NodeRef to the created *graph.Node so edges can reference them.
func createFixtureNodes(tx graph.Transaction, fixture GraphFixture) (map[NodeRef]*graph.Node, error) {
	nodeMap := make(map[NodeRef]*graph.Node, len(fixture.Nodes))

	for _, nf := range fixture.Nodes {
		var props *graph.Properties
		if len(nf.Props) > 0 {
			props = graph.AsProperties(nf.Props)
		} else {
			props = graph.NewProperties()
		}

		node, err := tx.CreateNode(props, nf.Kinds...)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", nf.Ref, err)
		}

		nodeMap[nf.Ref] = node
	}

	return nodeMap, nil
}

// createFixtureEdges creates all edges described by fixture, resolving node
// references from nodeMap.
func createFixtureEdges(tx graph.Transaction, nodeMap map[NodeRef]*graph.Node, fixture GraphFixture) error {
	for _, ef := range fixture.Edges {
		start, ok := nodeMap[ef.StartRef]
		if !ok {
			return fmt.Errorf("edge start ref %q not in fixture node map", ef.StartRef)
		}

		end, ok := nodeMap[ef.EndRef]
		if !ok {
			return fmt.Errorf("edge end ref %q not in fixture node map", ef.EndRef)
		}

		var props *graph.Properties
		if len(ef.Props) > 0 {
			props = graph.AsProperties(ef.Props)
		} else {
			props = graph.NewProperties()
		}

		if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, ef.Kind, props); err != nil {
			return fmt.Errorf("edge %q->%q (%s): %w", ef.StartRef, ef.EndRef, ef.Kind, err)
		}
	}

	return nil
}
