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

//go:build manual_integration

package integration

import (
	"os"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func assertRelationshipIDs(t *testing.T, actual []graph.ID, expected ...graph.ID) {
	t.Helper()

	if len(actual) != len(expected) {
		t.Fatalf("relationship IDs: got %v, want %v", actual, expected)
	}

	remaining := make(map[graph.ID]int, len(expected))
	for _, id := range expected {
		remaining[id]++
	}

	for _, id := range actual {
		if remaining[id] == 0 {
			t.Fatalf("relationship IDs: got %v, want %v", actual, expected)
		}
		remaining[id]--
	}
}

func TestPostgreSQLPropertyTextEqualityCompatibility(t *testing.T) {
	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	driver, err := DriverFromConnectionString(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}
	if driver != pg.DriverName {
		t.Skipf("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	var (
		userKind   = graph.StringKind("User")
		groupKind  = graph.StringKind("Group")
		memberOf   = graph.StringKind("MemberOf")
		db, ctx    = SetupDBWithKinds(t, 0, graph.Kinds{userKind, groupKind}, graph.Kinds{memberOf})
		boolTrue   *graph.Relationship
		boolFalse  *graph.Relationship
		stringTrue *graph.Relationship
	)

	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		user, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"name": "user",
		}), userKind)
		if err != nil {
			return err
		}

		boolTrueGroup, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"isassignabletorole": true,
			"rank":               1,
		}), groupKind)
		if err != nil {
			return err
		}

		boolFalseGroup, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"isassignabletorole": false,
			"rank":               2,
		}), groupKind)
		if err != nil {
			return err
		}

		stringTrueGroup, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"isassignabletorole": "true",
			"rank":               "1",
		}), groupKind)
		if err != nil {
			return err
		}

		if boolTrue, err = tx.CreateRelationshipByIDs(user.ID, boolTrueGroup.ID, memberOf, graph.NewProperties()); err != nil {
			return err
		}
		if boolFalse, err = tx.CreateRelationshipByIDs(user.ID, boolFalseGroup.ID, memberOf, graph.NewProperties()); err != nil {
			return err
		}
		if stringTrue, err = tx.CreateRelationshipByIDs(user.ID, stringTrueGroup.ID, memberOf, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("failed to create fixture: %v", err)
	}

	testCases := []struct {
		name     string
		property string
		value    any
		expected []graph.ID
	}{{
		name:     "text true matches JSON boolean and string true",
		property: "isassignabletorole",
		value:    "true",
		expected: []graph.ID{boolTrue.ID, stringTrue.ID},
	}, {
		name:     "boolean true remains strict",
		property: "isassignabletorole",
		value:    true,
		expected: []graph.ID{boolTrue.ID},
	}, {
		name:     "text false matches JSON boolean false text",
		property: "isassignabletorole",
		value:    "false",
		expected: []graph.ID{boolFalse.ID},
	}, {
		name:     "boolean false remains strict",
		property: "isassignabletorole",
		value:    false,
		expected: []graph.ID{boolFalse.ID},
	}, {
		name:     "text number remains strict",
		property: "rank",
		value:    "1",
		expected: []graph.ID{stringTrue.ID},
	}, {
		name:     "numeric literal remains strict",
		property: "rank",
		value:    1,
		expected: []graph.ID{boolTrue.ID},
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var actual []graph.ID

			err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				return tx.Relationships().Filter(query.And(
					query.Kind(query.Relationship(), memberOf),
					query.Kind(query.Start(), userKind),
					query.Kind(query.End(), groupKind),
					query.Equals(query.EndProperty(testCase.property), testCase.value),
				)).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
					for id := range cursor.Chan() {
						actual = append(actual, id)
					}

					return cursor.Error()
				})
			})
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			assertRelationshipIDs(t, actual, testCase.expected...)
		})
	}
}
