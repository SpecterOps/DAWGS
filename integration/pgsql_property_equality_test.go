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
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
		userKind    = graph.StringKind("User")
		groupKind   = graph.StringKind("Group")
		memberOf    = graph.StringKind("MemberOf")
		db, ctx     = SetupDBWithKinds(t, 0, graph.Kinds{userKind, groupKind}, graph.Kinds{memberOf})
		boolTrue    *graph.Relationship
		boolFalse   *graph.Relationship
		stringTrue  *graph.Relationship
		stringFalse *graph.Relationship
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

		stringFalseGroup, err := tx.CreateNode(graph.AsProperties(map[string]any{
			"isassignabletorole": "false",
			"rank":               "2",
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
		if stringFalse, err = tx.CreateRelationshipByIDs(user.ID, stringFalseGroup.ID, memberOf, graph.NewProperties()); err != nil {
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
		name:     "text true only matches JSON string true",
		property: "isassignabletorole",
		value:    "true",
		expected: []graph.ID{stringTrue.ID},
	}, {
		name:     "boolean true remains strict",
		property: "isassignabletorole",
		value:    true,
		expected: []graph.ID{boolTrue.ID},
	}, {
		name:     "text false only matches JSON string false",
		property: "isassignabletorole",
		value:    "false",
		expected: []graph.ID{stringFalse.ID},
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

func TestPostgreSQLLiveObjectIDEqualityPlanUsesTextExpressionIndex(t *testing.T) {
	connStr := os.Getenv("CONNECTION_STRING")
	if connStr == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	driver, err := driverFromConnStr(connStr)
	if err != nil {
		t.Fatalf("failed to detect driver: %v", err)
	}
	if driver != pg.DriverName {
		t.Skipf("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse PG connection string: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	var hasObjectIDIndex bool
	if err := pool.QueryRow(ctx, `
		select exists (
			select 1
			from pg_indexes
			where tablename like 'node\_%' escape '\'
			  and indexdef like '%properties ->> ''objectid''%'
		)
	`).Scan(&hasObjectIDIndex); err != nil {
		t.Fatalf("failed to inspect node indexes: %v", err)
	}
	if !hasObjectIDIndex {
		t.Skip("connected PostgreSQL database has no node objectid text expression index")
	}

	var objectID string
	if err := pool.QueryRow(ctx, `
		select n.properties ->> 'objectid'
		from node n
		join kind k on k.name = 'Group'
		where n.kind_ids operator (pg_catalog.@>) array[k.id]::int2[]
		  and jsonb_typeof(n.properties -> 'objectid') = 'string'
		limit 1
	`).Scan(&objectID); err != nil {
		if err == pgx.ErrNoRows {
			t.Skip("connected PostgreSQL database has no Group node with a string objectid")
		}

		t.Fatalf("failed to find live objectid sample: %v", err)
	}

	rows, err := pool.Query(ctx, `
		explain (analyze, buffers, timing off, summary off)
		select n.id
		from node n
		where jsonb_typeof(n.properties -> 'objectid') = 'string'
		  and n.properties ->> 'objectid' = $1
		limit 1
	`, objectID)
	if err != nil {
		t.Fatalf("failed to explain objectid lookup: %v", err)
	}
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("failed to scan plan line: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed while reading plan: %v", err)
	}

	plan := strings.Join(planLines, "\n")
	if !strings.Contains(plan, "Index") || !strings.Contains(plan, "objectid") {
		t.Fatalf("expected objectid text expression index plan, got:\n%s", plan)
	}
}
