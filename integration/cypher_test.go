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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

// caseFile represents one JSON test case file.
type caseFile struct {
	Dataset     string     `json:"dataset"`
	SkipDrivers []string   `json:"skip_drivers,omitempty"`
	Cases       []testCase `json:"cases"`
}

// testCase is a single test: a Cypher query and an assertion on its result.
// Cases with a "fixture" field run in a write transaction that rolls back,
// so the inline data doesn't persist.
type testCase struct {
	Name        string           `json:"name"`
	SkipDrivers []string         `json:"skip_drivers,omitempty"`
	Cypher      string           `json:"cypher"`
	Assert      json.RawMessage  `json:"assert"`
	Fixture     *opengraph.Graph `json:"fixture,omitempty"`
}

func TestCypher(t *testing.T) {
	files, err := filepath.Glob("testdata/cases/*.json")
	if err != nil {
		t.Fatalf("failed to glob case files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no case files found in testdata/cases/")
	}

	// Parse all case files and group by dataset.
	type group struct {
		dataset string
		files   []caseFile
	}
	groups := map[string]*group{}
	var datasetNames []string

	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read %s: %v", path, err)
		}

		var cf caseFile
		if err := json.Unmarshal(raw, &cf); err != nil {
			t.Fatalf("failed to decode %s: %v", path, err)
		}

		ds := cf.Dataset
		if ds == "" {
			ds = "base"
		}

		if groups[ds] == nil {
			groups[ds] = &group{dataset: ds}
			datasetNames = append(datasetNames, ds)
		}
		groups[ds].files = append(groups[ds].files, cf)
	}

	db, ctx := SetupDB(t, datasetNames...)

	driver, err := driverFromConnStr(os.Getenv("CONNECTION_STRING"))
	if err != nil {
		t.Fatalf("Failed to detect driver: %v", err)
	}

	for _, g := range groups {
		ClearGraph(t, db, ctx)
		LoadDataset(t, db, ctx, g.dataset)

		for _, cf := range g.files {
			if slices.Contains(cf.SkipDrivers, driver) {
				continue
			}

			for _, tc := range cf.Cases {
				t.Run(tc.Name, func(t *testing.T) {
					if slices.Contains(tc.SkipDrivers, driver) {
						t.Skipf("skipped for driver %s", driver)
					}

					defer func() {
						if r := recover(); r != nil {
							t.Fatalf("panic: %v", r)
						}
					}()

					check := parseAssertion(t, tc.Assert)

					if tc.Fixture != nil {
						runWithFixture(t, ctx, db, tc, check)
					} else {
						runReadOnly(t, ctx, db, tc, check)
					}
				})
			}
		}
	}
}

// parseAssertion converts a JSON assertion value into a function that checks
// a query result. Supports:
//
//	"non_empty"                           — at least one row
//	"empty"                               — zero rows
//	"no_error"                            — drains result, checks no error
//	{"row_count": N}                      — exactly N rows
//	{"at_least_int": N}                   — first scalar >= N
//	{"exact_int": N}                      — first scalar == N
//	{"contains_node_with_prop": [K, V]}   — some row has a node with property K=V
func parseAssertion(t *testing.T, raw json.RawMessage) func(*testing.T, graph.Result) {
	t.Helper()

	// Try as a simple string first.
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		switch str {
		case "non_empty":
			return assertNonEmpty
		case "empty":
			return assertEmpty
		case "no_error":
			return assertNoError
		default:
			t.Fatalf("unknown string assertion: %q", str)
		}
	}

	// Otherwise it's an object with one key.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("failed to parse assertion: %v", err)
	}

	for key, val := range obj {
		switch key {
		case "row_count":
			var n int
			json.Unmarshal(val, &n)
			return assertRowCount(n)

		case "at_least_int":
			var n int64
			json.Unmarshal(val, &n)
			return assertAtLeastInt64(n)

		case "exact_int":
			var n int64
			json.Unmarshal(val, &n)
			return assertExactInt64(n)

		case "contains_node_with_prop":
			var pair [2]string
			json.Unmarshal(val, &pair)
			return assertContainsNodeWithProp(pair[0], pair[1])

		default:
			t.Fatalf("unknown assertion key: %q", key)
		}
	}

	t.Fatal("empty assertion object")
	return nil
}

// errFixtureRollback is returned to unconditionally roll back inline fixture data.
var errFixtureRollback = errors.New("fixture rollback")

// runReadOnly executes a test case against the pre-loaded dataset.
func runReadOnly(t *testing.T, ctx context.Context, db graph.Database, tc testCase, check func(*testing.T, graph.Result)) {
	t.Helper()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(tc.Cypher, nil)
		defer result.Close()
		check(t, result)
		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

// runWithFixture creates inline fixture data in a write transaction, runs the
// query, checks the assertion, then rolls back so the data doesn't persist.
func runWithFixture(t *testing.T, ctx context.Context, db graph.Database, tc testCase, check func(*testing.T, graph.Result)) {
	t.Helper()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := opengraph.WriteGraphTx(tx, tc.Fixture); err != nil {
			return fmt.Errorf("creating fixture: %w", err)
		}

		result := tx.Query(tc.Cypher, nil)
		defer result.Close()
		check(t, result)

		return errFixtureRollback
	})

	if !errors.Is(err, errFixtureRollback) {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// --- Assertion implementations ---

func assertNonEmpty(t *testing.T, result graph.Result) {
	t.Helper()
	if !result.Next() {
		if err := result.Error(); err != nil {
			t.Fatalf("query error: %v", err)
		}
		t.Fatal("expected non-empty result set")
	}
}

func assertEmpty(t *testing.T, result graph.Result) {
	t.Helper()
	if result.Next() {
		t.Fatal("expected empty result set but got rows")
	}
	if err := result.Error(); err != nil {
		t.Fatalf("query error: %v", err)
	}
}

func assertNoError(t *testing.T, result graph.Result) {
	t.Helper()
	for result.Next() {
	}
	if err := result.Error(); err != nil {
		t.Fatalf("query error: %v", err)
	}
}

func assertRowCount(n int) func(*testing.T, graph.Result) {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		count := 0
		for result.Next() {
			count++
		}
		if err := result.Error(); err != nil {
			t.Fatalf("query error: %v", err)
		}
		if count != n {
			t.Fatalf("row count: got %d, want %d", count, n)
		}
	}
}

func assertAtLeastInt64(min int64) func(*testing.T, graph.Result) {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		if !result.Next() {
			t.Fatal("no rows returned")
		}
		val, ok := result.Values()[0].(int64)
		if !ok {
			t.Fatalf("expected int64, got %T: %v", result.Values()[0], result.Values()[0])
		}
		if val < min {
			t.Fatalf("got %d, want >= %d", val, min)
		}
	}
}

func assertExactInt64(expected int64) func(*testing.T, graph.Result) {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		if !result.Next() {
			t.Fatal("no rows returned")
		}
		val, ok := result.Values()[0].(int64)
		if !ok {
			t.Fatalf("expected int64, got %T: %v", result.Values()[0], result.Values()[0])
		}
		if val != expected {
			t.Fatalf("got %d, want %d", val, expected)
		}
	}
}

func assertContainsNodeWithProp(key, expected string) func(*testing.T, graph.Result) {
	return func(t *testing.T, result graph.Result) {
		t.Helper()
		mapper := result.Mapper()
		for result.Next() {
			for _, rawVal := range result.Values() {
				var node graph.Node
				if mapper.Map(rawVal, &node) {
					if s, err := node.Properties.Get(key).String(); err == nil && s == expected {
						return
					}
				}
			}
		}
		if err := result.Error(); err != nil {
			t.Fatalf("query error: %v", err)
		}
		t.Fatalf("no row contains a node with %s = %q", key, expected)
	}
}

