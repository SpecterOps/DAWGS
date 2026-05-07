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
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
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
		datasetIDMap := LoadDataset(t, db, ctx, g.dataset)

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
						runReadOnly(t, ctx, db, datasetIDMap, tc, check)
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
//	{"node_ids": ["a", "b"]}              — exact multiset of returned fixture node IDs, order-independent
//	{"node_id_set": ["a", "b"]}           — exact set of returned fixture node IDs, order-independent
//	{"ordered_node_ids": ["a", "b"]}      — first returned node ID per row, preserving row order
//	{"path_node_ids": [["a", "b"]]}       — exact multiset of returned path node ID sequences
//
// Object assertions may combine multiple keys; every assertion must pass.
func parseAssertion(t *testing.T, raw json.RawMessage) resultAssertion {
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

	// Otherwise it's an object with one or more assertions.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("failed to parse assertion: %v", err)
	}

	var assertions []resultAssertion
	for key, val := range obj {
		switch key {
		case "row_count":
			assertions = append(assertions, assertRowCount(decodeAssertionValue[int](t, key, val)))

		case "at_least_int":
			assertions = append(assertions, assertAtLeastInt64(decodeAssertionValue[int64](t, key, val)))

		case "exact_int":
			assertions = append(assertions, assertExactInt64(decodeAssertionValue[int64](t, key, val)))

		case "contains_node_with_prop":
			pair := decodeAssertionValue[[2]string](t, key, val)
			assertions = append(assertions, assertContainsNodeWithProp(pair[0], pair[1]))

		case "node_ids":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), false))

		case "node_id_set":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), true))

		case "ordered_node_ids":
			assertions = append(assertions, assertOrderedNodeIDs(decodeAssertionValue[[]string](t, key, val)))

		case "path_node_ids":
			assertions = append(assertions, assertPathNodeIDs(decodeAssertionValue[[][]string](t, key, val)))

		default:
			t.Fatalf("unknown assertion key: %q", key)
		}
	}

	if len(assertions) == 0 {
		t.Fatal("empty assertion object")
	}

	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		for _, assertion := range assertions {
			assertion(t, result, ctx)
		}
	}
}

// errFixtureRollback is returned to unconditionally roll back inline fixture data.
var errFixtureRollback = errors.New("fixture rollback")

// runReadOnly executes a test case against the pre-loaded dataset.
func runReadOnly(t *testing.T, ctx context.Context, db graph.Database, idMap opengraph.IDMap, tc testCase, check resultAssertion) {
	t.Helper()

	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(tc.Cypher, nil)
		defer result.Close()
		check(t, collectResult(t, result), newAssertionContext(idMap))
		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

// runWithFixture creates inline fixture data in a write transaction, runs the
// query, checks the assertion, then rolls back so the data doesn't persist.
func runWithFixture(t *testing.T, ctx context.Context, db graph.Database, tc testCase, check resultAssertion) {
	t.Helper()

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if err := tx.Nodes().Delete(); err != nil {
			return fmt.Errorf("clearing graph before fixture: %w", err)
		}

		idMap, err := opengraph.WriteGraphTx(tx, tc.Fixture)
		if err != nil {
			return fmt.Errorf("creating fixture: %w", err)
		}

		result := tx.Query(tc.Cypher, nil)
		defer result.Close()
		check(t, collectResult(t, result), newAssertionContext(idMap))

		return errFixtureRollback
	})

	if !errors.Is(err, errFixtureRollback) {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// --- Assertion implementations ---

type resultAssertion func(*testing.T, queryResult, assertionContext)

type assertionContext struct {
	fixtureIDByID map[graph.ID]string
}

func newAssertionContext(idMap opengraph.IDMap) assertionContext {
	ctx := assertionContext{
		fixtureIDByID: make(map[graph.ID]string, len(idMap)),
	}

	for fixtureID, dbID := range idMap {
		ctx.fixtureIDByID[dbID] = fixtureID
	}

	return ctx
}

func (s assertionContext) fixtureID(t *testing.T, dbID graph.ID) string {
	t.Helper()

	if fixtureID, found := s.fixtureIDByID[dbID]; found {
		return fixtureID
	}

	t.Fatalf("database node ID %d was not found in the assertion fixture ID map", dbID)
	return ""
}

type resultRow struct {
	keys   []string
	values []any
}

type queryResult struct {
	rows   []resultRow
	mapper graph.ValueMapper
}

func collectResult(t *testing.T, result graph.Result) queryResult {
	t.Helper()

	collected := queryResult{
		mapper: result.Mapper(),
	}

	for result.Next() {
		collected.rows = append(collected.rows, resultRow{
			keys:   append([]string(nil), result.Keys()...),
			values: append([]any(nil), result.Values()...),
		})
	}

	if err := result.Error(); err != nil {
		t.Fatalf("query error: %v", err)
	}

	return collected
}

func decodeAssertionValue[T any](t *testing.T, key string, raw json.RawMessage) T {
	t.Helper()

	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("failed to decode assertion %q: %v", key, err)
	}

	return value
}

func assertNonEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) == 0 {
		t.Fatal("expected non-empty result set")
	}
}

func assertEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) > 0 {
		t.Fatalf("expected empty result set but got %d rows", len(result.rows))
	}
}

func assertNoError(t *testing.T, _ queryResult, _ assertionContext) {
	t.Helper()
}

func assertRowCount(n int) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if count := len(result.rows); count != n {
			t.Fatalf("row count: got %d, want %d", count, n)
		}
	}
}

func assertAtLeastInt64(min int64) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if len(result.rows) == 0 {
			t.Fatal("no rows returned")
		}
		val, ok := asInt64(firstScalarValue(t, result))
		if !ok {
			value := firstScalarValue(t, result)
			t.Fatalf("expected integer, got %T: %v", value, value)
		}
		if val < min {
			t.Fatalf("got %d, want >= %d", val, min)
		}
	}
}

func assertExactInt64(expected int64) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if len(result.rows) != 1 {
			t.Fatalf("exact integer assertion expected one row, got %d", len(result.rows))
		}
		val, ok := asInt64(firstScalarValue(t, result))
		if !ok {
			value := firstScalarValue(t, result)
			t.Fatalf("expected integer, got %T: %v", value, value)
		}
		if val != expected {
			t.Fatalf("got %d, want %d", val, expected)
		}
	}
}

func firstScalarValue(t *testing.T, result queryResult) any {
	t.Helper()

	if len(result.rows) == 0 {
		t.Fatal("no rows returned")
	}

	if len(result.rows[0].values) == 0 {
		t.Fatal("first row has no values")
	}

	return result.rows[0].values[0]
}

func asInt64(value any) (int64, bool) {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue), true
	case int8:
		return int64(typedValue), true
	case int16:
		return int64(typedValue), true
	case int32:
		return int64(typedValue), true
	case int64:
		return typedValue, true
	case uint:
		if uint64(typedValue) <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case uint8:
		return int64(typedValue), true
	case uint16:
		return int64(typedValue), true
	case uint32:
		return int64(typedValue), true
	case uint64:
		if typedValue <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case float32:
		if math.Trunc(float64(typedValue)) == float64(typedValue) {
			return int64(typedValue), true
		}
	case float64:
		if math.Trunc(typedValue) == typedValue {
			return int64(typedValue), true
		}
	}

	return 0, false
}

func assertContainsNodeWithProp(key, expected string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) {
					if s, err := node.Properties.Get(key).String(); err == nil && s == expected {
						return
					}
				}
			}
		}
		t.Fatalf("no row contains a node with %s = %q", key, expected)
	}
}

func assertNodeIDs(expected []string, unique bool) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := collectNodeIDs(t, result, ctx, unique)
		assertStringMultiset(t, got, expected, "node IDs")
	}
}

func assertOrderedNodeIDs(expected []string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for rowIdx, row := range result.rows {
			var found bool
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) {
					got = append(got, ctx.fixtureID(t, node.ID))
					found = true
					break
				}
			}

			if !found {
				t.Fatalf("row %d did not contain a node value", rowIdx)
			}
		}

		if strings.Join(got, "\x00") != strings.Join(expected, "\x00") {
			t.Fatalf("ordered node IDs mismatch:\n  got:  %v\n  want: %v", got, expected)
		}
	}
}

func collectNodeIDs(t *testing.T, result queryResult, ctx assertionContext, unique bool) []string {
	t.Helper()

	ids := make([]string, 0, len(result.rows))
	seen := map[string]bool{}

	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var node graph.Node
			if result.mapper.Map(rawVal, &node) {
				fixtureID := ctx.fixtureID(t, node.ID)
				if unique {
					if seen[fixtureID] {
						continue
					}
					seen[fixtureID] = true
				}

				ids = append(ids, fixtureID)
			}
		}
	}

	return ids
}

func assertPathNodeIDs(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var path graph.Path
				if result.mapper.Map(rawVal, &path) {
					got = append(got, pathNodeIDSignature(t, path, ctx))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, pathNodeIDs := range expected {
			want[idx] = strings.Join(pathNodeIDs, "->")
		}

		assertStringMultiset(t, got, want, "path node ID sequences")
	}
}

func pathNodeIDSignature(t *testing.T, path graph.Path, ctx assertionContext) string {
	t.Helper()

	nodeIDs := make([]string, len(path.Nodes))
	for idx, node := range path.Nodes {
		if node == nil {
			t.Fatalf("path contains nil node at index %d", idx)
		}

		nodeIDs[idx] = ctx.fixtureID(t, node.ID)
	}

	return strings.Join(nodeIDs, "->")
}

func assertStringMultiset(t *testing.T, got, expected []string, label string) {
	t.Helper()

	got = append([]string(nil), got...)
	expected = append([]string(nil), expected...)

	sort.Strings(got)
	sort.Strings(expected)

	if len(got) != len(expected) {
		t.Fatalf("%s count: got %d, want %d\n  got:  %v\n  want: %v", label, len(got), len(expected), got, expected)
	}

	for idx := range got {
		if got[idx] != expected[idx] {
			t.Fatalf("%s mismatch at index %d:\n  got:  %v\n  want: %v", label, idx, got, expected)
		}
	}
}
