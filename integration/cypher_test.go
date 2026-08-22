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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/testutil"
)

// caseFile represents one JSON test case file.
type caseFile struct {
	// Dataset selects the fixture dataset loaded before executing Cases.
	Dataset string `json:"dataset"`

	// Cases contains the queries and assertions decoded from this file.
	Cases []testCase `json:"cases"`
}

// testCase is a single test: a Cypher query and an assertion on its result.
// Cases with a "fixture" field run in a write transaction that rolls back,
// so the inline data doesn't persist.
type testCase struct {
	// Name identifies the case in test output.
	Name string `json:"name"`

	// Cypher is the statement executed by the case.
	Cypher string `json:"cypher"`

	// Params contains literal and generated query parameters.
	Params testutil.Params `json:"params,omitempty"`

	// NodeParams maps parameter names to fixture node identifiers.
	NodeParams map[string]string `json:"node_params,omitempty"`

	// NodeListParams maps parameter names to lists of fixture node identifiers.
	NodeListParams map[string][]string `json:"node_list_params,omitempty"`

	// Assert encodes the expected primary result assertion.
	Assert json.RawMessage `json:"assert"`

	// PostAssertions contains state checks executed after the primary result drains.
	PostAssertions []stateAssertion `json:"post_assertions,omitempty"`

	// Fixture optionally supplies inline graph data loaded in a rollback transaction.
	Fixture *opengraph.Graph `json:"fixture,omitempty"`
}

// stateAssertion runs after the primary query has been fully drained. It is
// executed in the same transaction and against the same fixture ID map.
type stateAssertion struct {
	// Name optionally identifies the assertion in diagnostics.
	Name string `json:"name,omitempty"`

	// Cypher is the state-inspection query executed after the primary query.
	Cypher string `json:"cypher"`

	// Params contains parameters for the state-inspection query.
	Params testutil.Params `json:"params,omitempty"`

	// Assert encodes the expected state-inspection result.
	Assert json.RawMessage `json:"assert"`
}

// TestCypher executes every fixture-backed case, grouping cases by dataset so each group shares one loaded graph.
func TestCypher(t *testing.T) {
	files, err := filepath.Glob("testdata/cases/*.json")
	if err != nil {
		t.Fatalf("failed to glob case files: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no case files found in testdata/cases/")
	}

	// group collects case files that share one fixture dataset.
	type group struct {
		// dataset names the fixture dataset shared by files.
		dataset string

		// files contains the parsed cases in the dataset group.
		files []caseFile
	}
	var (
		groups       = map[string]*group{}
		datasetNames []string
	)

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

	db, ctx := SetupDB(t, CleanupGraph, datasetNames...)

	for _, g := range groups {
		ClearGraph(t, db, ctx)
		datasetIDMap := LoadDataset(t, db, ctx, g.dataset)

		for _, cf := range g.files {
			for _, tc := range cf.Cases {
				t.Run(tc.Name, func(t *testing.T) {
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
//	"query_error"                         — drains result, expects an error
//	{"keys": ["a", "b"]}                  — every returned row has exactly these result keys, preserving order
//	{"row_count": N}                      — exactly N rows
//	{"at_least_int": N}                   — first scalar >= N
//	{"exact_int": N}                      — first scalar == N
//	{"scalar_values": [V...]}             — exact multiset of first scalar values, order-independent
//	{"ordered_scalar_values": [V...]}     — exact first scalar values, preserving row order
//	{"row_values": [[V...]]}              — exact multiset of scalar row values, order-independent
//	{"ordered_row_values": [[V...]]}      — exact scalar row values, preserving row order
//	{"contains_node_with_prop": [K, V]}   — some row has a node with property K=V
//	{"contains_node_with_props": {K: V}}  — some row has a node with all listed properties
//	{"contains_edge": {start,end,kind,props}} — some row/path has a relationship matching all listed fields
//	{"node_ids": ["a", "b"]}              — exact multiset of returned fixture node IDs, order-independent
//	{"node_id_set": ["a", "b"]}           — exact set of returned fixture node IDs, order-independent
//	{"node_records": [{id,kinds,props}]}     — exact returned nodes, including kinds and properties
//	{"relationship_triples": [{start,end,kind}]} — exact returned relationship triples
//	{"relationship_records": [{start,end,kind,props}]} — exact returned relationships and properties
//	{"ordered_node_ids": ["a", "b"]}      — first returned node ID per row, preserving row order
//	{"node_list_ids": [["a", "b"]]}       — exact multiset of returned node-list ID sequences
//	{"path_node_ids": [["a", "b"]]}       — exact multiset of returned path node ID sequences
//	{"path_lengths": [N...]}              — exact multiset of returned path edge counts
//	{"path_edge_kinds": [["K"...]]}       — exact multiset of returned path edge kind sequences
//	{"path_relationship_records": [[{start,end,kind,props}...]]} — exact ordered relationships for every returned path
//	{"relationship_list_kinds": [["K"...]]} — exact multiset of returned relationship-list kind sequences
//
// Object assertions may combine multiple keys; every assertion must pass.
func parseAssertion(t *testing.T, raw json.RawMessage) caseAssertion {
	t.Helper()

	// Try as a simple string first.
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		switch str {
		case "non_empty":
			return caseAssertion{check: assertNonEmpty}
		case "empty":
			return caseAssertion{check: assertEmpty}
		case "no_error":
			return caseAssertion{check: assertNoError}
		case "query_error":
			return caseAssertion{expectQueryError: true}
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
		case "keys":
			assertions = append(assertions, assertKeys(decodeAssertionValue[[]string](t, key, val)))

		case "row_count":
			assertions = append(assertions, assertRowCount(decodeAssertionValue[int](t, key, val)))

		case "at_least_int":
			assertions = append(assertions, assertAtLeastInt64(decodeAssertionValue[int64](t, key, val)))

		case "exact_int":
			assertions = append(assertions, assertExactInt64(decodeAssertionValue[int64](t, key, val)))

		case "scalar_values":
			assertions = append(assertions, assertScalarValues(decodeAssertionValue[[]any](t, key, val), false))

		case "ordered_scalar_values":
			assertions = append(assertions, assertScalarValues(decodeAssertionValue[[]any](t, key, val), true))

		case "row_values":
			assertions = append(assertions, assertRowValues(decodeAssertionValue[[][]any](t, key, val), false))

		case "ordered_row_values":
			assertions = append(assertions, assertRowValues(decodeAssertionValue[[][]any](t, key, val), true))

		case "contains_node_with_prop":
			pair := decodeAssertionValue[[2]string](t, key, val)
			assertions = append(assertions, assertContainsNodeWithProp(pair[0], pair[1]))

		case "contains_node_with_props":
			assertions = append(assertions, assertContainsNodeWithProps(decodeAssertionValue[map[string]any](t, key, val)))

		case "contains_edge":
			assertions = append(assertions, assertContainsEdge(decodeAssertionValue[edgeExpectation](t, key, val)))

		case "node_ids":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), false))

		case "node_id_set":
			assertions = append(assertions, assertNodeIDs(decodeAssertionValue[[]string](t, key, val), true))

		case "node_records":
			assertions = append(assertions, assertNodeRecords(decodeAssertionValue[[]nodeExpectation](t, key, val)))

		case "relationship_triples":
			assertions = append(assertions, assertRelationshipRecords(decodeAssertionValue[[]edgeExpectation](t, key, val), false))

		case "relationship_records":
			assertions = append(assertions, assertRelationshipRecords(decodeAssertionValue[[]edgeExpectation](t, key, val), true))

		case "ordered_node_ids":
			assertions = append(assertions, assertOrderedNodeIDs(decodeAssertionValue[[]string](t, key, val)))

		case "node_list_ids":
			assertions = append(assertions, assertNodeListIDs(decodeAssertionValue[[][]string](t, key, val)))

		case "path_node_ids":
			assertions = append(assertions, assertPathNodeIDs(decodeAssertionValue[[][]string](t, key, val)))

		case "path_lengths":
			assertions = append(assertions, assertPathLengths(decodeAssertionValue[[]int](t, key, val)))

		case "path_edge_kinds":
			assertions = append(assertions, assertPathEdgeKinds(decodeAssertionValue[[][]string](t, key, val)))

		case "path_relationship_records":
			assertions = append(assertions, assertPathRelationshipRecords(decodeAssertionValue[[][]edgeExpectation](t, key, val)))

		case "relationship_list_kinds":
			assertions = append(assertions, assertRelationshipListKinds(decodeAssertionValue[[][]string](t, key, val)))

		default:
			t.Fatalf("unknown assertion key: %q", key)
		}
	}

	if len(assertions) == 0 {
		t.Fatal("empty assertion object")
	}

	return caseAssertion{
		check: func(t *testing.T, result queryResult, ctx assertionContext) {
			t.Helper()

			for _, assertion := range assertions {
				assertion(t, result, ctx)
			}
		},
	}
}

// runReadOnly executes a test case against the pre-loaded dataset.
func runReadOnly(t *testing.T, ctx context.Context, db graph.Database, idMap opengraph.IDMap, tc testCase, assertion caseAssertion) {
	t.Helper()

	var (
		queryErrorObserved = false
		params             = resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, idMap)
		err                = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result := tx.Query(tc.Cypher, params)
			defer result.Close()
			assertion.checkResult(t, result, newAssertionContext(idMap))
			if assertion.expectQueryError {
				queryErrorObserved = true
			}
			return nil
		})
	)

	if err != nil {
		if assertion.expectQueryError && queryErrorObserved {
			return
		}

		t.Fatalf("transaction failed: %v", err)
	}
}

// runWithFixture creates inline fixture data in a write transaction, runs the
// query, checks the assertion, then rolls back so the data doesn't persist.
func runWithFixture(t *testing.T, ctx context.Context, db graph.Database, tc testCase, assertion caseAssertion) {
	t.Helper()

	queryErrorObserved := false
	session := &Session{
		DB:  db,
		Ctx: ctx,
	}
	err := session.WithRollbackFixture(t, tc.Fixture, true, func(tx graph.Transaction, idMap opengraph.IDMap) error {
		params := resolveFixtureParams(t, tc.Params, tc.NodeParams, tc.NodeListParams, idMap)
		result := tx.Query(tc.Cypher, params)
		assertion.checkResult(t, result, newAssertionContext(idMap))
		result.Close()
		if assertion.expectQueryError {
			queryErrorObserved = true
			return nil
		}

		return runStateAssertions(t, tx, idMap, tc.PostAssertions)
	})

	if assertion.expectQueryError && queryErrorObserved && err != nil {
		return
	}

	if err != nil {
		t.Fatalf("unexpected transaction error: %v", err)
	}
}

// resolveFixtureParams copies literal parameters and replaces fixture node
// references with their backend database IDs.
func resolveFixtureParams(
	t *testing.T,
	params map[string]any,
	nodeParams map[string]string,
	nodeListParams map[string][]string,
	idMap opengraph.IDMap,
) map[string]any {
	t.Helper()

	resolved := make(map[string]any, len(params)+len(nodeParams)+len(nodeListParams))
	for name, value := range params {
		resolved[name] = value
	}

	for paramName, fixtureID := range nodeParams {
		id, found := idMap[fixtureID]
		if !found {
			t.Fatalf("node parameter %q references unknown fixture ID %q", paramName, fixtureID)
		}
		resolved[paramName] = id.Int64()
	}

	for paramName, fixtureIDs := range nodeListParams {
		ids := make([]int64, len(fixtureIDs))
		for idx, fixtureID := range fixtureIDs {
			id, found := idMap[fixtureID]
			if !found {
				t.Fatalf("node list parameter %q references unknown fixture ID %q", paramName, fixtureID)
			}
			ids[idx] = id.Int64()
		}
		resolved[paramName] = ids
	}

	if len(resolved) == 0 {
		return nil
	}
	return resolved
}

// runStateAssertions executes and checks each postcondition query in the
// fixture's transaction.
func runStateAssertions(t *testing.T, tx graph.Transaction, idMap opengraph.IDMap, assertions []stateAssertion) error {
	t.Helper()

	for idx, spec := range assertions {
		name := spec.Name
		if name == "" {
			name = fmt.Sprintf("post assertion %d", idx+1)
		}
		if spec.Cypher == "" {
			t.Fatalf("%s has no Cypher query", name)
		}

		check := parseAssertion(t, spec.Assert)
		if check.expectQueryError {
			t.Fatalf("%s may not expect a query error", name)
		}

		result := tx.Query(spec.Cypher, spec.Params)
		check.checkResult(t, result, newAssertionContext(idMap))
		result.Close()
	}

	return nil
}

// --- Assertion implementations ---

// caseAssertion selects either a normalized result check or an expected query-error check.
type caseAssertion struct {
	// check validates a successfully drained result.
	check resultAssertion

	// expectQueryError selects the error path instead of invoking check.
	expectQueryError bool
}

// resultAssertion validates a normalized query result using fixture-aware identity mapping.
type resultAssertion func(*testing.T, queryResult, assertionContext)

// checkResult dispatches to the expected error path or the configured successful-result assertion.
func (s caseAssertion) checkResult(t *testing.T, result graph.Result, ctx assertionContext) {
	t.Helper()

	if s.expectQueryError {
		assertQueryError(t, result)
		return
	}

	if s.check == nil {
		t.Fatal("assertion has no result check")
	}

	s.check(t, collectResult(t, result), ctx)
}

// assertionContext translates backend database IDs back to stable fixture identifiers.
type assertionContext struct {
	// fixtureIDByID maps database node IDs to their source fixture IDs.
	fixtureIDByID map[graph.ID]string
}

// newAssertionContext reverses a fixture ID map for result assertions.
func newAssertionContext(idMap opengraph.IDMap) assertionContext {
	ctx := assertionContext{
		fixtureIDByID: make(map[graph.ID]string, len(idMap)),
	}

	for fixtureID, dbID := range idMap {
		ctx.fixtureIDByID[dbID] = fixtureID
	}

	return ctx
}

// fixtureID returns the stable fixture identifier for dbID and fails the current test if it is unknown.
func (s assertionContext) fixtureID(t *testing.T, dbID graph.ID) string {
	t.Helper()

	if fixtureID, found := s.fixtureIDByID[dbID]; found {
		return fixtureID
	}

	t.Fatalf("database node ID %d was not found in the assertion fixture ID map", dbID)
	return ""
}

// resultRow is an owned snapshot of one backend result row and its column names.
type resultRow struct {
	// keys contains the row's projected column names.
	keys []string

	// values contains an owned copy of the row's projected values.
	values []any
}

// queryResult contains drained rows and the backend mapper needed to decode graph values.
type queryResult struct {
	// rows contains every drained row in result order.
	rows []resultRow

	// mapper converts backend-specific values to graph-native representations.
	mapper graph.ValueMapper
}

// collectResult drains a backend result into owned rows and retains its value
// mapper for graph-value assertions.
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

// assertQueryError drains result and requires the backend to report an execution error.
func assertQueryError(t *testing.T, result graph.Result) {
	t.Helper()

	for result.Next() {
	}

	if err := result.Error(); err == nil {
		t.Fatal("expected query error but query completed successfully")
	}
}

// decodeAssertionValue decodes assertion JSON into T and fails the current test with the assertion key on error.
func decodeAssertionValue[T any](t *testing.T, key string, raw json.RawMessage) T {
	t.Helper()

	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("failed to decode assertion %q: %v", key, err)
	}

	return value
}

// assertNonEmpty requires at least one result row.
func assertNonEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) == 0 {
		t.Fatal("expected non-empty result set")
	}
}

// assertEmpty requires a result set with no rows.
func assertEmpty(t *testing.T, result queryResult, _ assertionContext) {
	t.Helper()
	if len(result.rows) > 0 {
		t.Fatalf("expected empty result set but got %d rows", len(result.rows))
	}
}

// assertNoError accepts any successfully collected result without imposing a row-shape assertion.
func assertNoError(t *testing.T, _ queryResult, _ assertionContext) {
	t.Helper()
}

// assertKeys requires every result row to expose exactly the expected projection keys in order.
func assertKeys(expected []string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		if len(result.rows) == 0 {
			t.Fatal("key assertion expected at least one row")
		}

		want := strings.Join(expected, "\x00")
		for rowIdx, row := range result.rows {
			if got := strings.Join(row.keys, "\x00"); got != want {
				t.Fatalf("row %d keys mismatch:\n  got:  %v\n  want: %v", rowIdx, row.keys, expected)
			}
		}
	}
}

// assertRowCount requires exactly n result rows.
func assertRowCount(n int) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()
		if count := len(result.rows); count != n {
			t.Fatalf("row count: got %d, want %d", count, n)
		}
	}
}

// assertAtLeastInt64 requires the first scalar result to be an integer no smaller than min.
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

// assertExactInt64 requires one row whose first scalar is exactly expected.
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

// assertScalarValues compares each row's first scalar with expected, optionally preserving row order.
func assertScalarValues(expected []any, ordered bool) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for rowIdx, row := range result.rows {
			if len(row.values) == 0 {
				t.Fatalf("row %d has no values", rowIdx)
			}

			got = append(got, scalarSignature(row.values[0]))
		}

		want := make([]string, len(expected))
		for idx, expectedValue := range expected {
			want[idx] = scalarSignature(expectedValue)
		}

		if ordered {
			if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("ordered scalar values mismatch:\n  got:  %v\n  want: %v", got, want)
			}
		} else {
			assertStringMultiset(t, got, want, "scalar values")
		}
	}
}

// assertRowValues compares complete scalar rows with expected, optionally preserving row order.
func assertRowValues(expected [][]any, ordered bool) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			got = append(got, rowScalarSignature(row.values))
		}

		want := make([]string, len(expected))
		for idx, expectedRow := range expected {
			want[idx] = rowScalarSignature(expectedRow)
		}

		if ordered {
			if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("ordered row values mismatch:\n  got:  %v\n  want: %v", got, want)
			}
		} else {
			assertStringMultiset(t, got, want, "row values")
		}
	}
}

// firstScalarValue returns the first projected value and fails for an empty
// result or row.
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

// asInt64 converts supported integer representations to int64 without accepting non-integral values.
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

// rowScalarSignature joins deterministic scalar signatures for one projected row.
func rowScalarSignature(values []any) string {
	parts := make([]string, len(values))
	for idx, value := range values {
		parts[idx] = scalarSignature(value)
	}

	encoded, err := json.Marshal(parts)
	if err != nil {
		return strings.Join(parts, "\x00")
	}

	return string(encoded)
}

// scalarSignature canonicalizes nil, numeric, string, boolean, and JSON-backed
// values for backend-independent comparisons.
func scalarSignature(value any) string {
	if value == nil {
		return "null:"
	}

	if number, ok := asFloat64(value); ok {
		return fmt.Sprintf("number:%g", number)
	}

	switch typedValue := value.(type) {
	case string:
		return "string:" + typedValue
	case bool:
		return fmt.Sprintf("bool:%t", typedValue)
	default:
		if encoded, err := json.Marshal(typedValue); err == nil {
			if signature, ok := jsonNumberSignature(encoded); ok {
				return signature
			}

			return fmt.Sprintf("json:%s", encoded)
		}

		return fmt.Sprintf("%T:%v", typedValue, typedValue)
	}
}

// jsonNumberSignature recognizes a JSON number and returns its canonical
// numeric signature.
func jsonNumberSignature(encoded []byte) (string, bool) {
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()

	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return "", false
	}

	number, ok := decoded.(json.Number)
	if !ok {
		return "", false
	}

	value, err := number.Float64()
	if err != nil {
		return "", false
	}

	return fmt.Sprintf("number:%g", value), true
}

// assertContainsNodeWithProp requires any returned node to contain key with the expected string value.
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

// assertContainsNodeWithProps requires any returned node to contain the expected property subset.
func assertContainsNodeWithProps(expected map[string]any) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var node graph.Node
				if result.mapper.Map(rawVal, &node) && propertiesMatch(node.Properties, expected) {
					return
				}

				var path graph.Path
				if result.mapper.Map(rawVal, &path) {
					for _, pathNode := range path.Nodes {
						if pathNode != nil && propertiesMatch(pathNode.Properties, expected) {
							return
						}
					}
				}
			}
		}

		t.Fatalf("no row contains a node with properties %v", expected)
	}
}

// edgeExpectation describes the stable identity, kind, and optional properties required of a relationship result.
type edgeExpectation struct {
	// Start is the expected fixture ID of the relationship's start node.
	Start string `json:"start,omitempty"`

	// End is the expected fixture ID of the relationship's end node.
	End string `json:"end,omitempty"`

	// Kind is the expected relationship kind.
	Kind string `json:"kind,omitempty"`

	// Props contains the expected relationship property subset.
	Props map[string]any `json:"props,omitempty"`
}

// nodeExpectation describes the stable identity, kinds, and optional properties required of a node result.
type nodeExpectation struct {
	// ID is the expected fixture node identifier.
	ID string `json:"id"`

	// Kinds contains the expected node kinds independent of order.
	Kinds []string `json:"kinds,omitempty"`

	// Props contains the expected node property subset.
	Props map[string]any `json:"props,omitempty"`
}

// assertContainsEdge requires any returned relationship to match expected endpoints, kind, and properties.
func assertContainsEdge(expected edgeExpectation) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		for _, relationship := range collectRelationships(t, result) {
			if relationshipMatches(t, relationship, expected, ctx) {
				return
			}
		}

		t.Fatalf("no row contains an edge matching %+v", expected)
	}
}

// assertNodeIDs compares collected fixture node IDs as a multiset, optionally deduplicating them first.
func assertNodeIDs(expected []string, unique bool) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := collectNodeIDs(t, result, ctx, unique)
		assertStringMultiset(t, got, expected, "node IDs")
	}
}

// assertNodeRecords compares returned nodes with expected fixture IDs, kinds, and property subsets independent of order.
func assertNodeRecords(expected []nodeExpectation) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(expected))
		for _, row := range result.rows {
			for _, rawValue := range row.values {
				var node graph.Node
				if result.mapper.Map(rawValue, &node) {
					got = append(got, nodeRecordSignature(t, node, ctx))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, node := range expected {
			want[idx] = expectedNodeRecordSignature(node)
		}

		assertStringMultiset(t, got, want, "node records")
	}
}

// assertRelationshipRecords compares returned relationships with expected records, optionally including properties.
func assertRelationshipRecords(expected []edgeExpectation, includeProperties bool) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		relationships := collectRelationships(t, result)
		got := make([]string, len(relationships))
		for idx, relationship := range relationships {
			got[idx] = relationshipRecordSignature(t, relationship, ctx, includeProperties)
		}

		want := make([]string, len(expected))
		for idx, relationship := range expected {
			want[idx] = expectedRelationshipRecordSignature(relationship, includeProperties)
		}

		assertStringMultiset(t, got, want, "relationship records")
	}
}

// assertPathRelationshipRecords compares the ordered relationship record sequence in each returned path.
func assertPathRelationshipRecords(expected [][]edgeExpectation) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		paths := collectPaths(t, result)
		got := make([]string, len(paths))
		for idx, path := range paths {
			got[idx] = pathRelationshipRecordSignature(t, path, ctx)
		}
		want := make([]string, len(expected))
		for pathIdx, relationships := range expected {
			parts := make([]string, len(relationships))
			for relationshipIdx, relationship := range relationships {
				parts[relationshipIdx] = expectedRelationshipRecordSignature(relationship, true)
			}
			want[pathIdx] = strings.Join(parts, "\x02")
		}

		assertStringMultiset(t, got, want, "ordered path relationship records")
	}
}

// assertOrderedNodeIDs compares fixture node IDs in result-row order.
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

// assertNodeListIDs compares each returned node-list projection by its ordered fixture IDs.
func assertNodeListIDs(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, ctx assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var nodes []*graph.Node
				if result.mapper.Map(rawVal, &nodes) {
					got = append(got, nodeListIDSignature(t, nodes, ctx))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, nodeIDs := range expected {
			want[idx] = strings.Join(nodeIDs, "->")
		}

		assertStringMultiset(t, got, want, "node-list ID sequences")
	}
}

// collectNodeIDs maps every returned node to a fixture ID, optionally removing
// duplicates while preserving first occurrence order.
func collectNodeIDs(t *testing.T, result queryResult, ctx assertionContext, unique bool) []string {
	t.Helper()

	var (
		ids  = make([]string, 0, len(result.rows))
		seen = map[string]bool{}
	)

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

// nodeListIDSignature renders a node slice as an ordered fixture-ID sequence.
func nodeListIDSignature(t *testing.T, nodes []*graph.Node, ctx assertionContext) string {
	t.Helper()

	nodeIDs := make([]string, len(nodes))
	for idx, node := range nodes {
		if node == nil {
			t.Fatalf("node list contains nil node at index %d", idx)
		}

		nodeIDs[idx] = ctx.fixtureID(t, node.ID)
	}

	return strings.Join(nodeIDs, "->")
}

// assertPathNodeIDs compares each returned path by its ordered fixture node IDs.
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

// assertPathLengths compares the relationship count of every returned path.
func assertPathLengths(expected []int) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			got = append(got, fmt.Sprintf("%d", len(path.Edges)))
		}

		want := make([]string, len(expected))
		for idx, expectedLength := range expected {
			want[idx] = fmt.Sprintf("%d", expectedLength)
		}

		assertStringMultiset(t, got, want, "path lengths")
	}
}

// assertPathEdgeKinds compares each returned path by its ordered relationship kinds.
func assertPathEdgeKinds(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, path := range collectPaths(t, result) {
			got = append(got, pathEdgeKindSignature(t, path))
		}

		want := make([]string, len(expected))
		for idx, expectedKinds := range expected {
			want[idx] = strings.Join(expectedKinds, "->")
		}

		assertStringMultiset(t, got, want, "path edge kind sequences")
	}
}

// assertRelationshipListKinds compares each relationship-list projection by ordered kind names.
func assertRelationshipListKinds(expected [][]string) resultAssertion {
	return func(t *testing.T, result queryResult, _ assertionContext) {
		t.Helper()

		got := make([]string, 0, len(result.rows))
		for _, row := range result.rows {
			for _, rawVal := range row.values {
				var relationshipPointers []*graph.Relationship
				if result.mapper.Map(rawVal, &relationshipPointers) {
					got = append(got, relationshipListKindSignature(t, relationshipPointers))
					continue
				}

				var relationships []graph.Relationship
				if result.mapper.Map(rawVal, &relationships) {
					got = append(got, relationshipValueListKindSignature(t, relationships))
				}
			}
		}

		want := make([]string, len(expected))
		for idx, expectedKinds := range expected {
			want[idx] = strings.Join(expectedKinds, "->")
		}

		assertStringMultiset(t, got, want, "relationship-list kind sequences")
	}
}

// pathNodeIDSignature renders a path as an ordered fixture-node-ID sequence.
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

// pathEdgeKindSignature renders a path as an ordered relationship-kind sequence.
func pathEdgeKindSignature(t *testing.T, path graph.Path) string {
	t.Helper()

	edgeKinds := make([]string, len(path.Edges))
	for idx, edge := range path.Edges {
		if edge == nil {
			t.Fatalf("path contains nil edge at index %d", idx)
		}

		if edge.Kind == nil {
			t.Fatalf("path edge at index %d has nil kind", idx)
		}

		edgeKinds[idx] = edge.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

// relationshipListKindSignature renders a relationship-pointer slice as an ordered kind sequence.
func relationshipListKindSignature(t *testing.T, relationships []*graph.Relationship) string {
	t.Helper()

	edgeKinds := make([]string, len(relationships))
	for idx, relationship := range relationships {
		if relationship == nil {
			t.Fatalf("relationship list contains nil relationship at index %d", idx)
		}

		if relationship.Kind == nil {
			t.Fatalf("relationship list item at index %d has nil kind", idx)
		}

		edgeKinds[idx] = relationship.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

// relationshipValueListKindSignature renders a relationship-value slice as an ordered kind sequence.
func relationshipValueListKindSignature(t *testing.T, relationships []graph.Relationship) string {
	t.Helper()

	edgeKinds := make([]string, len(relationships))
	for idx, relationship := range relationships {
		if relationship.Kind == nil {
			t.Fatalf("relationship list item at index %d has nil kind", idx)
		}

		edgeKinds[idx] = relationship.Kind.String()
	}

	return strings.Join(edgeKinds, "->")
}

// collectPaths maps every path-valued result cell into a graph path.
func collectPaths(t *testing.T, result queryResult) []graph.Path {
	t.Helper()

	var paths []graph.Path
	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var path graph.Path
			if result.mapper.Map(rawVal, &path) {
				paths = append(paths, path)
			}
		}
	}

	return paths
}

// collectRelationships maps standalone relationships and relationships nested
// in returned paths into one slice.
func collectRelationships(t *testing.T, result queryResult) []graph.Relationship {
	t.Helper()

	var relationships []graph.Relationship
	for _, row := range result.rows {
		for _, rawVal := range row.values {
			var relationship graph.Relationship
			if result.mapper.Map(rawVal, &relationship) {
				relationships = append(relationships, relationship)
			}

			var path graph.Path
			if result.mapper.Map(rawVal, &path) {
				for _, pathRelationship := range path.Edges {
					if pathRelationship != nil {
						relationships = append(relationships, *pathRelationship)
					}
				}
			}
		}
	}

	return relationships
}

// nodeRecordSignature renders a returned node into a stable fixture ID, sorted kinds, and property signature.
func nodeRecordSignature(t *testing.T, node graph.Node, ctx assertionContext) string {
	t.Helper()

	kinds := node.Kinds.Strings()
	sort.Strings(kinds)

	return strings.Join([]string{
		ctx.fixtureID(t, node.ID),
		strings.Join(kinds, ","),
		propertyMapSignature(node.Properties.MapOrEmpty()),
	}, "\x00")
}

// expectedNodeRecordSignature renders a node expectation in the same canonical form as a returned node.
func expectedNodeRecordSignature(node nodeExpectation) string {
	kinds := append([]string(nil), node.Kinds...)
	sort.Strings(kinds)

	return strings.Join([]string{
		node.ID,
		strings.Join(kinds, ","),
		propertyMapSignature(node.Props),
	}, "\x00")
}

// relationshipRecordSignature renders a returned relationship into canonical fixture endpoints, kind, and optional properties.
func relationshipRecordSignature(t *testing.T, relationship graph.Relationship, ctx assertionContext, includeProperties bool) string {
	t.Helper()

	kind := ""
	if relationship.Kind != nil {
		kind = relationship.Kind.String()
	}

	parts := []string{
		ctx.fixtureID(t, relationship.StartID),
		ctx.fixtureID(t, relationship.EndID),
		kind,
	}
	if includeProperties {
		parts = append(parts, propertyMapSignature(relationship.Properties.MapOrEmpty()))
	}

	return strings.Join(parts, "\x00")
}

// pathRelationshipRecordSignature renders a path's ordered relationships into one canonical comparison value.
func pathRelationshipRecordSignature(t *testing.T, path graph.Path, ctx assertionContext) string {
	t.Helper()

	parts := make([]string, 0, len(path.Edges))
	for _, relationship := range path.Edges {
		if relationship == nil {
			t.Fatal("path contains a nil relationship")
		}
		parts = append(parts, relationshipRecordSignature(t, *relationship, ctx, true))
	}
	return strings.Join(parts, "\x02")
}

// expectedRelationshipRecordSignature renders a relationship expectation in the same canonical form as a returned relationship.
func expectedRelationshipRecordSignature(relationship edgeExpectation, includeProperties bool) string {
	parts := []string{relationship.Start, relationship.End, relationship.Kind}
	if includeProperties {
		parts = append(parts, propertyMapSignature(relationship.Props))
	}

	return strings.Join(parts, "\x00")
}

// propertyMapSignature renders properties in key order using canonical scalar
// signatures.
func propertyMapSignature(properties map[string]any) string {
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, len(keys))
	for idx, key := range keys {
		parts[idx] = key + "=" + scalarSignature(properties[key])
	}

	return strings.Join(parts, "\x01")
}

// relationshipMatches reports whether a relationship satisfies the expected
// fixture endpoints, kind, and property subset.
func relationshipMatches(t *testing.T, relationship graph.Relationship, expected edgeExpectation, ctx assertionContext) bool {
	t.Helper()

	if expected.Start != "" && ctx.fixtureID(t, relationship.StartID) != expected.Start {
		return false
	}

	if expected.End != "" && ctx.fixtureID(t, relationship.EndID) != expected.End {
		return false
	}

	if expected.Kind != "" {
		if relationship.Kind == nil || relationship.Kind.String() != expected.Kind {
			return false
		}
	}

	return propertiesMatch(relationship.Properties, expected.Props)
}

// propertiesMatch reports whether properties contains every expected key with an equivalent value.
func propertiesMatch(properties *graph.Properties, expected map[string]any) bool {
	if len(expected) == 0 {
		return true
	}

	if properties == nil {
		return false
	}

	for key, expectedValue := range expected {
		actualValue := properties.Get(key).Any()
		if !valuesEqual(actualValue, expectedValue) {
			return false
		}
	}

	return true
}

// valuesEqual compares numeric values across concrete widths and delegates all other values to deep equality.
func valuesEqual(actual, expected any) bool {
	if actualNumber, actualIsNumber := asFloat64(actual); actualIsNumber {
		if expectedNumber, expectedIsNumber := asFloat64(expected); expectedIsNumber {
			return actualNumber == expectedNumber
		}
	}

	return reflect.DeepEqual(actual, expected)
}

// asFloat64 converts supported numeric representations to a common comparison value.
func asFloat64(value any) (float64, bool) {
	switch typedValue := value.(type) {
	case int:
		return float64(typedValue), true
	case int8:
		return float64(typedValue), true
	case int16:
		return float64(typedValue), true
	case int32:
		return float64(typedValue), true
	case int64:
		return float64(typedValue), true
	case uint:
		return float64(typedValue), true
	case uint8:
		return float64(typedValue), true
	case uint16:
		return float64(typedValue), true
	case uint32:
		return float64(typedValue), true
	case uint64:
		return float64(typedValue), true
	case float32:
		return float64(typedValue), true
	case float64:
		return typedValue, true
	default:
		return 0, false
	}
}

// assertStringMultiset compares string collections after sorting copies and reports label on mismatch.
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
