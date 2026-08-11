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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

// errScaleWriteRollback signals the intentional rollback used to isolate a measured write.
var errScaleWriteRollback = errors.New("scale write rollback")

// resolvedWriteScenario contains a write scenario after symbolic fixture parameters are resolved.
type resolvedWriteScenario struct {
	// SelectionCypher contains the write-selection Cypher statement.
	SelectionCypher string
	// SelectionParams contains resolved parameters for the write-selection query.
	SelectionParams map[string]any
	// AffectedEntity identifies the entity class counted after a write.
	AffectedEntity string
	// ExpectedMatched sets the required number of matched entities.
	ExpectedMatched int64
	// ExpectedAffected sets the required number of affected entities.
	ExpectedAffected int64
	// PostState defines the state query evaluated after a write.
	PostState []resolvedStateQuery
}

// resolvedStateQuery contains a post-write state query after fixture parameters are resolved.
type resolvedStateQuery struct {
	// Name labels the post-write state assertion in diagnostics and results.
	Name string
	// Cypher contains the Cypher statement under test.
	Cypher string
	// Params supplies literal query parameters.
	Params map[string]any
	// Expected defines the required observable result.
	Expected ExpectedResult
}

// writeMeasurement captures a write's matched and affected counts, duration, and post-state observations.
type writeMeasurement struct {
	// Matched records entities matched by the write selection.
	Matched int64
	// Affected records entities changed by the measured write.
	Affected int64
	// Duration records elapsed time for this observation.
	Duration time.Duration
	// PostState contains the observed results of post-write validation queries.
	PostState []StateQueryResult
}

// countCypherRows executes a Cypher query and returns the number of result rows.
func countCypherRows(tx graph.Transaction, cypher string, params map[string]any) (int64, error) {
	result := tx.Query(cypher, params)
	defer result.Close()

	var rowCount int64
	for result.Next() {
		rowCount++
	}

	if err := result.Error(); err != nil {
		return 0, err
	}

	return rowCount, nil
}

// countRawRows executes a raw backend query and returns the number of result rows.
func countRawRows(tx graph.Transaction, sql string, params map[string]any) (int64, error) {
	result := tx.Raw(sql, params)
	defer result.Close()

	var rowCount int64
	for result.Next() {
		rowCount++
	}

	if err := result.Error(); err != nil {
		return 0, err
	}

	return rowCount, nil
}

// stableNodeObservation serializes a node using fixture-stable identity, kinds, and properties.
type stableNodeObservation struct {
	// Identity contains the stable fixture identity emitted in observations.
	Identity string `json:"identity"`
	// Kinds lists stable node kinds in deterministic observation order.
	Kinds []string `json:"kinds,omitempty"`
	// Properties contains normalized property values.
	Properties map[string]any `json:"properties,omitempty"`
}

// stableRelationshipObservation serializes a relationship using stable endpoints, kind, identity, and properties.
type stableRelationshipObservation struct {
	// Identity contains the stable fixture identity emitted in observations.
	Identity string `json:"identity,omitempty"`
	// Start contains the stable identity of the relationship's start node.
	Start string `json:"start"`
	// End contains the stable identity of the relationship's end node.
	End string `json:"end"`
	// Kind names the relationship kind preserved in the stable observation.
	Kind string `json:"kind"`
	// Properties contains normalized property values.
	Properties map[string]any `json:"properties,omitempty"`
}

// stablePathObservation serializes an ordered path as stable node and relationship observations.
type stablePathObservation struct {
	// Nodes contains the stable node sequence.
	Nodes []stableNodeObservation `json:"nodes"`
	// Relationships contains the ordered stable relationship sequence in the path.
	Relationships []stableRelationshipObservation `json:"relationships"`
}

// reverseIDMap inverts fixture node-key mappings for stable result serialization.
func reverseIDMap(idMap opengraph.IDMap) map[graph.ID]string {
	reversed := make(map[graph.ID]string, len(idMap))
	for name, id := range idMap {
		reversed[id] = name
	}
	return reversed
}

// stableIdentity maps a database identifier to its fixture key, falling back to its decimal representation.
func stableIdentity(id graph.ID, reversed map[graph.ID]string) string {
	if name, found := reversed[id]; found {
		return name
	}
	return fmt.Sprintf("unmapped-node:%d", id)
}

// stableProperties returns properties with database identifiers replaced by stable fixture keys.
func stableProperties(properties *graph.Properties) map[string]any {
	if properties == nil {
		return nil
	}
	return properties.Map
}

// stableNode converts a backend node to a fixture-stable serialized observation.
func stableNode(node *graph.Node, reversed map[graph.ID]string) stableNodeObservation {
	kinds := node.Kinds.Strings()
	sort.Strings(kinds)
	return stableNodeObservation{
		Identity:   stableIdentity(node.ID, reversed),
		Kinds:      kinds,
		Properties: stableProperties(node.Properties),
	}
}

// stableRelationship converts a backend relationship to stable endpoints, kind, identity, and properties.
func stableRelationship(relationship *graph.Relationship, reversed map[graph.ID]string) stableRelationshipObservation {
	kind := ""
	if relationship.Kind != nil {
		kind = relationship.Kind.String()
	}
	identity := ""
	if relationship.Properties != nil {
		if logicalKey, err := relationship.Properties.Get("logical_key").String(); err == nil {
			identity = logicalKey
		}
	}
	return stableRelationshipObservation{
		Identity:   identity,
		Start:      stableIdentity(relationship.StartID, reversed),
		End:        stableIdentity(relationship.EndID, reversed),
		Kind:       kind,
		Properties: stableProperties(relationship.Properties),
	}
}

// stablePath converts a backend path to stable ordered node and relationship observations.
func stablePath(path graph.Path, reversed map[graph.ID]string) (stablePathObservation, error) {
	observation := stablePathObservation{
		Nodes:         make([]stableNodeObservation, len(path.Nodes)),
		Relationships: make([]stableRelationshipObservation, len(path.Edges)),
	}
	for idx, node := range path.Nodes {
		observation.Nodes[idx] = stableNode(node, reversed)
	}
	seenRelationships := make(map[graph.ID]struct{}, len(path.Edges))
	for idx, relationship := range path.Edges {
		if _, duplicate := seenRelationships[relationship.ID]; duplicate {
			return stablePathObservation{}, fmt.Errorf("path reuses relationship ID %d", relationship.ID)
		}
		seenRelationships[relationship.ID] = struct{}{}
		observation.Relationships[idx] = stableRelationship(relationship, reversed)
	}
	return observation, nil
}

// stableRowValues normalizes result values to stable scalar IDs or canonical path JSON.
func stableRowValues(values []any, mapper graph.ValueMapper, reversed map[graph.ID]string, scalarNodeIDs bool, pathValues bool) ([]any, error) {
	stable := make([]any, len(values))
	for idx, value := range values {
		switch typed := value.(type) {
		case *graph.Node:
			stable[idx] = stableNode(typed, reversed)
		case graph.Node:
			stable[idx] = stableNode(&typed, reversed)
		case *graph.Relationship:
			stable[idx] = stableRelationship(typed, reversed)
		case graph.Relationship:
			stable[idx] = stableRelationship(&typed, reversed)
		case graph.Path:
			path, err := stablePath(typed, reversed)
			if err != nil {
				return nil, err
			}
			stable[idx] = path
		case *graph.Path:
			path, err := stablePath(*typed, reversed)
			if err != nil {
				return nil, err
			}
			stable[idx] = path
		default:
			var relationship graph.Relationship
			if mapper.Map(value, &relationship) {
				stable[idx] = stableRelationship(&relationship, reversed)
				continue
			}

			var node graph.Node
			if mapper.Map(value, &node) {
				stable[idx] = stableNode(&node, reversed)
				continue
			}

			// The PostgreSQL path mapper accepts a map without path fields as an
			// empty path, so only attempt this mapping when the result contract
			// says the row contains paths.
			if pathValues {
				var path graph.Path
				if mapper.Map(value, &path) {
					observation, err := stablePath(path, reversed)
					if err != nil {
						return nil, err
					}
					stable[idx] = observation
					continue
				}
			}

			if scalarNodeIDs {
				if id, ok := scaleInt64(value); ok {
					stable[idx] = stableIdentity(graph.ID(id), reversed)
					continue
				}
			}
			stable[idx] = value
		}
	}
	return stable, nil
}

// expectedPathRows serializes expected paths to the same canonical representation as observed paths.
func expectedPathRows(rows []ExpectedPath) ([]string, error) {
	encoded := make([]string, len(rows))
	for idx, row := range rows {
		value, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		encoded[idx] = string(value)
	}

	sort.Strings(encoded)
	return encoded, nil
}

// observedPathRows extracts and sorts canonical path observations from normalized rows.
func observedPathRows(rows []string) ([]string, error) {
	encoded := make([]string, len(rows))
	for idx, row := range rows {
		var values []json.RawMessage
		if err := json.Unmarshal([]byte(row), &values); err != nil {
			return nil, err
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("expected one path column, got %d", len(values))
		}
		var path stablePathObservation
		if err := json.Unmarshal(values[0], &path); err != nil {
			return nil, err
		}
		signature := ExpectedPath{
			Nodes:             make([]string, len(path.Nodes)),
			RelationshipKinds: make([]string, len(path.Relationships)),
		}
		includeRelationshipKeys := false
		for _, relationship := range path.Relationships {
			includeRelationshipKeys = includeRelationshipKeys || relationship.Identity != ""
		}
		if includeRelationshipKeys {
			signature.RelationshipKeys = make([]string, len(path.Relationships))
		}
		for nodeIdx, node := range path.Nodes {
			signature.Nodes[nodeIdx] = node.Identity
		}
		for relationshipIdx, relationship := range path.Relationships {
			signature.RelationshipKinds[relationshipIdx] = relationship.Kind
			if includeRelationshipKeys {
				signature.RelationshipKeys[relationshipIdx] = relationship.Identity
			}
		}
		value, err := json.Marshal(signature)
		if err != nil {
			return nil, err
		}
		encoded[idx] = string(value)
	}
	sort.Strings(encoded)
	return encoded, nil
}

// observeCypherRows executes Cypher and returns row count plus normalized observations.
func observeCypherRows(tx graph.Transaction, cypher string, params map[string]any, idMap opengraph.IDMap, scalarNodeIDs bool, pathValues bool) (int64, []string, error) {
	result := tx.Query(cypher, params)
	return observeResultRows(result, idMap, scalarNodeIDs, pathValues)
}

// observeRawRows executes raw SQL and returns row count plus normalized observations.
func observeRawRows(tx graph.Transaction, sql string, params map[string]any, idMap opengraph.IDMap, scalarNodeIDs bool, pathValues bool) (int64, []string, error) {
	result := tx.Raw(sql, params)
	return observeResultRows(result, idMap, scalarNodeIDs, pathValues)
}

// observeResultRows drains a result iterator into a count and sorted stable observations.
func observeResultRows(result graph.Result, idMap opengraph.IDMap, scalarNodeIDs bool, pathValues bool) (int64, []string, error) {
	defer result.Close()

	var (
		rowCount int64
		rows     []string
	)
	for result.Next() {
		rowCount++
		stableValues, err := stableRowValues(result.Values(), result.Mapper(), reverseIDMap(idMap), scalarNodeIDs, pathValues)
		if err != nil {
			return 0, nil, fmt.Errorf("stabilize observed row %d: %w", rowCount, err)
		}
		encoded, err := json.Marshal(stableValues)
		if err != nil {
			return 0, nil, fmt.Errorf("encode observed row %d: %w", rowCount, err)
		}
		rows = append(rows, string(encoded))
	}
	if err := result.Error(); err != nil {
		return 0, nil, err
	}

	// Cypher does not promise row order without ORDER BY. Comparing sorted row
	// encodings preserves multiplicity while avoiding a false mismatch when an
	// otherwise identical plan returns rows in another order.
	sort.Strings(rows)
	return rowCount, rows, nil
}

// validateExpectedObservations compares normalized rows with explicit scalar, ID-row, or path expectations.
func validateExpectedObservations(expected ExpectedResult, observed []string) error {
	if len(expected.IDRows) > 0 {
		expectedRows := make([]string, len(expected.IDRows))
		for idx, row := range expected.IDRows {
			encoded, err := json.Marshal(row)
			if err != nil {
				return err
			}
			expectedRows[idx] = string(encoded)
		}
		sort.Strings(expectedRows)
		if !slices.Equal(expectedRows, observed) {
			return fmt.Errorf("stable ID rows differ: expected=%v observed=%v", expectedRows, observed)
		}
	}
	if len(expected.PathRows) > 0 {
		expectedRows, err := expectedPathRows(expected.PathRows)
		if err != nil {
			return err
		}
		observedRows, err := observedPathRows(observed)
		if err != nil {
			return err
		}
		if !slices.Equal(expectedRows, observedRows) {
			return fmt.Errorf("stable path rows differ: expected=%v observed=%v", expectedRows, observedRows)
		}
	}
	if expected.ScalarInt != nil {
		expectedRow := fmt.Sprintf("[%d]", *expected.ScalarInt)
		if len(observed) != 1 || observed[0] != expectedRow {
			return fmt.Errorf("scalar result differs: expected=%s observed=%v", expectedRow, observed)
		}
	}
	return nil
}

// observeCypher runs a Cypher query in a read transaction and returns stable observations.
func observeCypher(tx graph.Transaction, cypher string, params map[string]any) (StateQueryResult, error) {
	result := tx.Query(cypher, params)
	defer result.Close()

	var observation StateQueryResult
	for result.Next() {
		observation.RowCount++
		if observation.RowCount == 1 && len(result.Values()) > 0 {
			if scalar, ok := scaleInt64(result.Values()[0]); ok {
				observation.ScalarInt = &scalar
			}
		}
	}

	if err := result.Error(); err != nil {
		return StateQueryResult{}, err
	}

	return observation, nil
}

// resultContainsNodeIDs reports whether the expected result kind requires stable node-identifier mapping.
func resultContainsNodeIDs(expected ExpectedResult) bool {
	return expected.ResultKind == "id_set" || expected.ResultKind == "id_rows"
}

// resultContainsPaths reports whether expected observations require canonical path normalization.
func resultContainsPaths(expected ExpectedResult) bool {
	return expected.ResultKind == "path_set"
}

// measureCypher executes cypher and records its timing observations.
func measureCypher(ctx context.Context, db graph.Database, cypher string, params map[string]any, expected ExpectedResult, idMap opengraph.IDMap, iterations int) (int64, []string, DurationStats, error) {
	return measureCypherWithWarmups(ctx, db, cypher, params, expected, idMap, 0, iterations)
}

// measureCypherWithWarmups executes cypher with warmups and records its timing observations.
func measureCypherWithWarmups(ctx context.Context, db graph.Database, cypher string, params map[string]any, expected ExpectedResult, idMap opengraph.IDMap, warmupIterations, iterations int) (int64, []string, DurationStats, error) {
	return measureReadWithWarmups(ctx, db, cypher, params, expected, idMap, warmupIterations, iterations, false)
}

// measureRawSQLWithWarmups executes raw SQL with warmups and records its timing observations.
func measureRawSQLWithWarmups(ctx context.Context, db graph.Database, sql string, params map[string]any, expected ExpectedResult, idMap opengraph.IDMap, warmupIterations, iterations int) (int64, []string, DurationStats, error) {
	return measureReadWithWarmups(ctx, db, sql, params, expected, idMap, warmupIterations, iterations, true)
}

// measureReadWithWarmups executes read with warmups and records its timing observations.
func measureReadWithWarmups(ctx context.Context, db graph.Database, query string, params map[string]any, expected ExpectedResult, idMap opengraph.IDMap, warmupIterations, iterations int, raw bool) (int64, []string, DurationStats, error) {
	if iterations < 1 {
		return 0, nil, DurationStats{}, fmt.Errorf("iterations must be at least 1")
	}
	if warmupIterations < 0 {
		return 0, nil, DurationStats{}, fmt.Errorf("warmup iterations must not be negative")
	}

	coldStart := time.Now()
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, err := countReadRows(tx, query, params, raw)
		return err
	}); err != nil {
		return 0, nil, DurationStats{}, err
	}
	coldDuration := time.Since(coldStart)
	for range warmupIterations {
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := countReadRows(tx, query, params, raw)
			return err
		}); err != nil {
			return 0, nil, DurationStats{}, err
		}
	}

	var (
		warmupRows        int64
		preflightObserved []string
		stabilizeNodeIDs  = resultContainsNodeIDs(expected)
		stabilizePaths    = resultContainsPaths(expected)
	)
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		warmupRows, preflightObserved, err = observeReadRows(tx, query, params, idMap, stabilizeNodeIDs, stabilizePaths, raw)
		return err
	}); err != nil {
		return 0, nil, DurationStats{}, err
	}

	durations := make([]time.Duration, iterations)
	for idx := range iterations {
		start := time.Now()
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := countReadRows(tx, query, params, raw)
			return err
		}); err != nil {
			return 0, nil, DurationStats{}, err
		}
		durations[idx] = time.Since(start)
	}

	var (
		postflightRows     int64
		postflightObserved []string
	)
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		postflightRows, postflightObserved, err = observeReadRows(tx, query, params, idMap, stabilizeNodeIDs, stabilizePaths, raw)
		return err
	}); err != nil {
		return 0, nil, DurationStats{}, err
	}
	if postflightRows != warmupRows {
		return 0, nil, DurationStats{}, fmt.Errorf("postflight row count changed: preflight=%d postflight=%d", warmupRows, postflightRows)
	}
	if !slices.Equal(preflightObserved, postflightObserved) {
		return 0, nil, DurationStats{}, fmt.Errorf("postflight result changed despite stable row count")
	}
	if err := validateExpectedObservations(expected, preflightObserved); err != nil {
		return 0, nil, DurationStats{}, err
	}

	stats, err := computeDurationStats(durations)
	if err != nil {
		return 0, nil, DurationStats{}, err
	}
	stats.WarmupIterations = warmupIterations

	stats.Samples = append([]LatencySample{{
		Round:          1,
		Iteration:      0,
		Classification: "cold",
		Duration:       coldDuration,
	}}, stats.Samples...)

	return warmupRows, preflightObserved, stats, nil
}

// countReadRows dispatches to raw SQL or Cypher row counting according to raw.
func countReadRows(tx graph.Transaction, query string, params map[string]any, raw bool) (int64, error) {
	if raw {
		return countRawRows(tx, query, params)
	}
	return countCypherRows(tx, query, params)
}

// observeReadRows dispatches a read observation to raw SQL or Cypher execution.
func observeReadRows(tx graph.Transaction, query string, params map[string]any, idMap opengraph.IDMap, scalarNodeIDs, pathValues, raw bool) (int64, []string, error) {
	if raw {
		return observeRawRows(tx, query, params, idMap, scalarNodeIDs, pathValues)
	}
	return observeCypherRows(tx, query, params, idMap, scalarNodeIDs, pathValues)
}

// measureWriteCypher executes write cypher and records its timing observations.
func measureWriteCypher(
	ctx context.Context,
	db graph.Database,
	cypher string,
	params map[string]any,
	scenario resolvedWriteScenario,
	iterations int,
) (writeMeasurement, DurationStats, error) {
	return measureWriteCypherWithWarmups(ctx, db, cypher, params, scenario, 0, iterations)
}

// measureWriteCypherWithWarmups executes write cypher with warmups and records its timing observations.
func measureWriteCypherWithWarmups(
	ctx context.Context,
	db graph.Database,
	cypher string,
	params map[string]any,
	scenario resolvedWriteScenario,
	warmupIterations int,
	iterations int,
) (writeMeasurement, DurationStats, error) {
	if iterations < 1 {
		return writeMeasurement{}, DurationStats{}, fmt.Errorf("iterations must be at least 1")
	}
	if warmupIterations < 0 {
		return writeMeasurement{}, DurationStats{}, fmt.Errorf("warmup iterations must not be negative")
	}

	// The first untimed execution remains the cold diagnostic. Additional
	// configured warmups are also untimed and must preserve its semantics.
	warmup, err := measureWriteIteration(ctx, db, cypher, params, scenario)
	if err != nil {
		return writeMeasurement{}, DurationStats{}, err
	}
	for idx := 0; idx < warmupIterations; idx++ {
		next, err := measureWriteIteration(ctx, db, cypher, params, scenario)
		if err != nil {
			return writeMeasurement{}, DurationStats{}, err
		}
		if next.Matched != warmup.Matched || next.Affected != warmup.Affected {
			return writeMeasurement{}, DurationStats{}, fmt.Errorf("warm-up iteration %d changed cardinality", idx+1)
		}
	}

	durations := make([]time.Duration, iterations)
	for idx := range iterations {
		measurement, err := measureWriteIteration(ctx, db, cypher, params, scenario)
		if err != nil {
			return writeMeasurement{}, DurationStats{}, err
		}
		if measurement.Matched != warmup.Matched || measurement.Affected != warmup.Affected {
			return writeMeasurement{}, DurationStats{}, fmt.Errorf(
				"write iteration %d changed cardinality: matched=%d affected=%d, warm-up matched=%d affected=%d",
				idx+1,
				measurement.Matched,
				measurement.Affected,
				warmup.Matched,
				warmup.Affected,
			)
		}
		durations[idx] = measurement.Duration
	}

	stats, err := computeDurationStats(durations)
	if err != nil {
		return writeMeasurement{}, DurationStats{}, err
	}
	stats.WarmupIterations = warmupIterations

	stats.Samples = append([]LatencySample{{
		Round:          1,
		Iteration:      0,
		Classification: "cold",
		Duration:       warmup.Duration,
	}}, stats.Samples...)

	return warmup, stats, nil
}

// measureWriteIteration executes write iteration and records its timing observations.
func measureWriteIteration(
	ctx context.Context,
	db graph.Database,
	cypher string,
	params map[string]any,
	scenario resolvedWriteScenario,
) (writeMeasurement, error) {
	var measurement writeMeasurement

	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		matched, err := countCypherRows(tx, scenario.SelectionCypher, scenario.SelectionParams)
		if err != nil {
			return fmt.Errorf("count matched rows: %w", err)
		}
		measurement.Matched = matched
		if matched != scenario.ExpectedMatched {
			return fmt.Errorf("expected %d matched rows, got %d", scenario.ExpectedMatched, matched)
		}

		before, err := countAffectedEntities(tx, scenario.AffectedEntity)
		if err != nil {
			return err
		}

		start := time.Now()
		if _, err := countCypherRows(tx, cypher, params); err != nil {
			return fmt.Errorf("execute mutation: %w", err)
		}
		measurement.Duration = time.Since(start)

		after, err := countAffectedEntities(tx, scenario.AffectedEntity)
		if err != nil {
			return err
		}
		measurement.Affected = before - after
		if measurement.Affected != scenario.ExpectedAffected {
			return fmt.Errorf("expected %d affected %ss, got %d", scenario.ExpectedAffected, scenario.AffectedEntity, measurement.Affected)
		}

		for _, stateQuery := range scenario.PostState {
			observation, err := observeCypher(tx, stateQuery.Cypher, stateQuery.Params)
			if err != nil {
				return fmt.Errorf("post-state %q: %w", stateQuery.Name, err)
			}
			observation.Name = stateQuery.Name
			if err := checkStateExpectation(observation, stateQuery.Expected); err != nil {
				return fmt.Errorf("post-state %q: %w", stateQuery.Name, err)
			}
			measurement.PostState = append(measurement.PostState, observation)
		}

		return errScaleWriteRollback
	})
	if errors.Is(err, errScaleWriteRollback) {
		return measurement, nil
	}
	if err != nil {
		return writeMeasurement{}, err
	}

	return writeMeasurement{}, fmt.Errorf("write scenario committed instead of rolling back")
}

// countAffectedEntities returns the transaction-visible node or relationship count selected by entity.
func countAffectedEntities(tx graph.Transaction, entity string) (int64, error) {
	switch entity {
	case "node":
		return tx.Nodes().Count()
	case "relationship":
		return tx.Relationships().Count()
	default:
		return 0, fmt.Errorf("unsupported affected entity %q", entity)
	}
}

// checkStateExpectation validates a post-write observation against its declared row-count and scalar expectations.
func checkStateExpectation(observation StateQueryResult, expected ExpectedResult) error {
	if expected.RowCount != nil && observation.RowCount != *expected.RowCount {
		return fmt.Errorf("expected %d rows, got %d", *expected.RowCount, observation.RowCount)
	}
	if expected.ScalarInt != nil {
		if observation.ScalarInt == nil {
			return fmt.Errorf("expected scalar integer %d, got no integer scalar", *expected.ScalarInt)
		}
		if *observation.ScalarInt != *expected.ScalarInt {
			return fmt.Errorf("expected scalar integer %d, got %d", *expected.ScalarInt, *observation.ScalarInt)
		}
	}

	return nil
}

// scaleInt64 converts supported integral numeric representations to int64 without unsigned overflow.
func scaleInt64(value any) (int64, bool) {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue), true
	case int32:
		return int64(typedValue), true
	case int64:
		return typedValue, true
	case uint:
		if uint64(typedValue) <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case uint32:
		return int64(typedValue), true
	case uint64:
		if typedValue <= math.MaxInt64 {
			return int64(typedValue), true
		}
	case float64:
		if math.Trunc(typedValue) == typedValue {
			return int64(typedValue), true
		}
	}

	return 0, false
}
