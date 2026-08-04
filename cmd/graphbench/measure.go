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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/specterops/dawgs/graph"
)

var errScaleWriteRollback = errors.New("scale write rollback")

type resolvedWriteScenario struct {
	SelectionCypher  string
	SelectionParams  map[string]any
	AffectedEntity   string
	ExpectedMatched  int64
	ExpectedAffected int64
	PostState        []resolvedStateQuery
}

type resolvedStateQuery struct {
	Name     string
	Cypher   string
	Params   map[string]any
	Expected ExpectedResult
}

type writeMeasurement struct {
	Matched   int64
	Affected  int64
	Duration  time.Duration
	PostState []StateQueryResult
}

func countCypherRows(tx graph.Transaction, cypher string, params map[string]any) (int64, error) {
	result := tx.Query(cypher, params)
	defer result.Close()

	var rowCount int64
	for result.Next() {
		rowCount++
	}

	return rowCount, result.Error()
}

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

	return observation, result.Error()
}

func measureCypher(ctx context.Context, db graph.Database, cypher string, params map[string]any, iterations int) (int64, DurationStats, error) {
	if iterations < 1 {
		return 0, DurationStats{}, fmt.Errorf("iterations must be at least 1")
	}

	var warmupRows int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		warmupRows, err = countCypherRows(tx, cypher, params)
		return err
	}); err != nil {
		return 0, DurationStats{}, err
	}

	durations := make([]time.Duration, iterations)
	for idx := range iterations {
		start := time.Now()
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, err := countCypherRows(tx, cypher, params)
			return err
		}); err != nil {
			return 0, DurationStats{}, err
		}
		durations[idx] = time.Since(start)
	}

	stats, err := computeDurationStats(durations)
	if err != nil {
		return 0, DurationStats{}, err
	}

	return warmupRows, stats, nil
}

func measureWriteCypher(
	ctx context.Context,
	db graph.Database,
	cypher string,
	params map[string]any,
	scenario resolvedWriteScenario,
	iterations int,
) (writeMeasurement, DurationStats, error) {
	if iterations < 1 {
		return writeMeasurement{}, DurationStats{}, fmt.Errorf("iterations must be at least 1")
	}

	warmup, err := measureWriteIteration(ctx, db, cypher, params, scenario)
	if err != nil {
		return writeMeasurement{}, DurationStats{}, err
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

	return warmup, stats, nil
}

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
