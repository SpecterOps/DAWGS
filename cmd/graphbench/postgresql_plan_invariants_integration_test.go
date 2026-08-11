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

package main

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

// postgresPlanNodeLoops extracts Actual Loops for every EXPLAIN node with the requested alias, allowing integration assertions to detect repeated execution.
func postgresPlanNodeLoops(t *testing.T, raw json.RawMessage, alias string) []int64 {
	t.Helper()
	var document []map[string]any
	require.NoError(t, json.Unmarshal(raw, &document))
	require.NotEmpty(t, document)
	root, ok := document[0]["Plan"].(map[string]any)
	require.True(t, ok)

	var (
		loops []int64
		walk  func(map[string]any)
	)

	walk = func(node map[string]any) {
		nodeAlias, _ := node["Alias"].(string)
		functionName, _ := node["Function Name"].(string)
		if nodeAlias == alias || functionName == alias {
			if actualLoops, ok := node["Actual Loops"].(float64); ok {
				loops = append(loops, int64(actualLoops))
			}
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				walk(childNode)
			}
		}
	}
	walk(root)
	return loops
}

// TestPostgreSQLScalePlanInvariants verifies analyzed-plan capture, indexed anchors, correct mutation targets, and preserved branch-local predicates across required scale representatives.
func TestPostgreSQLScalePlanInvariants(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	required := scaleCorpusRequiredIDSet()
	filtered := ScaleCorpus{}
	for _, testCase := range corpus.Cases {
		id := scaleCorpusCaseID(testCase.Name)
		_, isRequired := required[id]
		if isRequired || id == "TRUST-03" {
			filtered.Cases = append(filtered.Cases, testCase)
		}
	}

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, filtered, 1, 1, nil, true, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, runner.Close(ctx))
	})

	records, err := runner.Run(ctx, 1, 1, filtered)
	require.NoError(t, err)
	require.Len(t, records, len(filtered.Cases))

	byID := map[string][]CaseResult{}
	for _, record := range records {
		record := record
		id := scaleCorpusCaseID(record.Name)
		byID[id] = append(byID[id], record)

		t.Run(record.Name, func(t *testing.T) {
			require.Equal(t, StatusOK, record.Status, record.Error)
			require.NotEmpty(t, record.SQL)
			require.NotEmpty(t, record.PostgresPlan)
			require.NotNil(t, record.PostgresMetrics)
			require.NotNil(t, record.PostgresMetrics.PlanningMS)
			require.NotNil(t, record.PostgresMetrics.ExecutionMS)
			require.NotNil(t, record.Optimization)

			plan := strings.Join(record.PostgresPlan, "\n")
			require.Contains(t, plan, "actual rows=", "plan must come from EXPLAIN ANALYZE")
			assertMutationPlanTarget(t, id, plan)
			assertAnchorPlanIndex(t, id, plan)
		})
	}

	for _, id := range scaleCorpusRequiredIDs {
		require.NotEmpty(t, byID[id], "missing PostgreSQL plan-invariant execution for %s", id)
	}

	t.Run("LOGIC-01 branch-local direction and kind plan", func(t *testing.T) {
		record := requireSingleScaleRecord(t, byID, "TRUST-03")
		normalizedSQL := strings.ToLower(record.SQL)
		require.Contains(t, normalizedSQL, " or ")
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, "kind_id"), 2)
		require.Contains(t, normalizedSQL, "start_id")
		require.Contains(t, normalizedSQL, "end_id")
	})

	t.Run("LOGIC-02 cross-binding temporal plan", func(t *testing.T) {
		record := requireSingleScaleRecord(t, byID, "TRUST-01")
		normalizedSQL := strings.ToLower(record.SQL)
		require.Contains(t, normalizedSQL, " or ")
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, "lastcollected"), 2)
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, " < "), 2)
	})

	t.Run("LOGIC-04 filtered mutation targets", func(t *testing.T) {
		edgeDelete := requireSingleScaleRecord(t, byID, "REC-01")
		nodeDelete := requireSingleScaleRecord(t, byID, "REC-08")
		require.Contains(t, strings.Join(edgeDelete.PostgresPlan, "\n"), "Delete on edge")
		require.Contains(t, strings.Join(nodeDelete.PostgresPlan, "\n"), "Delete on node")
	})
}

// TestPostgreSQLZeroLengthShortestMaterializersAreExact verifies that all search-and-hydration references reproduce a singleton zero-edge path and hydration-only arms avoid recursive search.
func TestPostgreSQLZeroLengthShortestMaterializersAreExact(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	var (
		zeroDepth = 0
		oneDepth  = 1
		oneRow    = int64(1)
	)
	testCase := ScaleCase{
		Name:     "GSP-D00-F001_path",
		Dataset:  "generated_shortest_paths_d1_f1",
		Category: "generated_shortest_path",
		Cypher:   "MATCH p = shortestPath((s)-[:Traverse*0..1]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN p",
		NodeParams: map[string]string{
			"start_id": "sp-start",
			"end_id":   "sp-start",
		},
		Expected: ExpectedResult{
			RowCount:   &oneRow,
			ResultKind: "path_set",
			PathRows: []ExpectedPath{{
				Nodes:             []string{"sp-start"},
				RelationshipKinds: []string{},
			}},
		},
		Observes: ObservedValues{
			Paths:         true,
			Nodes:         true,
			Relationships: true,
			Properties:    true,
		},
		Shape: WorkloadShape{
			RootPredicate:               "bound_id",
			TerminalPredicate:           "bound_id",
			EdgeKinds:                   []string{"Traverse"},
			MinDepth:                    &zeroDepth,
			MaxDepth:                    &oneDepth,
			PathMaterializationRequired: true,
		},
		CandidateModes: []ExecutionMode{ModePostgresSQL},
	}
	corpus := ScaleCorpus{Cases: []ScaleCase{testCase}}

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, corpus, 1, 1, nil, true, nil, "", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, runner.Close(ctx))
	})

	records, err := runner.Run(ctx, 0, 1, corpus)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.Equal(t, oneRow, record.RowCount)

	for _, name := range []string{
		"m0_directed_hydration_only",
		"m1_ordered_ids_hydration_only",
		"s3_unidirectional_cte_m0_directed",
		"s3_unidirectional_cte_m1_ordered_ids",
	} {
		reference := requirePostgresReference(t, record.PostgresReferences, name)
		require.Equal(t, oneRow, reference.RowCount)
		require.Equal(t, record.ObservedRows, reference.ObservedRows)
		if strings.Contains(name, "hydration_only") {
			require.NotContains(t, reference.SQL, "with recursive")
		}
	}
}

// TestPostgreSQLForcedShortestDistanceEndpointSemantics verifies zero-depth identity, missing-root emptiness, and the minimum-depth self-endpoint error under forced distance execution.
func TestPostgreSQLForcedShortestDistanceEndpointSemantics(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	var (
		zeroDepth  = 0
		oneDepth   = 1
		oneRow     = int64(1)
		zeroRows   = int64(0)
		zeroScalar = int64(0)
		maxDepth   = 1
	)
	baseShape := WorkloadShape{
		RootPredicate:               "bound_id",
		TerminalPredicate:           "bound_id",
		EdgeKinds:                   []string{"Traverse"},
		MaxDepth:                    &maxDepth,
		PathMaterializationRequired: false,
	}
	zeroShape := baseShape
	zeroShape.MinDepth = &zeroDepth
	oneShape := baseShape
	oneShape.MinDepth = &oneDepth

	corpus := ScaleCorpus{
		Cases: []ScaleCase{
			{
				Name:       "forced-shortest-zero-depth",
				Dataset:    "generated_shortest_paths_d1_f1",
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((s)-[:Traverse*0..1]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)",
				NodeParams: map[string]string{"start_id": "sp-start", "end_id": "sp-start"},
				Expected: ExpectedResult{
					RowCount:   &oneRow,
					ScalarInt:  &zeroScalar,
					ResultKind: "scalar",
				},
				Shape:          zeroShape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{
				Name:       "forced-shortest-missing-root",
				Dataset:    "generated_shortest_paths_d1_f1",
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((s)-[:Traverse*1..1]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)",
				Params:     testutil.Params{"start_id": int64(9223372036854775807)},
				NodeParams: map[string]string{"end_id": "sp-end"},
				Expected: ExpectedResult{
					RowCount: &zeroRows,
				},
				Shape:          oneShape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{
				Name:       "forced-shortest-min-one-same-endpoint",
				Dataset:    "generated_shortest_paths_d1_f1",
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((s)-[:Traverse*1..1]->(e)) WHERE id(s) = $start_id AND id(e) = $end_id RETURN length(p)",
				NodeParams: map[string]string{"start_id": "sp-start", "end_id": "sp-start"},
				Expected: ExpectedResult{
					RowCount: &zeroRows,
				},
				Shape:          oneShape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
		},
	}

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, corpus, 1, 1, nil, false, nil, "SP-S3-U-D", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 1, corpus)
	require.NoError(t, err)
	require.Len(t, records, 3)
	require.Equal(t, StatusOK, records[0].Status, records[0].Error)
	require.Equal(t, []string{"[0]"}, records[0].ObservedRows)
	require.Equal(t, StatusOK, records[1].Status, records[1].Error)
	require.Equal(t, zeroRows, records[1].RowCount)
	require.Equal(t, StatusError, records[2].Status)
	require.Contains(t, records[2].Error, "shortest path")
}

// TestPostgreSQLForcedShortestDirectPreflightSkipsAndFallsBackExactly verifies that one-hop direct hits bypass the recursive harness while longer paths invoke it and preserve exact ordered path output.
func TestPostgreSQLForcedShortestDirectPreflightSkipsAndFallsBackExactly(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	oneRow := int64(1)
	minDepth, maxDepth := 1, 3
	dataset := "generated_shortest_paths_v2_d3_o2_r1_fo2_fi128_l2_k7_t16_w2_x16_p0_c1_s1"
	shape := WorkloadShape{
		RootPredicate:               "bound_id",
		TerminalPredicate:           "bound_id",
		EdgeKinds:                   []string{"Traverse"},
		Direction:                   "inbound",
		RelationshipKindCount:       1,
		MinDepth:                    &minDepth,
		MaxDepth:                    &maxDepth,
		PathMaterializationRequired: true,
	}
	multiKindMaxDepth := 2
	multiKindShape := WorkloadShape{
		RootPredicate:               "bound_id",
		TerminalPredicate:           "bound_id",
		EdgeKinds:                   []string{"ParallelKind00", "ParallelKind01", "ParallelKind02", "ParallelKind03", "ParallelKind04", "ParallelKind05", "ParallelKind06"},
		Direction:                   "outbound",
		RelationshipKindCount:       7,
		MinDepth:                    &minDepth,
		MaxDepth:                    &multiKindMaxDepth,
		PathMaterializationRequired: true,
	}
	corpus := ScaleCorpus{
		Cases: []ScaleCase{
			{
				Name:       "direct-hit",
				Dataset:    dataset,
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((root)<-[:Traverse*1..3]-(terminal)) WHERE id(root) = $root_id AND id(terminal) = $end_id RETURN p",
				NodeParams: map[string]string{"root_id": "sp-v2-inbound-root", "end_id": "sp-v2-inbound-linear-01"},
				Expected: ExpectedResult{
					RowCount:   &oneRow,
					ResultKind: "path_set",
					PathRows: []ExpectedPath{{
						Nodes:             []string{"sp-v2-inbound-root", "sp-v2-inbound-linear-01"},
						RelationshipKinds: []string{"Traverse"},
						RelationshipKeys:  []string{"inbound-primary-03"},
					}},
				},
				Observes: ObservedValues{
					Paths:         true,
					Nodes:         true,
					Relationships: true,
					Properties:    true,
				},
				Shape:          shape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{
				Name:       "fallback-hit",
				Dataset:    dataset,
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((root)<-[:Traverse*1..3]-(terminal)) WHERE id(root) = $root_id AND id(terminal) = $end_id RETURN p",
				NodeParams: map[string]string{"root_id": "sp-v2-inbound-root", "end_id": "sp-v2-inbound-end"},
				Expected: ExpectedResult{
					RowCount:   &oneRow,
					ResultKind: "path_set",
					PathRows: []ExpectedPath{{
						Nodes:             []string{"sp-v2-inbound-root", "sp-v2-inbound-linear-01", "sp-v2-inbound-linear-02", "sp-v2-inbound-end"},
						RelationshipKinds: []string{"Traverse", "Traverse", "Traverse"},
						RelationshipKeys:  []string{"inbound-primary-03", "inbound-primary-02", "inbound-primary-01"},
					}},
				},
				Observes: ObservedValues{
					Paths:         true,
					Nodes:         true,
					Relationships: true,
					Properties:    true,
				},
				Shape:          shape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
			{
				Name:       "direct-multi-kind",
				Dataset:    dataset,
				Category:   "generated_shortest_path",
				Cypher:     "MATCH p = shortestPath((root)-[:ParallelKind00|ParallelKind01|ParallelKind02|ParallelKind03|ParallelKind04|ParallelKind05|ParallelKind06*1..2]->(terminal)) WHERE id(root) = $root_id AND id(terminal) = $end_id RETURN p",
				NodeParams: map[string]string{"root_id": "sp-v2-parallel-start", "end_id": "sp-v2-parallel-target-000000"},
				Expected: ExpectedResult{
					RowCount:   &oneRow,
					ResultKind: "path_set",
					PathRows: []ExpectedPath{{
						Nodes:             []string{"sp-v2-parallel-start", "sp-v2-parallel-target-000000"},
						RelationshipKinds: []string{"ParallelKind00"},
						RelationshipKeys:  []string{"parallel-k00-t000000"},
					}},
				},
				Observes: ObservedValues{
					Paths:         true,
					Nodes:         true,
					Relationships: true,
					Properties:    true,
				},
				Shape:          multiKindShape,
				CandidateModes: []ExecutionMode{ModePostgresSQL},
			},
		},
	}

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, corpus, 1, 1, nil, false, nil, "SP-S0-DIRECT", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 1, corpus)
	require.NoError(t, err)
	require.Len(t, records, 3)
	for _, record := range records {
		require.Equal(t, StatusOK, record.Status, "%s: %s", record.Name, record.Error)
		require.Equal(t, oneRow, record.RowCount)
		require.NotEmpty(t, record.PostgresPlanJSON)
	}

	directLoops := postgresPlanNodeLoops(t, records[0].PostgresPlanJSON, "bidirectional_sp_harness")
	require.NotEmpty(t, directLoops)
	require.Equal(t, int64(0), directLoops[0], records[0].PostgresPlan)
	fallbackLoops := postgresPlanNodeLoops(t, records[1].PostgresPlanJSON, "bidirectional_sp_harness")
	require.NotEmpty(t, fallbackLoops)
	require.Positive(t, fallbackLoops[0], records[1].PostgresPlan)
	multiKindLoops := postgresPlanNodeLoops(t, records[2].PostgresPlanJSON, "bidirectional_sp_harness")
	require.NotEmpty(t, multiKindLoops)
	require.Equal(t, int64(0), multiKindLoops[0], records[2].PostgresPlan)
}

// TestPostgreSQLForcedShortestDistanceCancellationReusesSession verifies prompt timeout cancellation, rollback recovery on the same backend PID, and successful replay of forced distance SQL.
func TestPostgreSQLForcedShortestDistanceCancellationReusesSession(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"GSP-D64-F1000_distance"}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "SP-S3-U-D", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, StatusOK, records[0].Status, records[0].Error)

	translation, sqlQuery, err := runner.translateCypher(ctx, selected.Cases[0].Cypher, records[0].Params)
	require.NoError(t, err)
	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}, pgx.NamedArgs(translation.Parameters)}

	connectionHandle, err := runner.pool.Acquire(ctx)
	require.NoError(t, err)
	defer connectionHandle.Release()
	backendPID := connectionHandle.Conn().PgConn().PID()

	tx, err := connectionHandle.BeginTx(ctx, postgresConcurrencyTxOptions())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "set local statement_timeout = '1ms'")
	require.NoError(t, err)
	started := time.Now()
	rows, queryErr := tx.Query(ctx, sqlQuery, queryArgs...)
	if queryErr == nil {
		for rows.Next() {
			_, queryErr = rows.Values()
			if queryErr != nil {
				break
			}
		}
		rows.Close()
		if queryErr == nil {
			queryErr = rows.Err()
		}
	}
	cancellationLatency := time.Since(started)
	var postgresError *pgconn.PgError
	require.ErrorAs(t, queryErr, &postgresError)
	require.Equal(t, "57014", postgresError.Code)
	require.Less(t, cancellationLatency, 250*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))

	var reusedPID uint32
	require.NoError(t, connectionHandle.QueryRow(ctx, "select pg_backend_pid()").Scan(&reusedPID))
	require.Equal(t, backendPID, reusedPID)

	rows, err = connectionHandle.Query(ctx, sqlQuery, queryArgs...)
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		_, err = rows.Values()
		require.NoError(t, err)
		rowCount++
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, 1, rowCount)
	t.Logf("cancelled exact SP-S3-U-D SQL in %s and reused backend PID %d", cancellationLatency, backendPID)
}

// TestPostgreSQLForcedShortestPathEdgeM0PlanResourcesAndConcurrency verifies direct edge-array hydration, zero local/temp/WAL usage, concurrency sample counts, and no edge work for a missing endpoint.
func TestPostgreSQLForcedShortestPathEdgeM0PlanResourcesAndConcurrency(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"GSP-D16-F016_path"}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)
	zeroRows := int64(0)
	missingEndpoint := selected.Cases[0]
	missingEndpoint.Name = "forced-m0-missing-start-endpoint"
	missingEndpoint.Params = testutil.Params{"start_id": int64(9223372036854775807)}
	missingEndpoint.NodeParams = map[string]string{"end_id": "sp-end"}
	missingEndpoint.Expected = ExpectedResult{
		RowCount:   &zeroRows,
		ResultKind: "path_set",
	}
	selected.Cases = append(selected.Cases, missingEndpoint)

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 2, 1, []int{1, 2, 4}, true, nil, "SP-S3-U-E+MAT-M0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 25, selected)
	require.NoError(t, err)
	require.Len(t, records, 2)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.Contains(t, record.SQL, "s1(next_id, depth, path)")
	require.Equal(t, 1, strings.Count(record.SQL, "generate_subscripts(s1.path, 1)"), record.SQL)
	require.NotContains(t, record.SQL, "ordered_edge_ids_to_path")
	require.NotContains(t, record.SQL, "sp_harness")

	require.NotNil(t, record.PostgresMetrics)
	metrics := record.PostgresMetrics
	require.Greater(t, metrics.RecursiveRows, int64(0))
	require.Greater(t, metrics.HydrationLoops, int64(0))
	require.Zero(t, metrics.Buffers.LocalHit)
	require.Zero(t, metrics.Buffers.LocalRead)
	require.Zero(t, metrics.Buffers.LocalDirtied)
	require.Zero(t, metrics.Buffers.LocalWritten)
	require.Zero(t, metrics.Buffers.TempRead)
	require.Zero(t, metrics.Buffers.TempWritten)
	require.Zero(t, metrics.TempFiles)
	require.Zero(t, metrics.TempBytes)
	require.Zero(t, metrics.WALRecords)
	require.Zero(t, metrics.WALBytes)

	require.Len(t, record.Concurrency, 3)
	for index, level := range []int{1, 2, 4} {
		block := record.Concurrency[index]
		require.Equal(t, level, block.Concurrency)
		require.Equal(t, 2, block.PoolSize)
		require.Equal(t, level*25, block.Operations)
		require.Len(t, block.Samples, level*25)
	}

	missingRecord := records[1]
	require.Equal(t, StatusOK, missingRecord.Status, missingRecord.Error)
	require.Zero(t, missingRecord.RowCount)
	require.NotNil(t, missingRecord.PostgresMetrics)
	require.Zero(t, missingRecord.PostgresMetrics.RecursiveRows)
	var missingEdgeLoops int64
	for _, node := range missingRecord.PostgresMetrics.PlanNodes {
		if node.RelationName == "edge" || strings.HasPrefix(node.RelationName, "edge_") {
			missingEdgeLoops += node.ActualLoops
		}
	}
	require.Zero(t, missingEdgeLoops, "missing endpoint must execute zero edge-search loops")
}

// TestPostgreSQLForcedShortestPathEdgeM0CancellationReusesSession verifies prompt timeout cancellation, rollback recovery on the same backend PID, and successful replay of M0 path SQL.
func TestPostgreSQLForcedShortestPathEdgeM0CancellationReusesSession(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{Cases: []string{"GSP-D64-F1000_path"}})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "SP-S3-U-E+MAT-M0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, StatusOK, records[0].Status, records[0].Error)

	translation, sqlQuery, err := runner.translateCypher(ctx, selected.Cases[0].Cypher, records[0].Params)
	require.NoError(t, err)
	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}, pgx.NamedArgs(translation.Parameters)}

	connectionHandle, err := runner.pool.Acquire(ctx)
	require.NoError(t, err)
	defer connectionHandle.Release()
	backendPID := connectionHandle.Conn().PgConn().PID()

	tx, err := connectionHandle.BeginTx(ctx, postgresConcurrencyTxOptions())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "set local statement_timeout = '1ms'")
	require.NoError(t, err)
	started := time.Now()
	rows, queryErr := tx.Query(ctx, sqlQuery, queryArgs...)
	if queryErr == nil {
		for rows.Next() {
			_, queryErr = rows.Values()
			if queryErr != nil {
				break
			}
		}
		rows.Close()
		if queryErr == nil {
			queryErr = rows.Err()
		}
	}
	cancellationLatency := time.Since(started)
	var postgresError *pgconn.PgError
	require.ErrorAs(t, queryErr, &postgresError)
	require.Equal(t, "57014", postgresError.Code)
	require.Less(t, cancellationLatency, 250*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))

	var reusedPID uint32
	require.NoError(t, connectionHandle.QueryRow(ctx, "select pg_backend_pid()").Scan(&reusedPID))
	require.Equal(t, backendPID, reusedPID)

	rows, err = connectionHandle.Query(ctx, sqlQuery, queryArgs...)
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		_, err = rows.Values()
		require.NoError(t, err)
		rowCount++
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, 1, rowCount)
	t.Logf("cancelled exact SP-S3-U-E+MAT-M0 SQL in %s and reused backend PID %d", cancellationLatency, backendPID)
}

// TestPostgreSQLForcedSuffixSeededReversePlanResourcesAndConcurrency verifies compact reverse-search SQL, relationship uniqueness, zero local/temp/WAL usage, and complete samples at each concurrency level.
func TestPostgreSQLForcedSuffixSeededReversePlanResourcesAndConcurrency(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{
		Cases: []string{"GFSE-V2-D16-F1000-R1-X1-M1-sparse_path"},
	})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 2, 1, []int{1, 2, 4}, false, nil, "", "EXPANSION-SUFFIX-SEEDED-REVERSE")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })

	records, err := runner.Run(ctx, 0, 25, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	record := records[0]
	require.Equal(t, StatusOK, record.Status, record.Error)
	require.Contains(t, record.SQL, "_suffix_seeded_suffix as materialized")
	require.Contains(t, record.SQL, "_suffix_seeded_reverse(boundary_id, next_id, depth, path)")
	require.Contains(t, record.SQL, "array_prepend")
	require.Contains(t, record.SQL, "!= all (")
	require.NotContains(t, record.SQL, "satisfied, is_cycle")

	require.NotNil(t, record.PostgresMetrics)
	metrics := record.PostgresMetrics
	require.Greater(t, metrics.RecursiveRows, int64(0))
	require.Zero(t, metrics.Buffers.LocalHit)
	require.Zero(t, metrics.Buffers.LocalRead)
	require.Zero(t, metrics.Buffers.LocalDirtied)
	require.Zero(t, metrics.Buffers.LocalWritten)
	require.Zero(t, metrics.Buffers.TempRead)
	require.Zero(t, metrics.Buffers.TempWritten)
	require.Zero(t, metrics.TempFiles)
	require.Zero(t, metrics.TempBytes)
	require.Zero(t, metrics.WALRecords)
	require.Zero(t, metrics.WALBytes)

	require.Len(t, record.Concurrency, 3)
	for index, level := range []int{1, 2, 4} {
		block := record.Concurrency[index]
		require.Equal(t, level, block.Concurrency)
		require.Equal(t, 2, block.PoolSize)
		require.Equal(t, level*25, block.Operations)
		require.Len(t, block.Samples, level*25)
	}
}

// TestPostgreSQLForcedSuffixSeededReverseCancellationReusesSession verifies prompt timeout cancellation, rollback recovery on the same backend PID, and cardinality-preserving replay of reverse expansion SQL.
func TestPostgreSQLForcedSuffixSeededReverseCancellationReusesSession(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	connectionURL, err := url.Parse(connection)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	corpus, err := loadScaleCorpus("../../benchmark/testdata/scale")
	require.NoError(t, err)
	selected, _, err := selectScaleCorpus(corpus, CorpusSelectors{
		Cases: []string{"GFSE-V2-D08-F016-R1-I1000-high_reverse_fanin"},
	})
	require.NoError(t, err)
	require.Len(t, selected.Cases, 1)

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, selected, 1, 1, nil, false, nil, "", "EXPANSION-SUFFIX-SEEDED-REVERSE")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runner.Close(ctx)) })
	records, err := runner.Run(ctx, 0, 1, selected)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, StatusOK, records[0].Status, records[0].Error)

	translation, sqlQuery, err := runner.translateCypher(ctx, selected.Cases[0].Cypher, records[0].Params)
	require.NoError(t, err)
	queryArgs := []any{pgx.QueryExecModeCacheStatement, pgx.QueryResultFormats{pgx.BinaryFormatCode}, pgx.NamedArgs(translation.Parameters)}

	connectionHandle, err := runner.pool.Acquire(ctx)
	require.NoError(t, err)
	defer connectionHandle.Release()
	backendPID := connectionHandle.Conn().PgConn().PID()
	tx, err := connectionHandle.BeginTx(ctx, postgresConcurrencyTxOptions())
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "set local statement_timeout = '1ms'")
	require.NoError(t, err)
	started := time.Now()
	rows, queryErr := tx.Query(ctx, sqlQuery, queryArgs...)
	if queryErr == nil {
		for rows.Next() {
			_, queryErr = rows.Values()
			if queryErr != nil {
				break
			}
		}
		rows.Close()
		if queryErr == nil {
			queryErr = rows.Err()
		}
	}
	cancellationLatency := time.Since(started)
	var postgresError *pgconn.PgError
	require.ErrorAs(t, queryErr, &postgresError)
	require.Equal(t, "57014", postgresError.Code)
	require.Less(t, cancellationLatency, 250*time.Millisecond)
	require.NoError(t, tx.Rollback(ctx))

	var reusedPID uint32
	require.NoError(t, connectionHandle.QueryRow(ctx, "select pg_backend_pid()").Scan(&reusedPID))
	require.Equal(t, backendPID, reusedPID)
	rows, err = connectionHandle.Query(ctx, sqlQuery, queryArgs...)
	require.NoError(t, err)
	rowCount := 0
	for rows.Next() {
		_, err = rows.Values()
		require.NoError(t, err)
		rowCount++
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, records[0].RowCount, int64(rowCount))
	t.Logf("cancelled exact EXPANSION-SUFFIX-SEEDED-REVERSE SQL in %s and reused backend PID %d", cancellationLatency, backendPID)
}

// requirePostgresReference returns the named comparator result or fails when the runner omitted that reference arm.
func requirePostgresReference(t *testing.T, references []PostgresReferenceResult, name string) PostgresReferenceResult {
	t.Helper()
	for _, reference := range references {
		if reference.Name == name {
			return reference
		}
	}
	t.Fatalf("missing PostgreSQL reference %s", name)
	return PostgresReferenceResult{}
}

// requireSingleScaleRecord returns the sole result for a corpus ID and rejects missing or duplicate representatives.
func requireSingleScaleRecord(t *testing.T, byID map[string][]CaseResult, id string) CaseResult {
	t.Helper()
	require.Len(t, byID[id], 1, "%s must have one representative", id)
	return byID[id][0]
}

// assertMutationPlanTarget verifies that delete representatives modify the physical entity table implied by their corpus ID.
func assertMutationPlanTarget(t *testing.T, id, plan string) {
	t.Helper()

	switch id {
	case "REC-01", "REC-02", "REC-04", "REC-06":
		require.Contains(t, plan, "Delete on edge")
	case "REC-08":
		require.Contains(t, plan, "Delete on node")
	}
}

// assertAnchorPlanIndex verifies that each indexed representative anchors through an endpoint or selective graph-partition index rather than a heap-wide scan.
func assertAnchorPlanIndex(t *testing.T, id, plan string) {
	t.Helper()

	switch id {
	case "HOP-01", "HOP-03", "HOP-04", "HOP-05":
		// PostgreSQL may prefer the covering kind index when the edge kind is
		// more selective than the bound endpoint. Both choices remain scoped
		// to the graph partition and avoid a heap-wide edge scan.
		require.Regexp(t, `(Bitmap Index Scan on|Index Scan using) edge_[0-9]+_(start_id|kind_id)`, plan)
		require.Contains(t, plan, "start_id =")
	case "HOP-02":
		require.Regexp(t, `(Bitmap Index Scan on|Index Scan using) edge_[0-9]+_end_id`, plan)
	case "HOP-07":
		// The selective terminal predicate can legitimately reverse the join
		// order, but either endpoint orientation must stay indexed.
		require.Regexp(t, `(Bitmap Index Scan on|Index Scan using) edge_[0-9]+_(start|end)_id`, plan)
	case "REC-01", "REC-02", "REC-04", "REC-06", "REC-08", "SCAN-05",
		"LOOKUP-02", "LOOKUP-04", "LOOKUP-05", "LOOKUP-09", "LOOKUP-11", "LOOKUP-13", "LOOKUP-16",
		"TRUST-01", "TRUST-02", "PRUNE-02", "PRUNE-03":
		require.Contains(t, plan, "Index Scan")
	}
}
