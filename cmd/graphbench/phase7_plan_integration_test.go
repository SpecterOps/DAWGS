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
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgreSQLPhase7PlanInvariants(t *testing.T) {
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
	required := phase7RequiredIDSet()
	filtered := ScaleCorpus{}
	for _, testCase := range corpus.Cases {
		id := phase7CaseID(testCase.Name)
		_, isRequired := required[id]
		if isRequired || id == "TRUST-03" {
			filtered.Cases = append(filtered.Cases, testCase)
		}
	}

	ctx := context.Background()
	runner, err := newPostgresSQLRunner(ctx, "../../integration/testdata", connection, filtered)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, runner.Close(ctx))
	})

	records, err := runner.Run(ctx, 1, filtered)
	require.NoError(t, err)
	require.Len(t, records, len(filtered.Cases))

	byID := map[string][]CaseResult{}
	for _, record := range records {
		record := record
		id := phase7CaseID(record.Name)
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
			assertPhase7MutationTarget(t, id, plan)
			assertPhase7AnchorIndex(t, id, plan)
		})
	}

	for _, id := range phase7RequiredScaleIDs {
		require.NotEmpty(t, byID[id], "missing PostgreSQL plan-invariant execution for %s", id)
	}

	t.Run("LOGIC-01 branch-local direction and kind plan", func(t *testing.T) {
		record := requireSinglePhase7Record(t, byID, "TRUST-03")
		normalizedSQL := strings.ToLower(record.SQL)
		require.Contains(t, normalizedSQL, " or ")
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, "kind_id"), 2)
		require.Contains(t, normalizedSQL, "start_id")
		require.Contains(t, normalizedSQL, "end_id")
	})

	t.Run("LOGIC-02 cross-binding temporal plan", func(t *testing.T) {
		record := requireSinglePhase7Record(t, byID, "TRUST-01")
		normalizedSQL := strings.ToLower(record.SQL)
		require.Contains(t, normalizedSQL, " or ")
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, "lastcollected"), 2)
		require.GreaterOrEqual(t, strings.Count(normalizedSQL, " < "), 2)
	})

	t.Run("LOGIC-04 filtered mutation targets", func(t *testing.T) {
		edgeDelete := requireSinglePhase7Record(t, byID, "REC-01")
		nodeDelete := requireSinglePhase7Record(t, byID, "REC-08")
		require.Contains(t, strings.Join(edgeDelete.PostgresPlan, "\n"), "Delete on edge")
		require.Contains(t, strings.Join(nodeDelete.PostgresPlan, "\n"), "Delete on node")
	})
}

func requireSinglePhase7Record(t *testing.T, byID map[string][]CaseResult, id string) CaseResult {
	t.Helper()
	require.Len(t, byID[id], 1, "%s must have one representative", id)
	return byID[id][0]
}

func assertPhase7MutationTarget(t *testing.T, id, plan string) {
	t.Helper()

	switch id {
	case "REC-01", "REC-02", "REC-04", "REC-06":
		require.Contains(t, plan, "Delete on edge")
	case "REC-08":
		require.Contains(t, plan, "Delete on node")
	}
}

func assertPhase7AnchorIndex(t *testing.T, id, plan string) {
	t.Helper()

	switch id {
	case "HOP-01", "HOP-03", "HOP-04", "HOP-05", "HOP-07":
		require.Regexp(t, `Index Scan using edge_[0-9]+_start_id`, plan)
	case "HOP-02":
		require.Regexp(t, `Index Scan using edge_[0-9]+_end_id`, plan)
	case "REC-01", "REC-02", "REC-04", "REC-06", "REC-08", "SCAN-05",
		"LOOKUP-02", "LOOKUP-04", "LOOKUP-05", "LOOKUP-09", "LOOKUP-11", "LOOKUP-13", "LOOKUP-16",
		"TRUST-01", "TRUST-02", "PRUNE-01", "PRUNE-02", "PRUNE-03":
		require.Contains(t, plan, "Index Scan")
	}
}
