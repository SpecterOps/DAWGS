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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig verifies odd/even round rotation without modifying the configured backend order.
func TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig(t *testing.T) {
	modes := []ExecutionMode{ModePostgresSQL, ModeNeo4j}

	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modesForRound(modes, 1))
	require.Equal(t, []ExecutionMode{ModeNeo4j, ModePostgresSQL}, modesForRound(modes, 2))
	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modes)
}

// TestParseConfigRequiresCompleteGateInputs verifies that baseline gating cannot be enabled without its paired candidate artifact.
func TestParseConfigRequiresCompleteGateInputs(t *testing.T) {
	_, err := parseConfig([]string{"-gate-baseline", "baseline.jsonl"}, func(string) string { return "" })

	require.ErrorContains(t, err, "must be supplied together")
}

// TestParseConfigAcceptsPoolAndConcurrencySmokeLevels verifies numeric pool parsing and stable deduplication of requested concurrency levels.
func TestParseConfigAcceptsPoolAndConcurrencySmokeLevels(t *testing.T) {
	cfg, err := parseConfig([]string{"-pool-size", "4", "-concurrency", "1,4,8,4"}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, 4, cfg.PoolSize)
	require.Equal(t, []int{1, 4, 8}, cfg.Concurrency)
}

// TestParseConfigAcceptsReferencePairDiscoveryProtocol verifies that the discovery protocol flag selects the corresponding reference-pair workflow.
func TestParseConfigAcceptsReferencePairDiscoveryProtocol(t *testing.T) {
	cfg, err := parseConfig([]string{"-reference-pair-protocol", "discovery"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, referencePairProtocolDiscovery, cfg.ReferencePairProtocol)
}

// TestParseConfigRejectsPoolMemoryBelowPerSessionBudget verifies that the pool ceiling must cover the per-session budget for every configured connection.
func TestParseConfigRejectsPoolMemoryBelowPerSessionBudget(t *testing.T) {
	_, err := parseConfig([]string{
		"-pool-size", "4",
		"-session-memory-ceiling-bytes", "100",
		"-pool-memory-ceiling-bytes", "399",
	}, func(string) string { return "" })

	require.ErrorContains(t, err, "session memory ceiling times pool size")
}

// TestParseConfigAcceptsDiagnosticSelectorsAndRunMetadata verifies parsing of case filters, warmups, arm identity, and block metadata used to reproduce diagnostic runs.
func TestParseConfigAcceptsDiagnosticSelectorsAndRunMetadata(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-cases", "case-a,case-b", "-datasets", "fixture", "-categories", "lookup", "-tags", "primary,control",
		"-warmup-iterations", "20", "-arm", "candidate", "-arm-order", "2", "-block", "7", "-run-uuid", "run-1",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, []string{"case-a", "case-b"}, cfg.Cases)
	require.Equal(t, 20, cfg.WarmupIterations)
	require.Equal(t, "candidate", cfg.Arm)
	require.Equal(t, 7, cfg.Block)
}

// TestParseConfigRejectsDuplicateExactSelectors verifies that repeated exact case names are rejected before corpus selection.
func TestParseConfigRejectsDuplicateExactSelectors(t *testing.T) {
	_, err := parseConfig([]string{"-cases", "case-a,case-a"}, func(string) string { return "" })
	require.ErrorContains(t, err, "duplicate case selector")
}

// TestParseConfigAcceptsOnlyQualifiedForcedShortestExecutor verifies the supported shortest-executor allowlist and rejects an incomplete strategy name.
func TestParseConfigAcceptsOnlyQualifiedForcedShortestExecutor(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-force-shortest-executor", "SP-S0"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "SP-S0", cfg.PostgresForceShortest)
	cfg, err = parseConfig([]string{"-postgres-force-shortest-executor", "SP-S0-DIRECT"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "SP-S0-DIRECT", cfg.PostgresForceShortest)
	cfg, err = parseConfig([]string{"-postgres-force-shortest-executor", "SP-S3-U-D"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "SP-S3-U-D", cfg.PostgresForceShortest)
	cfg, err = parseConfig([]string{"-postgres-force-shortest-executor", "SP-S3-U-E+MAT-M0"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "SP-S3-U-E+MAT-M0", cfg.PostgresForceShortest)
	cfg, err = parseConfig([]string{"-postgres-force-shortest-executor", "SP-S4-C-WE+MAT-M0"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "SP-S4-C-WE+MAT-M0", cfg.PostgresForceShortest)
	cfg, err = parseConfig([]string{"-postgres-force-shortest-executor", "ASP-A1-DAG"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "ASP-A1-DAG", cfg.PostgresForceShortest)

	_, err = parseConfig([]string{"-postgres-force-shortest-executor", "SP-S1"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unsupported PostgreSQL forced shortest executor")
}

// TestParseConfigExistingGraphWorkflow verifies that a fully specified live-graph discovery run retains checkpoint, resume, progress, timeout, and sampling settings.
func TestParseConfigExistingGraphWorkflow(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-existing-graph", "-anchor-manifest", "anchors.json", "-checkpoint", "checkpoint.json",
		"-resume", "-progress", "progress.jsonl", "-discovery", "-timeout-classes", "100ms,1s",
		"-discovery-sample-floor", "2",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.ExistingGraph)
	require.True(t, cfg.Resume)
	require.True(t, cfg.Discovery)
	require.Equal(t, []time.Duration{100 * time.Millisecond, time.Second}, cfg.TimeoutClasses)
	require.Equal(t, 2, cfg.DiscoverySampleFloor)
}

// TestParseConfigRejectsUnsafeExistingGraphCombinations verifies that live-graph mode requires an anchor manifest and disallows mismatched backends or orphaned resume/discovery flags.
func TestParseConfigRejectsUnsafeExistingGraphCombinations(t *testing.T) {
	for _, args := range [][]string{
		{"-existing-graph"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-modes", "postgres_sql,neo4j"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-resume"},
		{"-existing-graph", "-anchor-manifest", "anchors.json", "-timeout-classes", "1s"},
		{"-anchor-manifest", "anchors.json"},
	} {
		_, err := parseConfig(args, func(string) string { return "" })
		require.Error(t, err, args)
	}
}

// TestParseConfigAcceptsOnlyQualifiedForcedExpansionSearch verifies the expansion-strategy allowlist and prevents simultaneous forced expansion and shortest-path strategies.
func TestParseConfigAcceptsOnlyQualifiedForcedExpansionSearch(t *testing.T) {
	cfg, err := parseConfig([]string{"-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "EXPANSION-SUFFIX-SEEDED-REVERSE", cfg.PostgresForceExpansion)
	cfg, err = parseConfig([]string{"-postgres-force-expansion-search", "EXPANSION-ENDPOINT-SEEDED-REVERSE"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "EXPANSION-ENDPOINT-SEEDED-REVERSE", cfg.PostgresForceExpansion)

	_, err = parseConfig([]string{"-postgres-force-expansion-search", "unknown-strategy"}, func(string) string { return "" })
	require.ErrorContains(t, err, "unsupported PostgreSQL forced expansion search")

	_, err = parseConfig([]string{
		"-postgres-force-shortest-executor", "SP-S3-U-D",
		"-postgres-force-expansion-search", "EXPANSION-SUFFIX-SEEDED-REVERSE",
	}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestParseConfigRequiresOutputForJSONLAppend verifies that append mode names a destination and is retained once that destination is present.
func TestParseConfigRequiresOutputForJSONLAppend(t *testing.T) {
	_, err := parseConfig([]string{"-append-jsonl"}, func(string) string { return "" })
	require.ErrorContains(t, err, "append-jsonl requires jsonl-output")

	cfg, err := parseConfig([]string{"-append-jsonl", "-jsonl-output", "rounds.jsonl"}, func(string) string { return "" })
	require.NoError(t, err)
	require.True(t, cfg.AppendJSONL)
}

// TestParseConfigAcceptsReferenceClosureMode verifies reference-closure artifact parsing, confidence propagation, required output pairing, and exclusion of incompatible A/A mode.
func TestParseConfigAcceptsReferenceClosureMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-reference-closure-artifact", "reference.jsonl",
		"-reference-closure-output", "report.json",
		"-reference-closure-arm", "s3_unidirectional_trail_cte",
		"-confidence-level", "0.975",
	}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "reference.jsonl", cfg.ReferenceClosureArtifact)
	require.Equal(t, 0.975, cfg.Confidence)

	_, err = parseConfig([]string{"-reference-closure-output", "report.json"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires reference-closure-artifact")
	_, err = parseConfig([]string{"-reference-closure-artifact", "reference.jsonl", "-aa-artifact", "aa.jsonl"}, func(string) string { return "" })
	require.ErrorContains(t, err, "mutually exclusive")
}

// TestParseConfigAcceptsReferencePairMode verifies that pair-report configuration retains its artifact and explicit baseline/candidate arm names.
func TestParseConfigAcceptsReferencePairMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-reference-pair-artifact", "pair.jsonl",
		"-reference-pair-baseline", "s3",
		"-reference-pair-candidate", "s1",
	}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, "pair.jsonl", cfg.ReferencePairArtifact)
	require.Equal(t, "s3", cfg.ReferencePairBaseline)
	require.Equal(t, "s1", cfg.ReferencePairCandidate)
}
