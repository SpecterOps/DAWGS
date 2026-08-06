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

	"github.com/stretchr/testify/require"
)

func TestModesForRoundAlternatesBackendOrderWithoutMutatingConfig(t *testing.T) {
	modes := []ExecutionMode{ModePostgresSQL, ModeNeo4j}

	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modesForRound(modes, 1))
	require.Equal(t, []ExecutionMode{ModeNeo4j, ModePostgresSQL}, modesForRound(modes, 2))
	require.Equal(t, []ExecutionMode{ModePostgresSQL, ModeNeo4j}, modes)
}

func TestParseConfigRequiresCompleteGateInputs(t *testing.T) {
	_, err := parseConfig([]string{"-gate-baseline", "baseline.jsonl"}, func(string) string { return "" })

	require.ErrorContains(t, err, "must be supplied together")
}

func TestParseConfigAcceptsPoolAndConcurrencySmokeLevels(t *testing.T) {
	cfg, err := parseConfig([]string{"-pool-size", "4", "-concurrency", "1,4,8,4"}, func(string) string { return "" })

	require.NoError(t, err)
	require.Equal(t, 4, cfg.PoolSize)
	require.Equal(t, []int{1, 4, 8}, cfg.Concurrency)
}

func TestParseConfigRejectsPoolMemoryBelowPerSessionBudget(t *testing.T) {
	_, err := parseConfig([]string{
		"-pool-size", "4",
		"-session-memory-ceiling-bytes", "100",
		"-pool-memory-ceiling-bytes", "399",
	}, func(string) string { return "" })

	require.ErrorContains(t, err, "session memory ceiling times pool size")
}

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

func TestParseConfigRejectsDuplicateExactSelectors(t *testing.T) {
	_, err := parseConfig([]string{"-cases", "case-a,case-a"}, func(string) string { return "" })
	require.ErrorContains(t, err, "duplicate case selector")
}
