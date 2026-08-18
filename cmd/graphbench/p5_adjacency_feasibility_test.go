// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseConfigP5AdjacencyFeasibility(t *testing.T) {
	cfg, err := parseConfig([]string{"-p5-adjacency-feasibility-output", "p5.json"}, func(string) string { return "" })
	require.NoError(t, err)
	require.Equal(t, "p5.json", cfg.P5AdjacencyFeasibilityOutput)

	_, err = parseConfig([]string{"-p5-adjacency-feasibility-output", "p5.json", "-pool-size", "2"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires pool-size 1")

	_, err = parseConfig([]string{"-p5-adjacency-feasibility-output", "p5.json", "-modes", "neo4j"}, func(string) string { return "" })
	require.ErrorContains(t, err, "requires only postgres_sql mode")

	_, err = parseConfig([]string{"-p5-adjacency-feasibility-output", "p5.json", "-cases", "SP-S0-DIRECT"}, func(string) string { return "" })
	require.ErrorContains(t, err, "does not accept corpus selectors")
}

func TestP5AdjacencyQuantiles(t *testing.T) {
	median, p95 := p5AdjacencyQuantiles([]time.Duration{5, 1, 3, 2, 4})
	require.Equal(t, 3*time.Nanosecond, median)
	require.Equal(t, 5*time.Nanosecond, p95)
}

func TestWriteP5AdjacencyFeasibilityReportIsImmutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p5.json")
	report := P5AdjacencyFeasibilityReport{Schema: p5AdjacencyFeasibilitySchema, Passed: true}
	require.NoError(t, writeP5AdjacencyFeasibilityReport(path, report))
	require.ErrorContains(t, writeP5AdjacencyFeasibilityReport(path, report), "create immutable")
}
