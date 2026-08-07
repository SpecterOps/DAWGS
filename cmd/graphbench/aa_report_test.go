// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildAAResolutionReportSplitsMatchedSamplesAndKeepsP99Diagnostic(t *testing.T) {
	record := perfGateRecord("case", ModePostgresSQL, time.Millisecond, 5, 40)
	report, err := buildAAResolutionReport([]CaseResult{record}, PerfGateOptions{Seed: 1, Confidence: 0.95, BootstrapCount: 100})

	require.NoError(t, err)
	require.Len(t, report.Cases, 1)
	require.Equal(t, 100, report.Cases[0].SamplesPerArm)
	require.InDelta(t, 1, report.Cases[0].P50.Ratio.Estimate, 0.0001)
	require.False(t, report.Cases[0].P99Gated)
	require.Contains(t, report.Cases[0].P99Reason, "diagnostic only")
}
