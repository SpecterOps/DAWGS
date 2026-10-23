// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSPI2HierarchicalBootstrapV2DeterministicGoldenVector(t *testing.T) {
	baseline := roundSamples{
		1: {900 * time.Microsecond, 1 * time.Millisecond, 2 * time.Millisecond, 2200 * time.Microsecond},
		2: {1 * time.Millisecond, 1100 * time.Microsecond, 2100 * time.Microsecond, 2400 * time.Microsecond},
		3: {950 * time.Microsecond, 1050 * time.Microsecond, 2300 * time.Microsecond, 2500 * time.Microsecond},
	}
	candidate := roundSamples{
		1: {850 * time.Microsecond, 950 * time.Microsecond, 1800 * time.Microsecond, 2100 * time.Microsecond},
		2: {900 * time.Microsecond, 1 * time.Millisecond, 1900 * time.Microsecond, 2200 * time.Microsecond},
		3: {875 * time.Microsecond, 975 * time.Microsecond, 2 * time.Millisecond, 2300 * time.Microsecond},
	}
	seed := spI2BootstrapSeedV2("dataset-a", "case-a", "p95")
	require.Equal(t, "20c91adbc448b55ac3d9dff2b91f60378f144fec7c2df53df6f2b702afc457d4", hex.EncodeToString(seed[:]))

	first, err := bootstrapSPI2HierarchicalTailV2(baseline, candidate, "dataset-a", "case-a", "p95", 0.95, 0.975, 1000)
	require.NoError(t, err)
	second, err := bootstrapSPI2HierarchicalTailV2(baseline, candidate, "dataset-a", "case-a", "p95", 0.95, 0.975, 1000)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, RatioInterval{Estimate: 0.92, Lower: 0.8, Upper: 1.0454545454545454}, first.Ratio)
	require.Equal(t, DurationInterval{Estimate: -200 * time.Microsecond, Lower: -500 * time.Microsecond, Upper: 100 * time.Microsecond}, first.Change)
}

func TestSPI2HierarchicalBootstrapV2RejectsMissingAndUnequalSamples(t *testing.T) {
	baseline := roundSamples{1: {time.Millisecond, 2 * time.Millisecond}, 2: {time.Millisecond, 2 * time.Millisecond}}
	_, err := bootstrapSPI2HierarchicalTailV2(baseline, roundSamples{1: {time.Millisecond, 2 * time.Millisecond}}, "d", "c", "p95", 0.95, 0.975, 10)
	require.ErrorContains(t, err, "identical nonempty round sets")
	_, err = bootstrapSPI2HierarchicalTailV2(baseline, roundSamples{1: {time.Millisecond}, 2: {time.Millisecond}}, "d", "c", "p95", 0.95, 0.975, 10)
	require.ErrorContains(t, err, "unequal arm sample counts")
}

func TestSPI2RoundMedianBootstrapDoesNotResampleWithinRounds(t *testing.T) {
	baseline := roundSamples{1: {time.Millisecond, 100 * time.Millisecond}, 2: {2 * time.Millisecond, 200 * time.Millisecond}}
	candidate := roundSamples{1: {900 * time.Microsecond, 90 * time.Millisecond}, 2: {1800 * time.Microsecond, 180 * time.Millisecond}}
	ratio, saving, err := bootstrapSPI2RoundMedianV2(baseline, candidate, "d", "c", "median", 0.975, 100)
	require.NoError(t, err)
	require.InDelta(t, 0.9, ratio.Estimate, 0.000001)
	require.Equal(t, 100*time.Microsecond, saving.Estimate)
}

func BenchmarkSPI2HierarchicalBootstrapV2_40x100x100000(b *testing.B) {
	baseline, candidate := roundSamples{}, roundSamples{}
	for round := 1; round <= 40; round++ {
		for sample := range 100 {
			baseline[round] = append(baseline[round], time.Duration(900+round*3+sample*11)*time.Microsecond)
			candidate[round] = append(candidate[round], time.Duration(875+round*3+sample*10)*time.Microsecond)
		}
	}
	b.ResetTimer()
	for range b.N {
		_, err := bootstrapSPI2HierarchicalTailV2(baseline, candidate, "benchmark", "representative", "p95", 0.95, 0.975, 100_000)
		require.NoError(b, err)
	}
}
