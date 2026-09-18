// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOrientationProbePolicyRecognitionIsVersionExplicit verifies orientation probe policy recognition is version explicit behavior.
func TestOrientationProbePolicyRecognitionIsVersionExplicit(t *testing.T) {
	require.True(t, isOrientationProbePolicy("orientation-probe-v1"))
	require.True(t, isOrientationProbePolicy("orientation-probe-v2"))
	require.False(t, isOrientationProbePolicy("orientation-probe-v3"))
	require.False(t, isOrientationProbePolicy("ORIENTATION-PROBE-V2"))
	require.False(t, isOrientationProbePolicy(""))
}

// TestSuffixReverseGuardPolicyIsNotAnOrientationGeneration verifies the new
// admission-only policy cannot consume orientation-v2 report or manifest paths.
func TestSuffixReverseGuardPolicyIsNotAnOrientationGeneration(t *testing.T) {
	require.True(t, isSuffixReverseGuardPolicy("suffix-reverse-guard-v1"))
	require.True(t, isGuardedExpansionPolicy("suffix-reverse-guard-v1"))
	require.False(t, isOrientationProbePolicy("suffix-reverse-guard-v1"))
	require.False(t, isSuffixReverseGuardPolicy("orientation-probe-v2"))
}
