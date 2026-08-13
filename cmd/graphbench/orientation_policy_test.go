// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrientationProbePolicyRecognitionIsVersionExplicit(t *testing.T) {
	require.True(t, isOrientationProbePolicy("orientation-probe-v1"))
	require.True(t, isOrientationProbePolicy("orientation-probe-v2"))
	require.False(t, isOrientationProbePolicy("orientation-probe-v3"))
	require.False(t, isOrientationProbePolicy("ORIENTATION-PROBE-V2"))
	require.False(t, isOrientationProbePolicy(""))
}
