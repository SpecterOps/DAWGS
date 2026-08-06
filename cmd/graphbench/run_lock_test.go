// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestructiveRunLockRejectsOverlap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graphbench.lock")
	first, err := acquireDestructiveRunLock(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })

	_, err = acquireDestructiveRunLock(path)
	require.ErrorContains(t, err, "another GraphBench process")
}
