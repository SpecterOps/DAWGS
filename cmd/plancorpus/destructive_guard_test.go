// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/databaseguard"
	"github.com/stretchr/testify/require"
)

// TestCaptureCorpusRequiresTargetAuthorization verifies that plan capture refuses an unallowlisted PostgreSQL target before loading destructive fixture data.
func TestCaptureCorpusRequiresTargetAuthorization(t *testing.T) {
	t.Setenv(databaseguard.AllowDestructiveEnv, "")
	t.Setenv(databaseguard.DisposableTargetsEnv, "")

	_, err := captureCorpus(context.Background(), "", corpus{}, captureSpec{
		DriverName: pgDriverName(),
		Connection: "postgresql://user:secret@localhost/dawgs",
	})
	require.ErrorContains(t, err, "refuse destructive plan-corpus")
}
