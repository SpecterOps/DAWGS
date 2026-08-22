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

// TestDestructiveRunnersRequireTargetAuthorization verifies that neither PostgreSQL nor Neo4j runners can initialize against an unapproved destructive target.
func TestDestructiveRunnersRequireTargetAuthorization(t *testing.T) {
	t.Setenv(databaseguard.AllowDestructiveEnv, "")
	t.Setenv(databaseguard.DisposableTargetsEnv, "")

	_, err := newPostgresSQLRunner(context.Background(), "", "postgresql://user:secret@localhost/dawgs", ScaleCorpus{}, 1, 1, nil, false, nil, "", "")
	require.ErrorContains(t, err, "refuse destructive PostgreSQL")

	_, err = newNeo4jRunner(context.Background(), "", "neo4j://user:secret@localhost", ScaleCorpus{})
	require.ErrorContains(t, err, "refuse destructive Neo4j")
}
