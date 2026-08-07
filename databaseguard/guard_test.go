// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package databaseguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetRedactsCredentialsAndQuery(t *testing.T) {
	target, err := Target("postgresql://user:secret@LOCALHOST:65432/dawgs?sslmode=disable&password=other")
	require.NoError(t, err)
	require.Equal(t, "postgresql://localhost:65432/dawgs", target)
	require.NotContains(t, target, "user")
	require.NotContains(t, target, "secret")
	require.NotContains(t, target, "password")
}

func TestTargetNamesDefaultDatabase(t *testing.T) {
	target, err := Target("neo4j://localhost:7687")
	require.NoError(t, err)
	require.Equal(t, "neo4j://localhost:7687/<default>", target)
}

func TestValidateRequiresAcknowledgementAndExactTarget(t *testing.T) {
	connection := "postgresql://user:secret@localhost:65432/dawgs"
	target := "postgresql://localhost:65432/dawgs"

	require.ErrorContains(t, Validate(connection, "", target), AllowDestructiveEnv)
	require.ErrorContains(t, Validate(connection, "1", "postgresql://localhost:65432/other"), DisposableTargetsEnv)
	require.NoError(t, Validate(connection, "1", "neo4j://localhost:7687/<default>, "+target))
	require.ErrorContains(t, Validate("postgresql://localhost:65432/CaseSensitive", "1", "postgresql://localhost:65432/casesensitive"), DisposableTargetsEnv)
}

func TestTargetRejectsIncompleteConnection(t *testing.T) {
	_, err := Target("localhost/dawgs")
	require.Error(t, err)
}
