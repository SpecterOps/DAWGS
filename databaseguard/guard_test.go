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

func TestTargetUsesEffectivePostgreSQLEndpoint(t *testing.T) {
	target, err := Target("postgresql://user:secret@localhost:65432/disposable?host=PROD&port=5433&dbname=live")
	require.NoError(t, err)
	require.Equal(t, "postgresql://prod:5433/live", target)
}

func TestTargetCanonicalizesPostgreSQLSchemeAndDefaultPort(t *testing.T) {
	target, err := Target("postgres://user:secret@LOCALHOST/dawgs?sslmode=disable")
	require.NoError(t, err)
	require.Equal(t, "postgresql://localhost:5432/dawgs", target)
}

func TestTargetCanonicalizesNeo4jDefaultPortAndEscapedDatabase(t *testing.T) {
	target, err := Target("neo4j+s://user:secret@[2001:DB8::1]/Case%20Sensitive")
	require.NoError(t, err)
	require.Equal(t, "neo4j+s://[2001:db8::1]:7687/Case%20Sensitive", target)
}

func TestValidateRequiresAcknowledgementAndExactTarget(t *testing.T) {
	connection := "postgresql://user:secret@localhost:65432/dawgs"
	target := "postgresql://localhost:65432/dawgs"

	require.ErrorContains(t, Validate(connection, "", target), AllowDestructiveEnv)
	require.ErrorContains(t, Validate(connection, "1", "postgresql://localhost:65432/other"), DisposableTargetsEnv)
	require.NoError(t, Validate(connection, "1", "neo4j://localhost:7687/<default>, "+target))
	require.ErrorContains(t, Validate("postgresql://localhost:65432/CaseSensitive", "1", "postgresql://localhost:65432/casesensitive"), DisposableTargetsEnv)
}

func TestValidateEnvironment(t *testing.T) {
	t.Setenv(AllowDestructiveEnv, "1")
	t.Setenv(DisposableTargetsEnv, "postgresql://localhost:5432/dawgs")
	require.NoError(t, ValidateEnvironment("postgres://user:secret@localhost/dawgs"))
}

func TestTargetRejectsIncompleteConnection(t *testing.T) {
	_, err := Target("localhost/dawgs")
	require.Error(t, err)
}

func TestTargetErrorsDoNotExposeCredentials(t *testing.T) {
	connection := "postgresql://user:super-secret@localhost/%zz"
	_, err := Target(connection)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "user")
	require.NotContains(t, err.Error(), "super-secret")
	require.NotContains(t, err.Error(), connection)
}

func TestTargetRejectsMultiplePostgreSQLEndpoints(t *testing.T) {
	_, err := Target("postgresql://user:secret@localhost/dawgs?host=one,two")
	require.ErrorContains(t, err, "one endpoint")
}
