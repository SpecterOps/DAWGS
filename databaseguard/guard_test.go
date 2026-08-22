// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package databaseguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTargetRedactsCredentialsAndQuery verifies canonical targets never expose connection credentials or query parameters.
func TestTargetRedactsCredentialsAndQuery(t *testing.T) {
	target, err := Target("postgresql://user:secret@LOCALHOST:65432/dawgs?sslmode=disable&password=other")
	require.NoError(t, err)
	require.Equal(t, "postgresql://localhost:65432/dawgs", target)
	require.NotContains(t, target, "user")
	require.NotContains(t, target, "secret")
	require.NotContains(t, target, "password")
}

// TestTargetNamesDefaultDatabase verifies a missing Neo4j database is represented by an explicit placeholder.
func TestTargetNamesDefaultDatabase(t *testing.T) {
	target, err := Target("neo4j://localhost:7687")
	require.NoError(t, err)
	require.Equal(t, "neo4j://localhost:7687/<default>", target)
}

// TestTargetUsesEffectivePostgreSQLEndpoint verifies pgx query parameters override authority components during target canonicalization.
func TestTargetUsesEffectivePostgreSQLEndpoint(t *testing.T) {
	target, err := Target("postgresql://user:secret@localhost:65432/disposable?host=PROD&port=5433&dbname=live")
	require.NoError(t, err)
	require.Equal(t, "postgresql://prod:5433/live", target)
}

// TestTargetCanonicalizesPostgreSQLSchemeAndDefaultPort verifies PostgreSQL aliases and implicit ports produce one stable identity.
func TestTargetCanonicalizesPostgreSQLSchemeAndDefaultPort(t *testing.T) {
	target, err := Target("postgres://user:secret@LOCALHOST/dawgs?sslmode=disable")
	require.NoError(t, err)
	require.Equal(t, "postgresql://localhost:5432/dawgs", target)
}

// TestTargetCanonicalizesNeo4jDefaultPortAndEscapedDatabase verifies IPv6 hosts, default ports, and escaped database names remain stable.
func TestTargetCanonicalizesNeo4jDefaultPortAndEscapedDatabase(t *testing.T) {
	target, err := Target("neo4j+s://user:secret@[2001:DB8::1]/Case%20Sensitive")
	require.NoError(t, err)
	require.Equal(t, "neo4j+s://[2001:db8::1]:7687/Case%20Sensitive", target)
}

// TestValidateRequiresAcknowledgementAndExactTarget verifies both safety gates are mandatory and target matching is exact.
func TestValidateRequiresAcknowledgementAndExactTarget(t *testing.T) {
	connection := "postgresql://user:secret@localhost:65432/dawgs"
	target := "postgresql://localhost:65432/dawgs"

	require.ErrorContains(t, Validate(connection, "", target), AllowDestructiveEnv)
	require.ErrorContains(t, Validate(connection, "1", "postgresql://localhost:65432/other"), DisposableTargetsEnv)
	require.NoError(t, Validate(connection, "1", "neo4j://localhost:7687/<default>, "+target))
	require.ErrorContains(t, Validate("postgresql://localhost:65432/CaseSensitive", "1", "postgresql://localhost:65432/casesensitive"), DisposableTargetsEnv)
}

// TestValidateEnvironment verifies process environment values authorize the corresponding canonical target.
func TestValidateEnvironment(t *testing.T) {
	t.Setenv(AllowDestructiveEnv, "1")
	t.Setenv(DisposableTargetsEnv, "postgresql://localhost:5432/dawgs")
	require.NoError(t, ValidateEnvironment("postgres://user:secret@localhost/dawgs"))
}

// TestTargetRejectsIncompleteConnection verifies target derivation rejects connection strings without a scheme and host.
func TestTargetRejectsIncompleteConnection(t *testing.T) {
	_, err := Target("localhost/dawgs")
	require.Error(t, err)
}

// TestTargetErrorsDoNotExposeCredentials verifies malformed connection errors do not echo sensitive input.
func TestTargetErrorsDoNotExposeCredentials(t *testing.T) {
	connection := "postgresql://user:super-secret@localhost/%zz"
	_, err := Target(connection)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "user")
	require.NotContains(t, err.Error(), "super-secret")
	require.NotContains(t, err.Error(), connection)
}

// TestTargetRejectsMultiplePostgreSQLEndpoints verifies destructive authorization cannot cover a multi-host PostgreSQL failover configuration.
func TestTargetRejectsMultiplePostgreSQLEndpoints(t *testing.T) {
	_, err := Target("postgresql://user:secret@localhost/dawgs?host=one,two")
	require.ErrorContains(t, err, "one endpoint")
}
