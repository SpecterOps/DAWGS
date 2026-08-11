// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSQLFingerprintIsStableAndContentSensitive verifies that identical SQL yields a repeatable 256-bit digest while a query change alters that digest.
func TestSQLFingerprintIsStableAndContentSensitive(t *testing.T) {
	require.Equal(t, sqlFingerprint("select 1"), sqlFingerprint("select 1"))
	require.NotEqual(t, sqlFingerprint("select 1"), sqlFingerprint("select 2"))
	require.Len(t, sqlFingerprint("select 1"), 64)
}

// TestSanitizedInvocationRedactsConnectionStrings verifies redaction for split and inline connection flags while preserving unrelated arguments and the caller's input slice.
func TestSanitizedInvocationRedactsConnectionStrings(t *testing.T) {
	args := []string{
		"graphbench",
		"-connection", "postgres://user:secret@host/database",
		"-pg-connection=postgres://user:secret@host/database",
		"-neo4j-connection", "neo4j://user:secret@host",
		"-iterations", "30",
	}

	require.Equal(t, []string{
		"graphbench",
		"-connection", "<redacted>",
		"-pg-connection=<redacted>",
		"-neo4j-connection", "<redacted>",
		"-iterations", "30",
	}, sanitizedInvocation(args))
	require.Contains(t, args[2], "secret", "the caller's argument slice must not be mutated")
}
