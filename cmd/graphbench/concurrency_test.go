// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPostgresConcurrencyTransactionsPermitSessionWorkspaceMaintenance verifies that concurrent benchmark transactions are read-write so session-scoped workspace tables can be maintained.
func TestPostgresConcurrencyTransactionsPermitSessionWorkspaceMaintenance(t *testing.T) {
	require.Equal(t, pgx.ReadWrite, postgresConcurrencyTxOptions().AccessMode)
	require.Empty(t, postgresConcurrencyTxOptions().IsoLevel)
	require.Equal(t, pgx.RepeatableRead, postgresConcurrencyTxOptions(pgx.RepeatableRead).IsoLevel)
}
