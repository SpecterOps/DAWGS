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

func TestPostgresConcurrencyTransactionsPermitSessionWorkspaceMaintenance(t *testing.T) {
	require.Equal(t, pgx.ReadWrite, postgresConcurrencyTxOptions().AccessMode)
}
