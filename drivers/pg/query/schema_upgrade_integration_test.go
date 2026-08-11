// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration && integration

package query

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/databaseguard"
	"github.com/stretchr/testify/require"
)

func TestSchemaUpgradeRemovesLegacyPathMaterializerOverloads(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if connection == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}
	target, err := databaseguard.Target(connection)
	require.NoError(t, err)
	if len(target) < len("postgresql://") || target[:len("postgresql://")] != "postgresql://" {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}
	require.NoError(t, databaseguard.ValidateEnvironment(connection))

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		drop function public.nodes_to_path(int4, int8[]);
		drop function public.edges_to_path(int4, int8[]);
		drop function public.ordered_edges_to_path(int4, nodeComposite, edgeComposite[], nodeComposite[]);
		create function public.nodes_to_path(nodes variadic int8[]) returns pathComposite language sql immutable strict as $$
			select row(array[]::nodeComposite[], array[]::edgeComposite[])::pathComposite
		$$;
		create function public.edges_to_path(path variadic int8[]) returns pathComposite language sql immutable strict as $$
			select row(array[]::nodeComposite[], array[]::edgeComposite[])::pathComposite
		$$;
		create function public.ordered_edges_to_path(root nodeComposite, edges edgeComposite[], known_nodes nodeComposite[]) returns pathComposite language sql immutable strict as $$
			select row(array[root]::nodeComposite[], edges)::pathComposite
		$$;
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)

	var legacyNodes, legacyEdges, legacyOrdered, scopedNodes, scopedEdges, scopedOrdered bool
	err = pool.QueryRow(ctx, `select
		to_regprocedure('public.nodes_to_path(bigint[])') is not null,
		to_regprocedure('public.edges_to_path(bigint[])') is not null,
		to_regprocedure('public.ordered_edges_to_path(nodecomposite,edgecomposite[],nodecomposite[])') is not null,
		to_regprocedure('public.nodes_to_path(integer,bigint[])') is not null,
		to_regprocedure('public.edges_to_path(integer,bigint[])') is not null,
		to_regprocedure('public.ordered_edges_to_path(integer,nodecomposite,edgecomposite[],nodecomposite[])') is not null
	`).Scan(&legacyNodes, &legacyEdges, &legacyOrdered, &scopedNodes, &scopedEdges, &scopedOrdered)
	require.NoError(t, err)
	require.False(t, legacyNodes)
	require.False(t, legacyEdges)
	require.False(t, legacyOrdered)
	require.True(t, scopedNodes)
	require.True(t, scopedEdges)
	require.True(t, scopedOrdered)
}
