// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build manual_integration && integration

package query

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestUnidirectionalASPResolvesPairsIndependently protects both overloads from
// stopping at the first satisfied root or retiring a root before its other
// terminals are reached. Equal-depth witnesses survive; longer paths do not.
func TestUnidirectionalASPResolvesPairsIndependently(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if !isPostgreSQLConnection(connection) {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}

	ctx := t.Context()
	conn, err := pgx.Connect(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	_, err = conn.Exec(ctx, sqlSchemaUp)
	require.NoError(t, err)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `
		create temporary table edge (id int8, start_id int8, end_id int8) on commit drop;
		insert into edge values
			(101, 1, 2), (102, 1, 2), (103, 1, 10),
			(104, 10, 2), (105, 10, 4),
			(106, 3, 30), (107, 30, 4),
			(108, 3, 40), (109, 40, 2),
			(110, 5, 50), (111, 10, 11), (112, 11, 4);
	`)
	require.NoError(t, err)

	const primer = `insert into next_front (root_id, next_id, depth, satisfied, is_cycle, path)
		select e.start_id, e.end_id, 1, e.end_id in (2, 4), false, array[e.id]
		from edge e join traversal_root_filter r on r.id = e.start_id`
	const recursive = `insert into next_front (root_id, next_id, depth, satisfied, is_cycle, path)
		select f.root_id, e.end_id, f.depth + 1, e.end_id in (2, 4), false, f.path || e.id
		from forward_front f join edge e on e.start_id = f.next_id`

	for _, testCase := range []struct {
		name    string
		filters []any
	}{
		{name: "array filters", filters: []any{[]int64{1, 3, 5}, []int64{2, 4}}},
		{name: "array open terminals", filters: []any{[]int64{1, 3, 5}, []int64{}}},
		{name: "text filters", filters: []any{
			"insert into traversal_root_filter values (1), (3), (5)",
			"insert into traversal_terminal_filter values (2), (4)",
		}},
		{name: "text open terminals", filters: []any{
			"insert into traversal_root_filter values (1), (3), (5)", "",
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			filterType := "text"
			if _, ok := testCase.filters[0].([]int64); ok {
				filterType = "int8[]"
			}
			query := `select root_id, next_id, depth, path
				from public.unidirectional_asp_harness($1::text, $2::text, 4, $3::` + filterType + `, $4::` + filterType + `)
				order by root_id, next_id, path`

			// Reuse the same physical session so resolved pairs from a previous
			// invocation cannot suppress a later invocation's results.
			for range 2 {
				args := append([]any{primer, recursive}, testCase.filters...)
				rows, err := tx.Query(ctx, query, args...)
				require.NoError(t, err)
				type pathResult struct {
					RootID int64
					NextID int64
					Depth  int32
					Path   []int64
				}
				results, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pathResult])
				require.NoError(t, err)
				require.Equal(t, []pathResult{
					{1, 2, 1, []int64{101}},
					{1, 2, 1, []int64{102}},
					{1, 4, 2, []int64{103, 105}},
					{3, 2, 2, []int64{108, 109}},
					{3, 4, 2, []int64{106, 107}},
				}, results)
			}
		})
	}
}
