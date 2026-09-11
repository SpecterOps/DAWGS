//go:build manual_integration

package query

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// Exercise the actual schema DDL in a transaction-local schema. Stop before the
// public helper functions so the tests never replace functions in another schema.
func TestPostgreSQLEdgeSchemaIndexes(t *testing.T) {
	connectionString := os.Getenv("CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("CONNECTION_STRING is not set")
	}
	connectionURL, err := url.Parse(connectionString)
	require.NoError(t, err)
	if connectionURL.Scheme != "postgres" && connectionURL.Scheme != "postgresql" {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}

	schemaDDL, _, found := strings.Cut(sqlSchemaUp, "create or replace function public.kind_name")
	require.True(t, found, "schema DDL must precede the public helper functions")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, connectionString)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	for _, testCase := range []struct {
		name       string
		legacyKeys string
		indexes    string
	}{
		{name: "fresh schema"},
		{
			name:       "graph-first uniqueness and narrow indexes",
			legacyKeys: "graph_id, start_id, end_id, kind_id",
			indexes: `
				create index edge_graph_id_index on edge (graph_id);
				create index edge_start_id_index on edge (start_id);
				create index edge_end_id_index on edge (end_id);
				create index edge_kind_index on edge (kind_id);
				create index edge_start_kind_index on edge (start_id, kind_id);
				create index edge_end_kind_index on edge (end_id, kind_id);`,
		},
		{
			name:       "start-first uniqueness and covering indexes",
			legacyKeys: "start_id, end_id, kind_id, graph_id",
			indexes: `
				create index edge_start_id_kind_id_id_end_id_index on edge (start_id, kind_id) include (id, end_id);
				create index edge_end_id_kind_id_id_start_id_index on edge (end_id, kind_id) include (id, start_id);
				create index edge_kind_id_id_start_id_end_id_index on edge (kind_id) include (id, start_id, end_id);`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tx, err := conn.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(context.Background())
			schemaName := pgx.Identifier{fmt.Sprintf("dawgs_edge_indexes_%d", time.Now().UnixNano())}.Sanitize()
			execEdgeSchemaSQL(t, ctx, tx, "create schema "+schemaName)
			execEdgeSchemaSQL(t, ctx, tx, "set local search_path to "+schemaName)

			if testCase.legacyKeys == "" {
				execEdgeSchemaSQL(t, ctx, tx, schemaDDL)
			} else {
				execEdgeSchemaSQL(t, ctx, tx, fmt.Sprintf(`
					create table graph (id bigserial primary key, name varchar(256) not null unique);
					create table edge (
						id bigserial not null,
						graph_id integer not null references graph (id) on delete cascade,
						start_id bigint not null,
						end_id bigint not null,
						kind_id smallint not null,
						properties jsonb not null,
						primary key (id, graph_id),
						unique (%s)
					) partition by list (graph_id);
					%s`, testCase.legacyKeys, testCase.indexes))
			}

			execEdgeSchemaSQL(t, ctx, tx, `
				insert into graph (id, name) values (1, 'first'), (2, 'second');
				create table edge_first partition of edge for values in (1);
				create table edge_second partition of edge for values in (2);
				create index edge_property_fixture_index on edge ((properties ->> 'name'));
				insert into edge (id, graph_id, start_id, end_id, kind_id, properties) values
					(10, 1, 100, 101, 1, '{"keep": true}'),
					(11, 1, 101, 102, 1, '{}'),
					(12, 1, 101, 103, 2, '{}'),
					(10, 2, 100, 101, 1, '{}');
				insert into edge (id, graph_id, start_id, end_id, kind_id, properties)
				select n, 1, n, n + 10000, 1, '{}'::jsonb from generate_series(1000, 2999) n;`)

			var preservedIndexes []int64
			require.NoError(t, tx.QueryRow(ctx, `
				select array_agg(indexrelid::bigint order by indexrelid)
				from pg_index
				where indrelid in ('edge'::regclass, 'edge_first'::regclass, 'edge_second'::regclass)
				  and (indisprimary or indexprs is not null)`).Scan(&preservedIndexes))

			execEdgeSchemaSQL(t, ctx, tx, schemaDDL)
			for _, table := range []string{"edge", "edge_first", "edge_second"} {
				assertEdgeSchemaIndexes(t, ctx, tx, table)
			}
			var remainingIndexes int
			require.NoError(t, tx.QueryRow(ctx, `select count(*) from pg_index where indexrelid::bigint = any($1)`, preservedIndexes).Scan(&remainingIndexes))
			require.Equal(t, len(preservedIndexes), remainingIndexes, "primary and property indexes must survive migration")

			// A second assertion must preserve all parent and child index OIDs.
			var before, after []int64
			const indexOIDs = `select array_agg(indexrelid::bigint order by indexrelid) from pg_index
				where indrelid in ('edge'::regclass, 'edge_first'::regclass, 'edge_second'::regclass)`
			require.NoError(t, tx.QueryRow(ctx, indexOIDs).Scan(&before))
			execEdgeSchemaSQL(t, ctx, tx, schemaDDL)
			require.NoError(t, tx.QueryRow(ctx, indexOIDs).Scan(&after))
			require.Equal(t, before, after, "schema reapplication must not rebuild indexes")

			// Partitions created after the upgrade must inherit the same indexes.
			execEdgeSchemaSQL(t, ctx, tx, "create table edge_third partition of edge for values in (3)")
			assertEdgeSchemaIndexes(t, ctx, tx, "edge_third")
			assertEdgeSchemaUniqueness(t, ctx, tx)
			assertEdgeSchemaTraversalPlans(t, ctx, tx)
		})
	}
}

func execEdgeSchemaSQL(t *testing.T, ctx context.Context, tx pgx.Tx, statement string) {
	t.Helper()
	_, err := tx.Exec(ctx, statement)
	require.NoError(t, err)
}

type edgeSchemaIndex struct {
	name     string
	keys     string
	included string
	unique   bool
	primary  bool
}

func assertEdgeSchemaIndexes(t *testing.T, ctx context.Context, tx pgx.Tx, table string) map[string]edgeSchemaIndex {
	t.Helper()
	rows, err := tx.Query(ctx, `
		select c.relname, i.indisunique, i.indisprimary, i.indisvalid,
			array(select pg_get_indexdef(i.indexrelid, n, false)
				from generate_series(1, i.indnkeyatts) n order by n),
			array(select pg_get_indexdef(i.indexrelid, n, false)
				from generate_series(i.indnkeyatts + 1, i.indnatts) n order by n)
		from pg_index i join pg_class c on c.oid = i.indexrelid
		where i.indrelid = $1::regclass and i.indexprs is null`, table)
	require.NoError(t, err)
	defer rows.Close()

	indexes := map[string]edgeSchemaIndex{}
	for rows.Next() {
		var (
			index          edgeSchemaIndex
			keys, included []string
			valid          bool
		)
		require.NoError(t, rows.Scan(&index.name, &index.unique, &index.primary, &valid, &keys, &included))
		require.True(t, valid, "index %s must be valid", index.name)
		index.keys, index.included = strings.Join(keys, ","), strings.Join(included, ",")
		require.NotContains(t, indexes, index.keys, "duplicate baseline index on %s", table)
		indexes[index.keys] = index
	}
	require.NoError(t, rows.Err())
	require.Len(t, indexes, 4, "table %s must have exactly four baseline indexes", table)
	for _, expected := range []edgeSchemaIndex{
		{keys: "id,graph_id", unique: true, primary: true},
		{keys: "start_id,kind_id,end_id,graph_id", included: "id", unique: true},
		{keys: "end_id,kind_id", included: "id,start_id"},
		{keys: "kind_id"},
	} {
		actual, exists := indexes[expected.keys]
		require.True(t, exists, "missing index on %s (%s)", table, expected.keys)
		require.Equal(t, expected.included, actual.included)
		require.Equal(t, expected.unique, actual.unique)
		require.Equal(t, expected.primary, actual.primary)
	}
	if table == "edge" {
		require.Equal(t, "edge_start_id_kind_id_end_id_graph_id_key", indexes["start_id,kind_id,end_id,graph_id"].name)
		require.Equal(t, "edge_inbound_traversal_index", indexes["end_id,kind_id"].name)
		require.Equal(t, "edge_kind_index", indexes["kind_id"].name)
	}
	return indexes
}

func assertEdgeSchemaUniqueness(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	var count int
	require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge").Scan(&count))
	require.Equal(t, 2004, count, "migration must preserve edges in both graphs")

	// The existing conflict target order must still infer the covering constraint,
	// even though the proposed edge has a different ID (an INCLUDE column).
	var id int64
	var properties []byte
	require.NoError(t, tx.QueryRow(ctx, `
		insert into edge as e (id, graph_id, start_id, end_id, kind_id, properties)
		values (99, 1, 100, 101, 1, '{"updated": true}')
		on conflict (start_id, end_id, kind_id, graph_id)
		do update set properties = e.properties || excluded.properties
		returning id, properties`).Scan(&id, &properties))
	require.Equal(t, int64(10), id)
	require.JSONEq(t, `{"keep": true, "updated": true}`, string(properties))

	execEdgeSchemaSQL(t, ctx, tx, "savepoint duplicate_edge")
	_, err := tx.Exec(ctx, `insert into edge (id, graph_id, start_id, end_id, kind_id, properties)
		values (99, 1, 100, 101, 1, '{}')`)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, "23505", pgErr.Code)
	execEdgeSchemaSQL(t, ctx, tx, "rollback to savepoint duplicate_edge")

	require.NoError(t, tx.QueryRow(ctx, "select count(*) from edge").Scan(&count))
	require.Equal(t, 2004, count)
}

func assertEdgeSchemaTraversalPlans(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	indexes := assertEdgeSchemaIndexes(t, ctx, tx, "edge_first")
	// Analyze the unrelated edges so an endpoint lookup is more selective than a
	// kind lookup. Cost controls test index-only eligibility rather than a preference
	// for sequential scans. No heap-fetch claim is made for these uncommitted rows.
	execEdgeSchemaSQL(t, ctx, tx, "analyze edge_first; set local enable_seqscan = off; set local enable_bitmapscan = off")
	for _, direction := range []struct {
		anchor, next, keys string
		root               int
	}{
		{anchor: "start_id", next: "end_id", keys: "start_id,kind_id,end_id,graph_id", root: 100},
		{anchor: "end_id", next: "start_id", keys: "end_id,kind_id", root: 102},
	} {
		statement := fmt.Sprintf(`
			with recursive walk(next_id, path) as (
				select %d::bigint, array[]::bigint[]
				union all
				select e.%s, w.path || e.id from walk w
				join lateral (
					select id, %s from edge_first e
					where e.%s = w.next_id and e.kind_id = 1 and e.id != all(w.path)
					offset 0
				) e on true
			) select array_agg(next_id order by next_id) from walk`, direction.root, direction.next, direction.next, direction.anchor)
		var ids []int64
		require.NoError(t, tx.QueryRow(ctx, statement).Scan(&ids))
		require.Equal(t, []int64{100, 101, 102}, ids)
		plan := edgeSchemaPlan(t, ctx, tx, statement)
		require.Contains(t, plan, "Recursive Union")
		require.Contains(t, plan, "Index Only Scan using "+indexes[direction.keys].name)
	}
	plan := edgeSchemaPlan(t, ctx, tx, "select count(*) from edge_first where kind_id = 1")
	require.Contains(t, plan, "Index Only Scan using "+indexes["kind_id"].name)
}

func edgeSchemaPlan(t *testing.T, ctx context.Context, tx pgx.Tx, statement string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "explain (costs off) "+statement)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}
