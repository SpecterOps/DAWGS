package pg

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

// postgresIntegrationConnectionString returns CONNECTION_STRING only for a PostgreSQL target and skips the driver-scoped test otherwise.
func postgresIntegrationConnectionString(t *testing.T) string {
	t.Helper()

	connectionString := os.Getenv("CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("CONNECTION_STRING env var is not set")
	}

	normalizedConnectionString := strings.ToLower(connectionString)
	if !strings.HasPrefix(normalizedConnectionString, "postgres://") &&
		!strings.HasPrefix(normalizedConnectionString, "postgresql://") {
		t.Skip("CONNECTION_STRING is not a PostgreSQL connection string")
	}

	return connectionString
}

// connectCompositeCodecIntegration opens a timeout-bounded PostgreSQL connection and registers cleanup for composite-codec integration tests.
func connectCompositeCodecIntegration(t *testing.T) (context.Context, *pgx.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	config, err := pgx.ParseConfig(postgresIntegrationConnectionString(t))
	require.NoError(t, err)

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	// Keep this driver-scoped test independent of application data and schema
	// state. PostgreSQL drops types in pg_temp with the connection.
	_, err = conn.Exec(ctx, `
set search_path = pg_temp, public;
create type pg_temp.nodeComposite as (
  id bigint,
  kind_ids smallint[],
  properties jsonb
);
create type pg_temp.edgeComposite as (
  id bigint,
  start_id bigint,
  end_id bigint,
  kind_id smallint,
  properties jsonb
);
create type pg_temp.pathComposite as (
  nodes nodeComposite[],
  edges edgeComposite[]
);`)
	require.NoError(t, err)

	require.NoError(t, AfterPooledConnectionEstablished(ctx, conn))

	return ctx, conn
}

// TestPostgresOwnedCompositeCodecRegistration verifies pooled connections register optimized codecs for every owned composite type.
func TestPostgresOwnedCompositeCodecRegistration(t *testing.T) {
	_, conn := connectCompositeCodecIntegration(t)
	typeMap := conn.TypeMap()

	nodeType, typeOK := typeMap.TypeForName(pgsql.NodeComposite.String())
	require.True(t, typeOK)
	require.IsType(t, &ownedCompositeCodec[nodeComposite]{}, nodeType.Codec)

	nodeArrayType, typeOK := typeMap.TypeForName(pgsql.NodeCompositeArray.String())
	require.True(t, typeOK)
	nodeArrayCodec, typeOK := nodeArrayType.Codec.(*ownedCompositeArrayCodec[nodeComposite])
	require.True(t, typeOK)
	require.Same(t, nodeType, nodeArrayCodec.arrayCodec.ElementType)

	edgeType, typeOK := typeMap.TypeForName(pgsql.EdgeComposite.String())
	require.True(t, typeOK)
	require.IsType(t, &ownedCompositeCodec[edgeComposite]{}, edgeType.Codec)

	pathType, typeOK := typeMap.TypeForName(pgsql.PathComposite.String())
	require.True(t, typeOK)
	require.IsType(t, &ownedCompositeCodec[pathComposite]{}, pathType.Codec)
}

// TestPostgresOwnedCompositeCodecRowsValues verifies Rows.Values returns driver-owned node and edge composites.
func TestPostgresOwnedCompositeCodecRowsValues(t *testing.T) {
	ctx, conn := connectCompositeCodecIntegration(t)

	for _, testCase := range []struct {
		// name identifies the wire-format subtest.
		name string

		// format selects the pgx result format used by the query.
		format int16
	}{
		{
			name:   "binary",
			format: pgtype.BinaryFormatCode,
		},
		{
			name:   "text",
			format: pgtype.TextFormatCode,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := conn.Query(ctx, `
select (series.id, array[1::smallint, 2::smallint], jsonb_build_object('id', series.id))::nodeComposite
from generate_series(101::bigint, 102::bigint) series(id)
order by series.id`, pgx.QueryResultFormats{testCase.format})
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			firstValues, err := rows.Values()
			require.NoError(t, err)
			require.Len(t, firstValues, 1)
			first, typeOK := firstValues[0].(nodeComposite)
			require.True(t, typeOK)
			require.Equal(t, int64(101), first.ID)
			require.Equal(t, []int16{1, 2}, first.KindIDs)

			require.True(t, rows.Next())
			secondValues, err := rows.Values()
			require.NoError(t, err)
			second, typeOK := secondValues[0].(nodeComposite)
			require.True(t, typeOK)
			require.Equal(t, int64(102), second.ID)

			// Reading the next row must not overwrite data retained from the
			// first Rows.Values call.
			require.Equal(t, int64(101), first.ID)
			require.Equal(t, []int16{1, 2}, first.KindIDs)
			require.Equal(t, float64(101), first.Properties["id"])

			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}

// TestPostgresOwnedCompositeCodecArraysAndPaths verifies composite arrays and paths decode into their typed graph representations.
func TestPostgresOwnedCompositeCodecArraysAndPaths(t *testing.T) {
	ctx, conn := connectCompositeCodecIntegration(t)

	for _, testCase := range []struct {
		// name identifies the wire-format subtest.
		name string

		// format selects the pgx result format used by the query.
		format int16
	}{
		{
			name:   "binary",
			format: pgtype.BinaryFormatCode,
		},
		{
			name:   "text",
			format: pgtype.TextFormatCode,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := conn.Query(ctx, `
select
  array[
    (101, array[1::smallint], '{"name":"first"}'::jsonb)::nodeComposite,
    null::nodeComposite,
    (102, array[2::smallint], '{"name":"second"}'::jsonb)::nodeComposite
  ]::nodeComposite[],
  (
    array[
      (101, array[1::smallint], '{"name":"first"}'::jsonb)::nodeComposite,
      (102, array[2::smallint], '{"name":"second"}'::jsonb)::nodeComposite
    ]::nodeComposite[],
    array[
      (201, 101, 102, 3::smallint, '{"name":"edge"}'::jsonb)::edgeComposite
    ]::edgeComposite[]
  )::pathComposite`, pgx.QueryResultFormats{testCase.format})
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			values, err := rows.Values()
			require.NoError(t, err)
			require.Len(t, values, 2)

			nodes, typeOK := values[0].([]any)
			require.True(t, typeOK)
			require.Len(t, nodes, 3)
			require.IsType(t, nodeComposite{}, nodes[0])
			require.Nil(t, nodes[1])
			require.IsType(t, nodeComposite{}, nodes[2])

			path, typeOK := values[1].(pathComposite)
			require.True(t, typeOK)
			require.Len(t, path.Nodes, 2)
			require.Len(t, path.Edges, 1)
			require.Equal(t, int64(101), path.Nodes[0].ID)
			require.Equal(t, int64(102), path.Nodes[1].ID)
			require.Equal(t, int64(201), path.Edges[0].ID)

			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}

// TestPostgresOwnedCompositeCodecNullInternalFieldFallback verifies nullable composite fields retain pgx's lossless fallback representation.
func TestPostgresOwnedCompositeCodecNullInternalFieldFallback(t *testing.T) {
	ctx, conn := connectCompositeCodecIntegration(t)

	for _, testCase := range []struct {
		// name identifies the wire-format subtest.
		name string

		// format selects the pgx result format used by the query.
		format int16
	}{
		{
			name:   "binary",
			format: pgtype.BinaryFormatCode,
		},
		{
			name:   "text",
			format: pgtype.TextFormatCode,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rows, err := conn.Query(ctx, `
select
  (null::bigint, array[1::smallint], '{"name":"nullable"}'::jsonb)::nodeComposite,
  array[(null::bigint, array[1::smallint], '{"name":"nullable"}'::jsonb)::nodeComposite]::nodeComposite[]`,
				pgx.QueryResultFormats{testCase.format})
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			values, err := rows.Values()
			require.NoError(t, err)
			require.Len(t, values, 2)

			node, typeOK := values[0].(map[string]any)
			require.True(t, typeOK)
			require.Nil(t, node["id"])
			require.Equal(t, []any{int16(1)}, node["kind_ids"])

			nodes, typeOK := values[1].([]any)
			require.True(t, typeOK)
			require.Len(t, nodes, 1)
			node, typeOK = nodes[0].(map[string]any)
			require.True(t, typeOK)
			require.Nil(t, node["id"])

			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
