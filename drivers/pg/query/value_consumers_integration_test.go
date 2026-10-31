//go:build manual_integration && integration

package query

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCypherValueConsumersSchema(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if !isPostgreSQLConnection(connection) {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	for range 2 {
		_, err = pool.Exec(ctx, sqlSchemaUp)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		value, consumer string
		want            any
	}{
		{`"a\"b\\c\n雪"`, "toString", "a\"b\\c\n雪"},
		{`""`, "toString", ""}, {"true", "toString", "true"},
		{"9007199254740993", "toString", "9007199254740993"},
		{"1.25", "toString", "1.25"}, {"null", "toString", nil},
		{`"ALIce"`, "toLower", "ALIce"}, {"null", "toLower", nil},
	} {
		t.Run(tc.value+tc.consumer, func(t *testing.T) {
			var got any
			require.NoError(t, pool.QueryRow(ctx, "select cypher_text($1::jsonb, $2)", tc.value, tc.consumer).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct {
		value string
		want  int64
		null  bool
	}{
		{`"a雪😀"`, 3, false}, {`""`, 0, false}, {"[1,2]", 2, false}, {"[]", 0, false},
		{"[null,[1,2],{}]", 3, false}, {"null", 0, true},
	} {
		var got pgtype.Int8
		require.NoError(t, pool.QueryRow(ctx, "select cypher_size($1::jsonb)", tc.value).Scan(&got))
		require.Equal(t, !tc.null, got.Valid)
		require.Equal(t, tc.want, got.Int64)
	}
	for _, tc := range []struct{ sql, value string }{
		{"select cypher_text($1::jsonb,'toString')", "[]"},
		{"select cypher_text($1::jsonb,'toString')", "{}"},
		{"select cypher_text($1::jsonb,'toLower')", "123"},
		{"select cypher_text($1::jsonb,'split')", "true"},
		{"select cypher_size($1::jsonb)", "123"},
		{"select cypher_size($1::jsonb)", "{}"},
		{"select cypher_list($1::jsonb)", "{}"},
		{"select cypher_list($1::jsonb)", "123"},
	} {
		_, err := pool.Exec(ctx, tc.sql, tc.value)
		require.ErrorContains(t, err, "SQLSTATE 22023")
	}
	for _, value := range []string{"[1, true, null, [2], {}]", "[]", "null"} {
		var valid bool
		require.NoError(t, pool.QueryRow(ctx, "select cypher_list($1::jsonb) is not distinct from nullif($1::jsonb, 'null'::jsonb)", value).Scan(&valid))
		require.True(t, valid)
	}
	var missing any
	require.NoError(t, pool.QueryRow(ctx, "select cypher_text(null,'toString')").Scan(&missing))
	require.Nil(t, missing)
	require.NoError(t, pool.QueryRow(ctx, "select cypher_size(null)").Scan(&missing))
	require.Nil(t, missing)
}
