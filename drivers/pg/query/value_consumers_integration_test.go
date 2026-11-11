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

// TestCypherValueConsumersSchema checks typed and null property values through PostgreSQL Cypher consumers.
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
		// value supplies the Cypher value expression under test.
		value string
		// consumer selects the SQL consumer applied to the decoded value.
		consumer string
		// want contains the expected scalar result from the value consumer.
		want any
	}{
		{
			value:    `"a\"b\\c\n雪"`,
			consumer: "toString",
			want:     "a\"b\\c\n雪",
		},
		{
			value:    `""`,
			consumer: "toString",
			want:     "",
		}, {
			value:    "true",
			consumer: "toString",
			want:     "true",
		},
		{
			value:    "9007199254740993",
			consumer: "toString",
			want:     "9007199254740993",
		},
		{
			value:    "1.25",
			consumer: "toString",
			want:     "1.25",
		}, {
			value:    "null",
			consumer: "toString",
			want:     nil,
		},
		{
			value:    `"ALIce"`,
			consumer: "toLower",
			want:     "ALIce",
		}, {
			value:    "null",
			consumer: "toLower",
			want:     nil,
		},
	} {
		t.Run(tc.value+tc.consumer, func(t *testing.T) {
			var got any
			require.NoError(t, pool.QueryRow(ctx, "select cypher_text($1::jsonb, $2)", tc.value, tc.consumer).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
	for _, tc := range []struct {
		// value supplies the Cypher value expression under test.
		value string
		// want contains the expected scalar result from the value consumer.
		want int64
		// null reports whether the consumer is expected to produce SQL NULL.
		null bool
	}{
		{
			value: `"a雪😀"`,
			want:  3,
			null:  false,
		}, {
			value: `""`,
			want:  0,
			null:  false,
		}, {
			value: "[1,2]",
			want:  2,
			null:  false,
		}, {
			value: "[]",
			want:  0,
			null:  false,
		},
		{
			value: "[null,[1,2],{}]",
			want:  3,
			null:  false,
		}, {
			value: "null",
			want:  0,
			null:  true,
		},
	} {
		var got pgtype.Int8
		require.NoError(t, pool.QueryRow(ctx, "select cypher_size($1::jsonb)", tc.value).Scan(&got))
		require.Equal(t, !tc.null, got.Valid)
		require.Equal(t, tc.want, got.Int64)
	}
	for _, tc := range []struct {
		// sql is the SQL statement or identifier spelling exercised by the case.
		sql string
		// value is the input value supplied to the statement or setting.
		value string
	}{
		{
			sql:   "select cypher_text($1::jsonb,'toString')",
			value: "[]",
		},
		{
			sql:   "select cypher_text($1::jsonb,'toString')",
			value: "{}",
		},
		{
			sql:   "select cypher_text($1::jsonb,'toLower')",
			value: "123",
		},
		{
			sql:   "select cypher_text($1::jsonb,'split')",
			value: "true",
		},
		{
			sql:   "select cypher_size($1::jsonb)",
			value: "123",
		},
		{
			sql:   "select cypher_size($1::jsonb)",
			value: "{}",
		},
		{
			sql:   "select cypher_list($1::jsonb)",
			value: "{}",
		},
		{
			sql:   "select cypher_list($1::jsonb)",
			value: "123",
		},
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
