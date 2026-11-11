//go:build manual_integration && integration

package query

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestCypherSumSchema checks numeric sum behavior through the installed PostgreSQL Cypher functions.
func TestCypherSumSchema(t *testing.T) {
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
		// name identifies the numeric sum case in subtest output.
		name string
		// values supplies the SQL input values passed to the Cypher sum implementation.
		values string
		// want contains the expected textual representation of the aggregate result.
		want string
	}{
		{
			name:   "empty",
			values: "[]",
			want:   "0",
		},
		{
			name:   "SQL and JSON nulls",
			values: "[null, null]",
			want:   "0",
		},
		{
			name:   "large",
			values: "[9007199254740993, 2, null]",
			want:   "9007199254740995",
		},
		{
			name:   "maximum",
			values: "[9223372036854775807]",
			want:   "9223372036854775807",
		},
		{
			name:   "minimum",
			values: "[-9223372036854775808]",
			want:   "-9223372036854775808",
		},
		{
			name:   "float",
			values: "[1.25, 2.5]",
			want:   "3.75",
		},
		{
			name:   "integral float",
			values: "[1.25, 0.75]",
			want:   "2.0",
		},
		{
			name:   "float input",
			values: "[1.0]",
			want:   "1.0",
		},
		{
			name:   "mixed",
			values: "[2, 1.25]",
			want:   "3.25",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			require.NoError(t, pool.QueryRow(ctx, "select cypher_sum(value)::text from jsonb_array_elements($1::jsonb) as v(value)", tc.values).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
	var distinct string
	require.NoError(t, pool.QueryRow(ctx, "select cypher_sum(distinct value)::text from jsonb_array_elements('[9007199254740993,9007199254740993,2]'::jsonb) as v(value)").Scan(&distinct))
	require.Equal(t, "9007199254740995", distinct)
	require.NoError(t, pool.QueryRow(ctx, "select cypher_sum(value)::text from (values (null::jsonb), ('null'::jsonb)) as v(value)").Scan(&distinct))
	require.Equal(t, "0", distinct)
	for _, tc := range []struct {
		// values is the JSON array supplied to the numeric aggregate.
		values string
		// code is the database error code required for the rejected input.
		code string
	}{
		{
			values: `["123"]`,
			code:   "22023",
		}, {
			values: "[true]",
			code:   "22023",
		}, {
			values: "[[1]]",
			code:   "22023",
		}, {
			values: "[{}]",
			code:   "22023",
		},
		{
			values: "[9223372036854775807, 1]",
			code:   "22003",
		}, {
			values: "[-9223372036854775808, -1]",
			code:   "22003",
		},
	} {
		_, err := pool.Exec(ctx, "select cypher_sum(value) from jsonb_array_elements($1::jsonb) as v(value)", tc.values)
		require.ErrorContains(t, err, "SQLSTATE "+tc.code)
	}
}
