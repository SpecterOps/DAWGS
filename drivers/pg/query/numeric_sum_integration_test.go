//go:build manual_integration && integration

package query

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

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
		name, values, want string
	}{
		{"empty", "[]", "0"},
		{"SQL and JSON nulls", "[null, null]", "0"},
		{"large", "[9007199254740993, 2, null]", "9007199254740995"},
		{"maximum", "[9223372036854775807]", "9223372036854775807"},
		{"minimum", "[-9223372036854775808]", "-9223372036854775808"},
		{"float", "[1.25, 2.5]", "3.75"},
		{"integral float", "[1.25, 0.75]", "2.0"},
		{"float input", "[1.0]", "1.0"},
		{"mixed", "[2, 1.25]", "3.25"},
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
	for _, tc := range []struct{ values, code string }{
		{`["123"]`, "22023"}, {"[true]", "22023"}, {"[[1]]", "22023"}, {"[{}]", "22023"},
		{"[9223372036854775807, 1]", "22003"}, {"[-9223372036854775808, -1]", "22003"},
	} {
		_, err := pool.Exec(ctx, "select cypher_sum(value) from jsonb_array_elements($1::jsonb) as v(value)", tc.values)
		require.ErrorContains(t, err, "SQLSTATE "+tc.code)
	}
}
