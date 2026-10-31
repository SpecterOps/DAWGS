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

func TestCypherCompareSchema(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if !isPostgreSQLConnection(connection) {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	for range 2 {
		_, err := pool.Exec(ctx, sqlSchemaUp)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		left, right string
		want        int32
		null        bool
	}{
		{"123", "200", -1, false}, {"123", "123.0", 0, false},
		{"9007199254740993", "9007199254740992", 1, false},
		{"-9223372036854775808", "9223372036854775807", -1, false},
		{`"123"`, "200", 0, true}, {"123", `"200"`, 0, true},
		{"true", "1", 0, true}, {"false", "true", -1, false},
		{`"Z"`, `"a"`, -1, false}, {`"a"`, `"aa"`, -1, false},
		{`"é"`, `"z"`, 1, false}, {`"a"`, `"a"`, 0, false},
		{"[2]", "[1, 3]", 1, false}, {"[1, 2]", "[1, 2, 0]", -1, false},
		{"[]", "[]", 0, false}, {"[]", "[null]", -1, false},
		{"[1, 2]", "[1.0, 2.0]", 0, false},
		{"[null, 1]", "[null, 2]", 0, true},
		{"[1, null]", "[1, 3]", 0, true},
		{`[1, "2"]`, "[1, 3]", 0, true},
		{"[[1, 2], 3]", "[[1, 3], 0]", -1, false},
		{"[]", "1", 0, true}, {"{}", "{}", 0, true},
		{"null", "null", 0, true}, {"null", "1", 0, true},
	} {
		t.Run(tc.left+" vs "+tc.right, func(t *testing.T) {
			var got pgtype.Int4
			require.NoError(t, pool.QueryRow(ctx, "select cypher_compare($1::jsonb, $2::jsonb)", tc.left, tc.right).Scan(&got))
			require.Equal(t, !tc.null, got.Valid)
			require.Equal(t, tc.want, got.Int32)
			var reverse pgtype.Int4
			require.NoError(t, pool.QueryRow(ctx, "select cypher_compare($2::jsonb, $1::jsonb)", tc.left, tc.right).Scan(&reverse))
			require.Equal(t, got.Valid, reverse.Valid)
			require.Equal(t, -got.Int32, reverse.Int32)
		})
	}
	var missing pgtype.Int4
	require.NoError(t, pool.QueryRow(ctx, "select cypher_compare(null, '1')").Scan(&missing))
	require.False(t, missing.Valid)
}
