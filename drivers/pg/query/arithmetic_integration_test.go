//go:build manual_integration && integration

package query

import (
	"context"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCypherArithmeticSchema(t *testing.T) {
	connection := os.Getenv("CONNECTION_STRING")
	if !isPostgreSQLConnection(connection) {
		t.Skip("CONNECTION_STRING does not select PostgreSQL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connection)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	// An existing installation receives the helper through ordinary assertion;
	// asserting it again must keep the same callable signature.
	for range 2 {
		_, err := pool.Exec(ctx, sqlSchemaUp)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		left, right, op, want, code string
	}{
		{"9007199254740992", "1", "+", "9007199254740993", ""},
		{"-7", "2", "/", "-3", ""},
		{"7", "-2", "%", "1", ""},
		{"7.5", "2.5", "+", "10.0", ""},
		{"7.0", "2", "/", "3.5", ""},
		{"1.0", "0.1", "%", "0.09999999999999995", ""},
		{`"a"`, `"b"`, "+", `"ab"`, ""},
		{`"a"`, "2.0", "+", `"a2.0"`, ""},
		{"[]", "[1]", "+", "[1]", ""},
		{"[1]", "2", "+", "[1, 2]", ""},
		{"2", "[1]", "+", "[2, 1]", ""},
		{"null", "2", "+", "", ""},
		{"2", "null", "%", "", ""},
		{"true", "false", "+", "", "22023"},
		{`"7"`, `"2"`, "%", "", "22023"},
		{"{}", "2", "*", "", "22023"},
		{"9223372036854775807", "1", "+", "", "22003"},
		{"7", "0", "/", "", "22012"},
		{"7.5", "0.0", "%", "", "22012"},
		{"7", "2", "unknown", "", "22023"},
	} {
		t.Run(tc.left+tc.op+tc.right, func(t *testing.T) {
			var got pgtype.Text
			err := pool.QueryRow(ctx, "select cypher_arithmetic($1::jsonb, $2::jsonb, $3)::text", tc.left, tc.right, tc.op).Scan(&got)
			if tc.code != "" {
				var pgError *pgconn.PgError
				require.ErrorAs(t, err, &pgError)
				require.Equal(t, tc.code, pgError.Code)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want != "", got.Valid)
				require.Equal(t, tc.want, got.String)
			}
		})
	}
	var missing pgtype.Text
	require.NoError(t, pool.QueryRow(ctx, "select cypher_arithmetic(null, '2', '+')::text").Scan(&missing))
	require.False(t, missing.Valid)

	// Decimal modulo is not floating remainder. Compare the helper against Go's
	// binary fmod across signs, exponent gaps, and deterministic random values.
	random := rand.New(rand.NewPCG(16, 16))
	for range 100 {
		left, right := math.Float64frombits(random.Uint64()), math.Float64frombits(random.Uint64())
		if math.IsNaN(left) || math.IsInf(left, 0) || math.IsNaN(right) || math.IsInf(right, 0) || right == 0 {
			continue
		}
		var result string
		err := pool.QueryRow(ctx, "select cypher_arithmetic($1::jsonb, $2::jsonb, '%')::text",
			strconv.FormatFloat(left, 'e', -1, 64), strconv.FormatFloat(right, 'e', -1, 64)).Scan(&result)
		require.NoError(t, err, "%g %% %g", left, right)
		got, err := strconv.ParseFloat(result, 64)
		require.NoError(t, err)
		require.Equal(t, math.Mod(left, right), got, "%g %% %g", left, right)
	}
}
