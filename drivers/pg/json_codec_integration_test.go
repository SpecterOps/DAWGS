package pg

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// TestPostgresJSONResultNumbers exercises real wire formats, including the
// nullable-composite fallback that does not use the owned property scanner.
func TestPostgresJSONResultNumbers(t *testing.T) {
	ctx, conn := connectCompositeCodecIntegration(t)
	for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
		rows, err := conn.Query(ctx, `select
 '4611686018427387905'::json,
 '9223372036854775807'::jsonb,
 '{"nested":[-9223372036854775808,9007199254740993,1.25],"text":"4611686018427387905"}'::jsonb,
 array['4611686018427387905'::json, null::json],
 array['-9223372036854775808'::jsonb, null::jsonb],
 (null::bigint, array[1::smallint], '{"n":4611686018427387905}'::jsonb)::nodeComposite`, pgx.QueryResultFormats{format})
		require.NoError(t, err)
		result := &queryResult{ctx: ctx, rows: rows}
		require.True(t, result.Next(), "%v", result.Error())
		values := result.Values()
		require.Equal(t, int64(4611686018427387905), values[0])
		require.Equal(t, int64(math.MaxInt64), values[1])
		require.Equal(t, map[string]any{"nested": []any{int64(math.MinInt64), int64(9007199254740993), 1.25}, "text": "4611686018427387905"}, values[2])
		require.Equal(t, []any{int64(4611686018427387905), nil}, values[3])
		require.Equal(t, []any{int64(math.MinInt64), nil}, values[4])
		fallback, ok := values[5].(map[string]any)
		require.True(t, ok)
		require.Nil(t, fallback["id"])
		require.Equal(t, map[string]any{"n": int64(4611686018427387905)}, fallback["properties"])
		require.False(t, result.Next())
		require.NoError(t, result.Error())
		result.Close()
	}
}

func TestPostgresJSONResultRejectsUnsupportedNumbers(t *testing.T) {
	ctx, conn := connectCompositeCodecIntegration(t)
	for _, format := range []int16{pgtype.TextFormatCode, pgtype.BinaryFormatCode} {
		for _, statement := range []string{
			`select '9223372036854775808'::json`,
			`select '-9223372036854775809'::jsonb`,
			`select '{"nested":[1e309]}'::jsonb`,
			`select array['9223372036854775808'::jsonb]`,
			`select (1, array[1::smallint], '{"n":9223372036854775808}'::jsonb)::nodeComposite`,
			`select (null::bigint, array[1::smallint], '{"n":9223372036854775808}'::jsonb)::nodeComposite`,
		} {
			rows, err := conn.Query(ctx, statement, pgx.QueryResultFormats{format})
			require.NoError(t, err)
			result := &queryResult{ctx: ctx, rows: rows}
			require.False(t, result.Next(), statement)
			require.ErrorContains(t, result.Error(), "range", statement)
			require.False(t, result.Next())
			result.Close()
		}
	}
}
