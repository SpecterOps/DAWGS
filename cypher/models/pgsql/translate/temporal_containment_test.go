package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// Temporal constructions with an argument lower to a type cast, and their
// component lookups previously removed the cast operand without pushing a
// replacement or returning an error, underflowing the expression operand stack.
// Every component shape must now return an explicit translation error naming
// the unsupported component in both optimizer modes.
func TestTemporalCastComponentsReturnError(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				query   string
				params  map[string]any
				message string
			}{
				{
					"datetime literal epochmillis",
					`RETURN datetime('2020-01-01').epochmillis AS v`,
					nil,
					"unsupported temporal type component epochmillis on cast type timestamp with time zone",
				},
				{
					"datetime parameter epochmillis",
					`RETURN datetime($ts).epochmillis AS v`,
					map[string]any{"ts": "2020-01-01T00:00:00"},
					"unsupported temporal type component epochmillis on cast type timestamp with time zone",
				},
				{
					"date literal year",
					`RETURN date('2020-01-01').year AS v`,
					nil,
					"unsupported temporal type component year on cast type date",
				},
				{
					"localdatetime year",
					`RETURN localdatetime('2020-01-01T00:00:00').year AS v`,
					nil,
					"unsupported temporal type component year on cast type timestamp without time zone",
				},
				{
					"localtime hour",
					`RETURN localtime('12:00:00').hour AS v`,
					nil,
					"unsupported temporal type component hour on cast type time without time zone",
				},
				{
					"property argument",
					`MATCH (n:MemberOf) RETURN date(n.d).year AS v`,
					nil,
					"unsupported temporal type component year on cast type date",
				},
				{
					"where filter",
					`MATCH (n:MemberOf) WHERE date(n.d).year = 2020 RETURN n`,
					nil,
					"unsupported temporal type component year on cast type date",
				},
				{
					"where datetime comparison",
					`MATCH (n:MemberOf) WHERE datetime('2020-01-01').epochmillis > 0 RETURN n`,
					nil,
					"unsupported temporal type component epochmillis on cast type timestamp with time zone",
				},
				{
					"duration components keep the generic rejection",
					`RETURN duration('P3D').months AS v`,
					nil,
					"unsupported property lookup months on expression type interval",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					_, err = TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), tc.params, DefaultGraphID, Options{OptimizerMode: mode})
					require.EqualError(t, err, tc.message, "%s must return an explicit translation error instead of underflowing the operand stack", tc.query)
				})
			}
		})
	}
}

// Supported temporal lowering is unchanged: current-instant components, bare
// temporal projections, temporal comparisons and duration arithmetic keep their
// established SQL, while no-argument instant components retain their existing
// rejection message.
func TestTemporalConstructionControlsStillTranslate(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, fragment string
			}{
				{"current datetime epochseconds", `RETURN datetime().epochseconds AS v`, "extract(epoch from now()::timestamp with time zone)::numeric as v"},
				{"current datetime epochmillis", `RETURN datetime().epochmillis AS v`, "::numeric * 1000 as v"},
				{"bare date projection", `RETURN date('2020-01-01') AS v`, "::date as v"},
				{
					"datetime literal comparison",
					`MATCH (s:MemberOf) WHERE s.created_at < datetime('2019-06-01T18:40:32.142+0100') RETURN s`,
					"::timestamp with time zone < (@__strlit0::text)::timestamp with time zone",
				},
				{
					"localdatetime comparison",
					`MATCH (s:MemberOf) WHERE s.created_at < localdatetime('2019-06-01T18:40:32.142') RETURN s`,
					"::timestamp without time zone < (@__strlit0::text)::timestamp without time zone",
				},
				{
					"duration arithmetic",
					`MATCH (s:MemberOf) WHERE s.created_at < datetime() - duration('P3D') RETURN s`,
					"now()::timestamp with time zone - @__strlit0::interval",
				},
				{
					"epochseconds arithmetic",
					`MATCH (u:MemberOf) WHERE u.pwdlastset < (datetime().epochseconds - (365 * 86400)) RETURN u`,
					"extract(epoch from now()::timestamp with time zone)::numeric - (365 * 86400)",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)

					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, tc.fragment)
				})
			}

			// The no-argument instant functions keep their dedicated rejection for
			// components other than epochseconds/epochmillis.
			parsed, err := frontend.ParseCypher(frontend.NewContext(), `RETURN datetime().year AS v`)
			require.NoError(t, err)
			_, err = TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
			require.EqualError(t, err, "unsupported date time instant type component year from function call now")
		})
	}
}
