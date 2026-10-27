package pg

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

// TestTemporalCachePartitionsAndRebinds verifies both cache tiers separate local
// timestamps from zoned timestamps and rebind each zoned hit to its current value.
func TestTemporalCachePartitionsAndRebinds(t *testing.T) {
	for _, tier := range []string{"driver", "connection"} {
		t.Run(tier, func(t *testing.T) {
			driverCache := newTranslationCache(2)
			_, connectionCache, _ := newTestCache(t, 2)
			query := "RETURN $threshold"
			builds := 0
			utc := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
			for _, tc := range []struct {
				name  string
				value time.Time
				sql   string
			}{
				{"UTC", utc, "select @p0::timestamp with time zone"},
				{"local", utc.In(time.Local), "select @p0::timestamp without time zone"},
				{"zoned hit", utc.Add(time.Hour).In(time.FixedZone(time.Local.String(), 3600)), "select @p0::timestamp with time zone"},
				{"local hit", utc.Add(2 * time.Hour).In(time.Local), "select @p0::timestamp without time zone"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parameters := map[string]any{"threshold": tc.value}
					var sql string
					var bindings map[string]any
					var err error
					if tier == "driver" {
						sql, bindings, err = driverCache.GetOrBuild(driverCache.Key(query, 1, parameters), parameters, func() (string, translationCacheBuildResult, error) {
							builds++
							return tc.sql, translationCacheBuildResult{
								parameters:       map[string]any{"p0": tc.value},
								parameterSources: map[string]string{"p0": "threshold"},
							}, nil
						})
					} else {
						sql, bindings, err = connectionCache.TranslateWithPolicy(query, 1, parameters, "temporal", func() (translate.Result, string, error) {
							builds++
							return translate.Result{
								Parameters:       map[string]any{"p0": tc.value},
								ParameterSources: map[string]string{"p0": "threshold"},
							}, tc.sql, nil
						})
					}
					require.NoError(t, err)
					require.Equal(t, tc.sql, sql)
					require.Equal(t, map[string]any{"p0": tc.value}, bindings)
				})
			}
			require.Equal(t, 2, builds)
			if tier == "driver" {
				require.Equal(t, int64(2), driverCache.Stats().Hits)
			} else {
				require.Equal(t, uint64(2), connectionCache.statsSnapshot().Hits)
			}
		})
	}
}
