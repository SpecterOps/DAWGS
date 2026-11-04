package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestCarriedNodeRepresentation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, hops := range []string{"", "*1..2"} {
			t.Run(string(mode)+"/"+hops, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (a) WITH a MATCH (a)-[:MemberOf`+hops+`]->(b) RETURN id(a), id(b)`)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				require.Regexp(t, `\(s\d+\.n0\)\.id as "id\(a\)"`, formatted.Statement)
				if hops != "" {
					require.Regexp(t, `select distinct \(s\d+\.n0\)\.id as root_id`, formatted.Statement)
				}
			})
		}
	}
}
