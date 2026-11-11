package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestExpansionEndpointPredicatesSurviveFixedContinuation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, pattern := range []string{
				`(s)-[:MemberOf*1..2]->(g)-[:AdminTo]->(d)`,
				`(s)-[:MemberOf*0..2]->(g)-[:AdminTo]->(d)`,
				`(s)-[:MemberOf*1..2]->(g)-[:AdminTo]->(d)-[:AdminTo]->(e)`,
				`(g)<-[:MemberOf*1..2]-(s)-[:AdminTo]->(d)`,
				`(s)-[:MemberOf*1..2]->(g)-[:AdminTo]-(d)`,
				`(prefix)-[:AdminTo]->(s)-[:MemberOf*1..2]->(g)-[:AdminTo]->(d)`,
			} {
				t.Run(pattern, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH `+pattern+` WHERE s.endpoint_filter = 123 OR g.endpoint_filter = 123 RETURN count(*) AS c`)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					// Each branch must survive exactly once, with the root and
					// terminal read from a materialized frame outside recursion.
					require.Equal(t, 2, strings.Count(formatted.Statement, "E'endpoint_filter'"), formatted.Statement)
					require.Equal(t, 2, strings.Count(formatted.Statement, "123"), formatted.Statement)
					require.Regexp(t, `\(s\d+\.n\d+\)\.properties ->>? E'endpoint_filter'`, formatted.Statement)
				})
			}
		})
	}
}
