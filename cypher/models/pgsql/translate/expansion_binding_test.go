package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

// TestExpansionStepPreservesBoundTerminalIdentity checks the emitted endpoint
// equality independently of terminal predicates, traversal depth, and optimizer mode.
func TestExpansionStepPreservesBoundTerminalIdentity(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, testCase := range []struct {
				name  string
				query string
			}{
				{"outbound", `MATCH (a)-[:MemberOf]->(mid)-[:AdminTo*1..2]->(a) WHERE a.name = 'shared' RETURN a`},
				{"inbound", `MATCH (a)<-[:MemberOf]-(mid)<-[:AdminTo*1..2]-(a) WHERE a.name = 'shared' RETURN a`},
				{"undirected_prefix", `MATCH (a)-[:MemberOf]-(mid)-[:AdminTo*1..2]->(a) WHERE a.name = 'shared' RETURN a`},
				{"zero_depth", `MATCH (a)-[:MemberOf]->(mid)-[:AdminTo*0..2]->(a) WHERE a.name = 'shared' RETURN a`},
				{"recursive_depth", `MATCH (a)-[:MemberOf]->(mid)-[:AdminTo*2..3]->(a) WHERE a.name = 'shared' RETURN a`},
				{"path", `MATCH p = (a)-[:MemberOf]->(mid)-[:AdminTo*1..2]->(a) WHERE a.name = 'shared' RETURN a, p`},
				{"across_with", `MATCH (a) WHERE a.name = 'shared' WITH a MATCH (a)-[:MemberOf]->(mid)-[:AdminTo*1..2]->(a) RETURN a`},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					query, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
					require.NoError(t, err)
					translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					formatted, err := Translated(translation)
					require.NoError(t, err)
					// The prefix materializes a as n0. Every completed expansion
					// must match that identity, even after a.name was consumed.
					require.Regexp(t, `\(s\d+\.n0\)\.id = s\d+\.next_id`, formatted.Statement)
					require.NotContains(t, formatted.Statement, "_endpoint_seeded_")
					if testCase.name == "path" {
						require.NotContains(t, formatted.Statement, "n1.id as n1")
					}
				})
			}
		})
	}
}

// TestSelfLoopExpansionAfterPrefixHydratesSharedEndpointOnce prevents duplicate
// node aliases when an expansion reuses its own root after a fixed prefix.
func TestSelfLoopExpansionAfterPrefixHydratesSharedEndpointOnce(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH p = (a)-[:MemberOf]->(mid)-[:AdminTo*0..2]->(mid) WHERE a.name = 'shared' RETURN mid, p`)
			require.NoError(t, err)
			translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
			require.NoError(t, err)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Regexp(t, `s\d+\.root_id = s\d+\.next_id`, formatted.Statement)
			require.Regexp(t, `\(s\d+\.n1\)\.id = s\d+\.next_id`, formatted.Statement)
			require.Equal(t, 1, strings.Count(formatted.Statement, "from node n1"), formatted.Statement)
		})
	}
}
