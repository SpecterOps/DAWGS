package translate

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestExactExpansionLengthMatchesExplicitRange(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, depth := range []int{0, 1, 2, 15, 16, 32} {
				t.Run(fmt.Sprint(depth), func(t *testing.T) {
					for _, shape := range []string{
						`MATCH p = (a:Group)-[:MemberOf*%s]->(b) RETURN p`,
						`MATCH p = (a:Group)<-[:MemberOf*%s]-(b) RETURN p`,
						`MATCH (a:Group)-[:MemberOf]->(b)-[:MemberOf*%s]->(c) RETURN c`,
						`MATCH (a:Group)-[:MemberOf*%s]->(b)-[:MemberOf]->(c) SET c.selected = true RETURN c`,
						`MATCH p = shortestPath((a:Group)-[:MemberOf*%s]->(b)) RETURN p`,
						`MATCH p = allShortestPaths((a:Group)-[:MemberOf*%s]->(b)) RETURN p`,
					} {
						var statements []string
						var parameters []map[string]any
						for _, bound := range []string{fmt.Sprint(depth), fmt.Sprintf("%d..%d", depth, depth)} {
							source := fmt.Sprintf(shape, bound)
							parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
							require.NoError(t, err)
							translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
							require.NoError(t, err, source)
							formatted, err := Translated(translation)
							require.NoError(t, err, source)
							statements = append(statements, formatted.Statement)
							parameters = append(parameters, translation.Parameters)
						}
						require.Equal(t, statements[1], statements[0], shape)
						require.Equal(t, parameters[1], parameters[0], shape)
					}
				})
			}
		})
	}
}
