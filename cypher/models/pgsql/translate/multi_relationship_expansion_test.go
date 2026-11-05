package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Every hop must retain the type disjunction, including after frame binding,
// traversal reversal, and projection pruning for mutations.
func TestMultiRelationshipExpansionFiltersEveryHop(t *testing.T) {
	for _, kinds := range [][2]string{{"Okta_MemberOf", "Okta_HasRoleAssignment"}, {"MemberOf", "AdminTo"}} {
		mapper := pgutil.NewInMemoryKindMapper()
		for _, kind := range kinds {
			mapper.Put(graph.StringKind(kind))
		}
		for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
			for _, depth := range []int{2, 3} {
				for _, pattern := range []string{
					fmt.Sprintf("(s)-[:%s|%s*1..%d]->(t)", kinds[0], kinds[1], depth),
					fmt.Sprintf("(s)<-[:%s|%s*1..%d]-(t)", kinds[0], kinds[1], depth),
				} {
					for _, source := range []string{
						"MATCH p = " + pattern + " RETURN p",
						"MATCH p = " + pattern + " WHERE s.name = 'root' RETURN p",
						"MATCH p = " + pattern + " WHERE t.name = 'target' RETURN p",
						"MATCH (s) WHERE s.name = 'root' WITH s MATCH p = " + pattern + " RETURN p",
						"MATCH " + pattern + " WHERE s.name = 'root' SET t.selected = true RETURN t",
						"MATCH " + pattern + " WHERE s.name = 'root' SET t.selected = true",
					} {
						t.Run(string(mode)+"/"+source, func(t *testing.T) {
							parsed, err := frontend.ParseCypher(frontend.NewContext(), source)
							require.NoError(t, err)
							translation, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, DefaultGraphID, Options{OptimizerMode: mode})
							require.NoError(t, err)
							sql, err := Translated(translation)
							require.NoError(t, err)
							require.Equal(t, 2, strings.Count(sql.Statement, "e0.kind_id = any (array [1, 2]::int2[])"), sql.Statement)
							require.Contains(t, sql.Statement, fmt.Sprintf(".depth < %d", depth))
							require.Regexp(t, `(?s)join lateral \(select e0\..*?where e0\.(start_id|end_id) = s\d+\.next_id and e0\.id != all \(s\d+\.path\) and e0\.kind_id = any \(array \[1, 2\]::int2\[\]\) offset 0\)`, sql.Statement)
						})
					}
				}
			}
		}
	}
}
