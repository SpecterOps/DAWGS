package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

// TestShortestPathWorkspaceFragmentUsesDedicatedTablesAndConstraints verifies isolation and key constraints for each workspace relation.
func TestShortestPathWorkspaceFragmentUsesDedicatedTablesAndConstraints(t *testing.T) {
	fragment := "insert into next_front select * from forward_front " +
		"where not exists (select 1 from forward_visited) " +
		"on conflict on constraint forward_visited_pkey do nothing"

	rewritten := shortestPathWorkspaceFragment(fragment)
	require.Equal(t,
		"insert into pg_temp.bsp_next_front select * from pg_temp.bsp_forward_front "+
			"where not exists (select 1 from pg_temp.bsp_forward_visited) "+
			"on conflict on constraint bsp_forward_visited_pkey do nothing",
		rewritten,
	)
}

func TestSelfEndpointErrorRequiresAnEligibleSeedRoot(t *testing.T) {
	rootID := pgsql.CompoundIdentifier{"e0", pgsql.ColumnStartID}
	seed := newExpansionNodeSeed("eligible_roots", "n0", nil)
	guard, err := format.Expression(shortestPathEligibleSeedSelfEndpointGuard(&seed, rootID, false), format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, guard.Statement, "case when exists (select 1 from eligible_roots where eligible_roots.root_id = e0.start_id) then case")
	require.Contains(t, guard.Statement, "shortest_path_self_endpoint_error(e0.start_id, e0.start_id) end else true end")
	unfiltered, err := format.Expression(shortestPathEligibleSeedSelfEndpointGuard(nil, rootID, false), format.NewOutputBuilder())
	require.NoError(t, err)
	require.NotContains(t, unfiltered.Statement, "eligible_roots")
	// Exercise the actual primer builder as well as the expression helper.
	builder, expansion := newShortestPathSeedTestBuilder(true, false)
	expansion.PrimerNodeConstraints = shortestPathSeedTestLocalFunctionPredicate(shortestPathSeedTestRoot, "1")
	expansion.Options.MinDepth.Set = true
	expansion.Options.MinDepth.Value = 1
	query, _, err := builder.prepareForwardFrontPrimerQuery(expansion)
	require.NoError(t, err)
	formatted, err := format.SyntaxNode(query)
	require.NoError(t, err)
	require.Contains(t, formatted.Statement, "case when exists (select 1 from s1_seed where s1_seed.root_id = e0.start_id) then case")
}

func TestSingletonShortestPathFragmentsUseCurrentEndpointArrays(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, bound := range []int{65, 128} {
			for _, projection := range []string{"p", "length(p)"} {
				for _, direction := range []string{"(s:User)-[:MemberOf*1..%d]->(e:User)", "(s:User)<-[:MemberOf*1..%d]-(e:User)"} {
					t.Run(fmt.Sprintf("%s/%d/%s/%s", mode, bound, projection, direction), func(t *testing.T) {
						query, err := frontend.ParseCypher(frontend.NewContext(), "MATCH p=shortestPath("+fmt.Sprintf(direction, bound)+") WHERE id(s)=$start_id AND id(e)=$end_id RETURN "+projection)
						require.NoError(t, err)
						translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), map[string]any{"start_id": int64(1), "end_id": int64(128)}, DefaultGraphID, Options{OptimizerMode: mode})
						require.NoError(t, err)
						formatted, err := Translated(translation)
						require.NoError(t, err)
						require.Contains(t, formatted.Statement, "bidirectional_sp_harness(")
						fragments := 0
						for _, value := range translation.Parameters {
							fragment, ok := value.(string)
							if !ok || !strings.HasPrefix(fragment, "insert into pg_temp.bsp_next_front") {
								continue
							}
							fragments++
							require.NotContains(t, fragment, "traversal_terminal_filter")
							require.NotContains(t, fragment, "traversal_root_filter")
							require.NotContains(t, fragment, "traversal_pair_filter")
							require.Regexp(t, `any\s*\(\$[12]\)`, fragment)
						}
						require.Equal(t, 4, fragments)
					})
				}
			}
		}
	}
}

func TestArrayShortestPathFragmentRejectsFilterRelations(t *testing.T) {
	for _, name := range []pgsql.Identifier{expansionRootFilter, expansionTerminalFilter, expansionPairFilter, "bsp_root_filter", "bsp_terminal_filter", "bsp_pair_filter"} {
		query := pgsql.Select{From: []pgsql.FromClause{{Source: pgsql.TableReference{Name: name.AsCompoundIdentifier()}}}}
		require.ErrorContains(t, validateArrayShortestPathFragment(query), "array shortest-path fragment references filter table")
		// The parameter builder must enforce the invariant before emitting SQL.
		builder := ExpansionBuilder{}
		_, err := builder.bidirectionalShortestPathsParameters(&Expansion{
			SingletonRootID: pgsql.NewLiteral(1, pgsql.Int8), SingletonTerminalID: pgsql.NewLiteral(2, pgsql.Int8),
		}, query, query, query, query, true)
		require.ErrorContains(t, err, "array shortest-path fragment references filter table")
	}
	// A user property value with the same spelling is not a relation dependency.
	require.NoError(t, validateArrayShortestPathFragment(pgsql.Select{Projection: []pgsql.SelectItem{pgsql.NewLiteral("traversal_terminal_filter", pgsql.Text)}}))
}
