package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestMergeMaterializedStringValues(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MERGE (n:NodeKind1 {name:'a'}) RETURN n`,
			`MERGE (n:NodeKind1 {name:$name}) RETURN n`,
			`MERGE (n:NodeKind1) ON CREATE SET n.name='a' RETURN n`,
			`MERGE (n:NodeKind1) ON MATCH SET n.name=$name RETURN n`,
		} {
			t.Run(string(mode)+query, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				mapper.Put(graph.StringKind("NodeKind1"))
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, map[string]any{"name": "a"}, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				materialized, err := format.Statement(result.Statement, format.NewOutputBuilder().WithMaterializedParameters(result.Parameters))
				require.NoError(t, err)
				require.Contains(t, materialized.Statement, `to_jsonb((E'a')::text)`)
				require.NotContains(t, materialized.Statement, `to_jsonb(E'a')`)
				require.Empty(t, materialized.Parameters)
				parameterized, err := Translated(result)
				require.NoError(t, err)
				require.Contains(t, parameterized.Statement, "::text)::jsonb")
			})
		}
	}
}

func TestMergeTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MERGE (n:NodeKind1 {name:'new'}) RETURN n`,
			`MERGE (n:NodeKind1 {name:$name}) ON CREATE SET n.score=1 ON MATCH SET n.score=2 SET n.done=true RETURN n`,
			`MATCH (a:NodeKind1), (b:NodeKind2) MERGE (a)-[r:EdgeKind1]->(b) RETURN r`,
			`MERGE p=(a:NodeKind1 {name:'a'})-[r:EdgeKind1]->(b:NodeKind2 {name:'b'}) RETURN p`,
			`MERGE (a:NodeKind1)-[:EdgeKind1]-(b:NodeKind2) RETURN a,b`,
			`MERGE (a:NodeKind1)-[:EdgeKind1]->(b:NodeKind2)-[:EdgeKind2]->(c:NodeKind1) RETURN a,c`,
			`UNWIND ['a','b'] AS name MERGE (n:NodeKind1 {name:name}) RETURN n`,
			`MERGE (n:NodeKind1) ON CREATE SET n:NodeKind2`,
			`MERGE (n:NodeKind1) RETURN n LIMIT 0`,
			`MERGE (n:NodeKind1) WITH n RETURN n`,
			`MATCH (s:NodeKind1) WITH s MERGE (n:NodeKind2 {name:s.name}) RETURN n`,
			`MERGE (n:NodeKind1) RETURN count(n)`, `MERGE (n:NodeKind1) RETURN count(*)`, `MERGE (n:NodeKind1) RETURN 1`,
			`MERGE (n {name:'unlabeled'}) RETURN n`,
			`MATCH (a:NodeKind1),(b:NodeKind2) MERGE (a)<-[r:EdgeKind1]-(b) ON CREATE SET a.flag=true RETURN r`,
		} {
			t.Run(string(mode)+query, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, kind := range []string{"NodeKind1", "NodeKind2", "EdgeKind1", "EdgeKind2"} {
					mapper.Put(graph.StringKind(kind))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, map[string]any{"name": "a"}, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				_, ok := result.Statement.(pgsql.Query)
				require.True(t, ok)
				sql, err := Translated(result)
				require.NoError(t, err)
				require.Contains(t, sql.Statement, "merge into")
				require.Contains(t, sql.Statement, "then do nothing")
				require.Contains(t, sql.Statement, "union all")
				require.Contains(t, sql.Statement, "returning")
				require.NotContains(t, sql.Statement, "cypher_execute")
				require.NotContains(t, sql.Statement, "update set id")
				if strings.Contains(query, "ON MATCH") {
					require.Contains(t, sql.Statement, "then update set")
				}
			})
		}
	}
}

func TestMergeLargePropertyPatches(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, count := range []int{50, 51, 100, 101} {
			for _, action := range []string{"SET", "ON CREATE SET", "ON MATCH SET"} {
				t.Run(fmt.Sprintf("%s/%s/%d", mode, action, count), func(t *testing.T) {
					assignments := make([]string, count)
					for idx := range assignments {
						assignments[idx] = fmt.Sprintf("n.field%d=n.seed", idx)
					}
					query := `MERGE (n:NodeKind1 {name:'a'}) ` + action + " " + strings.Join(assignments, ",") + " RETURN n"
					mapper := pgutil.NewInMemoryKindMapper()
					mapper.Put(graph.StringKind("NodeKind1"))
					parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
					require.NoError(t, err)
					result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, 42, Options{OptimizerMode: mode})
					require.NoError(t, err)
					fields := map[string]int{}
					patchSizes := []int{}
					require.NoError(t, walk.PgSQL(result.Statement, walk.NewSimpleVisitor[pgsql.SyntaxNode](func(node pgsql.SyntaxNode, _ walk.VisitorHandler) {
						if call, ok := node.(pgsql.FunctionCall); ok && call.Function == pgsql.FunctionJSONBBuildObject {
							require.LessOrEqual(t, len(call.Parameters), 100, "PostgreSQL rejects calls with more than 100 arguments")
							isPatch := false
							for idx := 0; idx < len(call.Parameters); idx += 2 {
								key, ok := call.Parameters[idx].(pgsql.Literal)
								require.True(t, ok)
								if field, ok := key.Value.(string); ok && strings.HasPrefix(field, "field") {
									fields[field]++
									isPatch = true
								}
							}
							if isPatch {
								patchSizes = append(patchSizes, len(call.Parameters)/2)
							}
						}
					})))
					require.Len(t, fields, count)
					for _, occurrences := range fields {
						require.Equal(t, 1, occurrences, "each effective assignment appears once")
					}
					require.Len(t, patchSizes, (count+49)/50)
					sql, err := Translated(result)
					require.NoError(t, err)
					require.Equal(t, 1, strings.Count(sql.Statement, "cypher_apply_property_patch("), "apply the complete patch once")
				})
			}
		}
	}
}

func TestInvalidMergePatterns(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, test := range []struct {
			name, query, message string
		}{
			{"missing relationship type", `MERGE ()-[]->() RETURN 1`, "MERGE relationships require exactly one type and no range"},
			{"relationship range", `MERGE ()-[:EdgeKind1*1..2]->() RETURN 1`, "MERGE relationships require exactly one type and no range"},
			{"scalar binding", `WITH 1 AS n MERGE (n) RETURN n`, "invalid MERGE binding n: expected nodecomposite"},
			{"repeated node labels", `MERGE (a:NodeKind1)-[:EdgeKind1]->(a:NodeKind2) RETURN a`, "MERGE cannot redeclare labels or properties on a declared node a"},
			{"repeated node properties", `MERGE (a:NodeKind1)-[:EdgeKind1]->(a {score:1}) RETURN a`, "MERGE cannot redeclare labels or properties on a declared node a"},
			{"bound node labels", `MATCH (n:NodeKind1) MERGE (n:NodeKind2) RETURN n`, "MERGE cannot redeclare labels or properties on a declared node n"},
			{"unbound property reference", `MERGE (n:NodeKind1 {name:n.name}) RETURN n`, "MERGE properties reference an unbound entity n"},
			{"bound relationship", `MATCH ()-[r:EdgeKind1]->() MERGE ()-[r:EdgeKind1]->() RETURN r`, "MERGE cannot redeclare a bound relationship"},
		} {
			t.Run(string(mode)+"/"+test.name, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, kind := range []string{"NodeKind1", "NodeKind2", "EdgeKind1"} {
					mapper.Put(graph.StringKind(kind))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), test.query)
				require.NoError(t, err)
				_, err = TranslateWithOptions(context.Background(), parsed, mapper, nil, 0, Options{OptimizerMode: mode})
				require.ErrorContains(t, err, test.message)
			})
		}
	}
}

func TestMergeKindMapping(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, test := range []struct {
			name, query, message string
		}{
			{"register merge kind", `MERGE (n:NodeKind1) RETURN n`, ""},
			{"reject unknown match kind", `MATCH (n:NodeKind1) MERGE (n) RETURN n`, "failed to translate kinds: missing kinds: [NodeKind1]"},
		} {
			t.Run(string(mode)+"/"+test.name, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				parsed, err := frontend.ParseCypher(frontend.NewContext(), test.query)
				require.NoError(t, err)
				_, err = TranslateWithOptions(context.Background(), parsed, mapper, nil, 0, Options{OptimizerMode: mode})
				if test.message != "" {
					require.ErrorContains(t, err, test.message)
				} else {
					require.NoError(t, err)
					_, err = mapper.MapKind(context.Background(), graph.StringKind("NodeKind1"))
					require.NoError(t, err, "MERGE must register its kind before matching")
				}
			})
		}
	}
}

func TestMergeCopiedPropertyTypes(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		mapper := pgutil.NewInMemoryKindMapper()
		for _, kind := range []string{"NodeKind1", "NodeKind2", "EdgeKind1"} {
			mapper.Put(graph.StringKind(kind))
		}
		parsed, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (a:NodeKind1) MERGE (n:NodeKind2 {score:a.score, active:a.active, values:a.values}) RETURN n`)
		require.NoError(t, err)
		result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, 42, Options{OptimizerMode: mode})
		require.NoError(t, err)
		sql, err := Translated(result)
		require.NoError(t, err)
		for _, field := range []string{"score", "active", "values"} {
			require.Contains(t, sql.Statement, "properties -> E'"+field+"'")
			require.NotContains(t, sql.Statement, "properties ->> E'"+field+"'")
		}
	}
}

func TestMergeCarriesLazyPath(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, prefix := range []string{
			`MATCH p=(a:NodeKind1)-[:EdgeKind1]->(b:NodeKind2)`,
			`MERGE p=(a:NodeKind1)-[:EdgeKind1]->(b:NodeKind2)`,
		} {
			t.Run(string(mode)+prefix, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				for _, kind := range []string{"NodeKind1", "NodeKind2", "EdgeKind1"} {
					mapper.Put(graph.StringKind(kind))
				}
				parsed, err := frontend.ParseCypher(frontend.NewContext(), prefix+` MERGE (c:NodeKind1 {name:'independent'}) RETURN p`)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				query := result.Statement.(pgsql.Query)
				found := false
				for _, cte := range query.CommonTableExpressions.Expressions {
					selectQuery, ok := cte.Query.Body.(pgsql.Select)
					if !ok {
						continue
					}
					for _, item := range selectQuery.Projection {
						alias, ok := item.(*pgsql.AliasedExpression)
						if !ok || alias.Alias.Value != "pc0" || found {
							continue
						}
						_, columnReference := alias.Expression.(pgsql.CompoundIdentifier)
						require.False(t, columnReference, "the first path projection must build its value from dependencies")
						found = true
					}
				}
				require.True(t, found, "the incoming path must be projected into the MERGE pipeline")
			})
		}
	}
}

func TestMergeWritesDependOnCandidateValidation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		mapper := pgutil.NewInMemoryKindMapper()
		mapper.Put(graph.StringKind("NodeKind1"))
		mapper.Put(graph.StringKind("EdgeKind1"))
		parsed, err := frontend.ParseCypher(frontend.NewContext(), `MERGE (a:NodeKind1)-[:EdgeKind1]->(b:NodeKind1) ON MATCH SET a.score=1,b.score=2 RETURN a,b LIMIT 0`)
		require.NoError(t, err)
		result, err := TranslateWithOptions(context.Background(), parsed, mapper, nil, 42, Options{OptimizerMode: mode})
		require.NoError(t, err)
		query := result.Statement.(pgsql.Query)
		var guardID pgsql.Identifier
		guardedSources := pgsql.NewIdentifierSet()
		writes := 0
		for _, cte := range query.CommonTableExpressions.Expressions {
			if selectQuery, ok := cte.Query.Body.(pgsql.Select); ok {
				if len(selectQuery.Projection) == 1 {
					if alias, ok := selectQuery.Projection[0].(*pgsql.AliasedExpression); ok {
						if call, ok := alias.Expression.(pgsql.FunctionCall); ok && call.Function == "cypher_merge_assert" {
							require.NotNil(t, cte.Materialized)
							require.True(t, cte.Materialized.Materialized)
							guardID = cte.Alias.Name
						}
					}
				}
				for _, from := range selectQuery.From {
					if table, ok := from.Source.(pgsql.TableReference); ok && guardID != "" && table.Name[0] == guardID {
						require.Equal(t, pgsql.CompoundIdentifier{guardID, "_merge_valid"}, selectQuery.Where)
						guardedSources.Add(cte.Alias.Name)
					}
				}
			}
			if merge, ok := cte.Query.Body.(pgsql.Merge); ok {
				if merge.Source.Name[0] == guardID {
					continue
				}
				require.True(t, guardedSources.Contains(merge.Source.Name[0]), "each native write must read validated candidates")
				writes++
			}
		}
		require.NotEmpty(t, guardID)
		require.Equal(t, 3, writes)
	}
}

func TestMergeRelationalPipeline(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, test := range []struct {
			query                   string
			writes, values, patches int
			grouped, dynamic        bool
		}{
			{`MERGE (n:NodeKind1 {name:$name,score:1}) RETURN n`, 1, 2, 0, true, false},
			{`MATCH (a:NodeKind1) MERGE (n:NodeKind1 {name:a.name}) RETURN n`, 1, 1, 0, true, false},
			{`MERGE (n:NodeKind1 $props) RETURN n`, 1, 0, 0, false, true},
			{`UNWIND ['a','b'] AS name MERGE (n:NodeKind1 {name:name}) ON CREATE SET n.name='b',n.x=1,n.y=2 RETURN n`, 1, 1, 1, false, false},
			{`MATCH (a:NodeKind1),(b:NodeKind1) MERGE (a)-[r:EdgeKind1]->(b) RETURN r`, 1, 0, 0, false, false},
			{`MATCH (a:NodeKind1) MERGE (a) RETURN a`, 0, 0, 0, false, false},
			{`MATCH (a:NodeKind1) UNWIND [1,2] AS input MERGE (a) RETURN *`, 0, 0, 0, false, false},
			{`MERGE (n:NodeKind1 {name:'a'}) SET n.name='b' RETURN *`, 1, 1, 1, false, false},
			{`MERGE (n:NodeKind1 {name:'a'}) SET n.x=1 SET n.y=n.x RETURN n`, 1, 1, 2, true, false},
		} {
			t.Run(string(mode)+test.query, func(t *testing.T) {
				mapper := pgutil.NewInMemoryKindMapper()
				mapper.Put(graph.StringKind("NodeKind1"))
				mapper.Put(graph.StringKind("EdgeKind1"))
				parsed, err := frontend.ParseCypher(frontend.NewContext(), test.query)
				require.NoError(t, err)
				result, err := TranslateWithOptions(context.Background(), parsed, mapper, map[string]any{"name": "a", "props": map[string]any{"name": "a"}}, 42, Options{OptimizerMode: mode})
				require.NoError(t, err)
				sql, err := Translated(result)
				require.NoError(t, err)
				require.NotContains(t, sql.Statement, "cypher_merge_candidates")
				require.NotContains(t, sql.Statement, "jsonb_agg")
				require.NotContains(t, sql.Statement, "cypher_set_property")
				require.Equal(t, test.values, strings.Count(sql.Statement, "cypher_merge_value("))
				require.Equal(t, test.patches, strings.Count(sql.Statement, "cypher_apply_property_patch("))
				expectedWrites := test.writes
				if test.writes == 0 {
					expectedWrites++
				}
				require.Equal(t, expectedWrites, strings.Count(sql.Statement, "merge into"))
				if test.grouped && (strings.HasPrefix(test.query, "UNWIND") || strings.HasPrefix(test.query, "MATCH")) {
					require.Contains(t, sql.Statement, "group by")
					require.Contains(t, sql.Statement, "having count(distinct")
				}
				if test.dynamic {
					require.Equal(t, 1, strings.Count(sql.Statement, "cypher_merge_properties("))
					require.Contains(t, sql.Statement, "jsonb_each")
				}
				query := result.Statement.(pgsql.Query)
				if strings.HasSuffix(test.query, "RETURN *") {
					output := query.Body.(pgsql.Select)
					for _, item := range output.Projection {
						if alias, ok := item.(*pgsql.AliasedExpression); ok {
							require.Contains(t, []pgsql.Identifier{"n", "a", "input"}, alias.Alias.Value)
						}
					}
				}
				input := query.CommonTableExpressions.Expressions[0].Query.Body.(pgsql.Select)
				if !strings.HasPrefix(test.query, "MATCH") {
					require.Nil(t, input.Where, "validated input expressions are not duplicated in a predicate")
				}
			})
		}
	}
}
