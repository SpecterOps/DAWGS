package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestMultipartTraversalReadsInputBoundary(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf]->(c) WITH c RETURN c`,
			`MATCH (a:User) WITH a MATCH (a)<-[:MemberOf]-(c) WITH c RETURN c`,
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf]-(c) WITH c RETURN c`,
			`MATCH (a:User), (c:Computer) WITH a, c MATCH (a)-[:MemberOf]->(c) WITH c RETURN c`,
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf*1..3]->(c) WITH c RETURN c`,
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf]->(c) WITH c SET c.seen = true RETURN c`,
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				formatted, err := Translated(translation)
				require.NoError(t, err)
				boundaries := translation.Statement.(pgsql.Query).CommonTableExpressions.Expressions
				require.GreaterOrEqual(t, len(boundaries), 2)
				boundary := boundaries[1]
				if len(boundary.Query.CommonTableExpressions.Expressions) == 0 {
					t.Fatal("expected traversal CTE inside second WITH boundary")
				}
				step := boundary.Query.CommonTableExpressions.Expressions[0].Query.Body.(pgsql.Select)
				require.NotEmpty(t, step.From)
				require.Equal(t, pgsql.CompoundIdentifier{boundaries[0].Alias.Name}, step.From[0].Source.(pgsql.TableReference).Name, formatted.Statement)
			})
		}
	}
}

func TestMultipartLiteralReadBoundaryPreservesInput(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, query := range []string{
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf]->(c) WITH 1 AS value RETURN value`,
			`MATCH (a:User) WITH a MATCH (a)-[:MemberOf]->(c) WITH null AS value RETURN value`,
			`MATCH (a:User) WITH a WITH 1 AS value RETURN value`,
			`MATCH (a:User) WITH a MATCH (c:Computer) WITH 1 AS value RETURN value`,
		} {
			t.Run(string(mode)+"/"+query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				boundaries := translation.Statement.(pgsql.Query).CommonTableExpressions.Expressions
				boundary := boundaries[1]
				projection := boundary.Query.Body.(pgsql.Select)
				require.Len(t, projection.From, 1, "a literal WITH consumes every incoming row, including zero rows")
				input := boundaries[0].Alias.Name
				if boundary.Query.CommonTableExpressions != nil {
					ctes := boundary.Query.CommonTableExpressions.Expressions
					input = ctes[len(ctes)-1].Alias.Name
				}
				require.Equal(t, input, projection.From[0].Source)
			})
		}
	}
}

func TestMultipartExpansionRetainsIndependentInput(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			query      string
			correlated bool
		}{
			{query: `UNWIND [1, 2] AS x WITH x MATCH (a:User)-[:MemberOf*1..2]->(c) WITH x, c RETURN x, c`},
			{query: `WITH 1 AS x UNWIND ['a', 'b'] AS value MATCH (a:User)-[:MemberOf*1..2]->(c) WHERE a.name = value WITH x, c RETURN x, c`},
			{query: `WITH 1 AS x MATCH p = shortestPath((a:User)-[:MemberOf*1..2]->(c)) WITH x, p RETURN x, p`},
			{query: `UNWIND ['a', 'b'] AS name WITH name MATCH (a:User)-[:MemberOf*1..2]->(c) WHERE a.name = name WITH name, c RETURN name, c`, correlated: true},
		} {
			t.Run(string(mode)+"/"+tc.query, func(t *testing.T) {
				parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				boundaries := translation.Statement.(pgsql.Query).CommonTableExpressions.Expressions
				boundary := boundaries[1]
				formatted, err := format.Statement(boundary.Query, format.NewOutputBuilder())
				require.NoError(t, err)
				require.NotContains(t, formatted.Statement, "from "+boundary.Alias.Name.String()+" ")
				require.Contains(t, formatted.Statement, boundaries[0].Alias.Name.String()+".i0", "every expansion row must retain the independent input")
				if tc.correlated {
					projection := boundary.Query.CommonTableExpressions.Expressions[0].Query.Body.(pgsql.Select)
					require.NotNil(t, projection.Where, "seed filtering must also correlate completed paths with each input row")
					references, err := ExtractSyntaxNodeReferences(projection.Where)
					require.NoError(t, err)
					require.Contains(t, references.Slice(), boundaries[0].Alias.Name)
					require.Contains(t, formatted.Statement, "= "+boundaries[0].Alias.Name.String()+".i0", "the carried scalar must constrain the expansion primer")
				}
			})
		}
	}
}
