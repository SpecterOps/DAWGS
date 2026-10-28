package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestMultipartMutationsPreserveStatementBoundaries(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, tc := range []struct {
			name   string
			query  string
			stages int
		}{
			{name: "leading optional update and create", query: `OPTIONAL MATCH (n:User) SET n.x = 1 WITH n CREATE (c:User {source: coalesce(n.name, 'missing')}) RETURN c`, stages: 2},
			{name: "leading optional create without return", query: `OPTIONAL MATCH (n:User) SET n.x = 1 WITH n CREATE (c:User {source: coalesce(n.name, 'missing')})`, stages: 2},
			{name: "leading create", query: `CREATE (n:User {name: 'new'}) WITH n MATCH (m:User) RETURN count(m)`, stages: 2},
			{name: "read prefix and create", query: `UNWIND [1, 2] AS x WITH x CREATE (n:User {x: x}) WITH n MATCH (m:User) RETURN count(m)`, stages: 2},
			{name: "read part after mutation", query: `CREATE (n:User) WITH n WITH n RETURN n`, stages: 3},
			{name: "multiple mutations", query: `MATCH (n:User) WITH n SET n.x = 1 WITH n SET n.y = 2 WITH n RETURN n`, stages: 3},
			{name: "deletion", query: `MATCH (n:User) DELETE n WITH 1 AS x MATCH (m:User) RETURN count(m)`, stages: 2},
		} {
			t.Run(string(mode)+"/"+tc.name, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				require.Len(t, translation.Stages, tc.stages)
				require.Nil(t, translation.Statement, "staged queries must not expose a partial single-statement translation")
				_, err = Translated(translation)
				require.ErrorContains(t, err, "TranslatedStatements")
				formatted, err := TranslatedStatements(translation, "multipart_test_")
				require.NoError(t, err)
				require.Len(t, formatted, tc.stages)
				for index, stage := range translation.Stages {
					if index == len(translation.Stages)-1 {
						require.Empty(t, stage.Binding)
						require.NotContains(t, formatted[index].Statement, "create temporary table")
					} else {
						require.NotEmpty(t, stage.Binding)
						require.Contains(t, formatted[index].Statement, `create temporary table pg_temp."multipart_test_`+stage.Binding.String()+`" on commit drop as `)
					}
					for _, previous := range translation.Stages[:index] {
						require.Contains(t, formatted[index].Statement, `select * from pg_temp."multipart_test_`+previous.Binding.String()+`"`)
					}
				}
			})
		}
	}
}

func TestMultipartReadPrefixStaysWithFirstMutation(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), `UNWIND [1, 2] AS x WITH x CREATE (n:User {x: x}) WITH n SET n.name = 'done' WITH n RETURN n`)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)
	formatted, err := TranslatedStatements(translation, "prefix_")
	require.NoError(t, err)
	require.Len(t, formatted, 3)
	require.Contains(t, formatted[0].Statement, "unnest(")
	require.Contains(t, formatted[0].Statement, "insert into node")
	require.NotContains(t, formatted[0].Statement, "update node")
	require.Contains(t, formatted[1].Statement, "update node")
	require.NotContains(t, formatted[1].Statement, "insert into node")
	require.NotContains(t, formatted[2].Statement, "update node")
	_, err = FromCypher(context.Background(), query, optimizerSafetyKindMapper(), false, DefaultGraphID)
	require.ErrorContains(t, err, "TranslatedStatements")
}

func TestMultipartWithoutNonfinalMutationRemainsOneStatement(t *testing.T) {
	for _, queryText := range []string{
		`MATCH (n:User) WITH n RETURN n`,
		`MATCH (n:User) WITH n SET n.x = 1 RETURN n`,
		`CREATE (n:User) RETURN n`,
	} {
		t.Run(queryText, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
			require.NoError(t, err)
			translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)
			require.Empty(t, translation.Stages)
			formatted, err := Translated(translation)
			require.NoError(t, err)
			statements, err := TranslatedStatements(translation, "")
			require.NoError(t, err)
			require.Len(t, statements, 1)
			require.Equal(t, formatted, statements[0])
		})
	}
}

func TestMultipartWithModifiersStayOnTheirBoundary(t *testing.T) {
	for _, queryText := range []string{
		`UNWIND [1, 2, 3] AS x CREATE (n:User {x: x}) WITH n ORDER BY n.x DESC SKIP 1 LIMIT 1 SET n.seen = true WITH n RETURN n.x`,
		`UNWIND [1, 2, 3] AS x CREATE (n:User {x: x}) WITH n, n.x AS value ORDER BY value DESC SKIP 1 LIMIT 1 SET n.seen = true WITH n RETURN n.x`,
	} {
		t.Run(queryText, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
			require.NoError(t, err)
			translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)
			require.Len(t, translation.Stages, 3)
			require.Len(t, translation.Stages[0].Query.OrderBy, 1)
			require.NotNil(t, translation.Stages[0].Query.Offset)
			require.NotNil(t, translation.Stages[0].Query.Limit)
			statements, err := TranslatedStatements(translation, "ordered_")
			require.NoError(t, err)
			require.Contains(t, statements[0].Statement, " desc")
			require.Contains(t, statements[0].Statement, "offset 1")
			require.Contains(t, statements[0].Statement, "limit 1")
			require.NotContains(t, statements[1].Statement, "limit 1")
			require.NotContains(t, statements[2].Statement, "limit 1")
		})
	}
}

func TestTranslatedStatementsKeepStageLiteralsIsolated(t *testing.T) {
	translation := Result{Stages: []QueryStage{
		{Binding: "s0", Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral("first", pgsql.Text)}}}},
		{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral("last", pgsql.Text)}}}},
	}}
	formatted, err := TranslatedStatements(translation, "literal_")
	require.NoError(t, err)
	require.Len(t, formatted, 2)
	require.Equal(t, map[string]any{"__strlit0": "first"}, formatted[0].Parameters)
	require.Equal(t, map[string]any{"__strlit0": "last"}, formatted[1].Parameters)
	formatted[0].Parameters["__strlit0"] = "changed"
	require.Equal(t, "last", formatted[1].Parameters["__strlit0"])
	repeated, err := TranslatedStatements(translation, "second_")
	require.NoError(t, err)
	require.Contains(t, repeated[1].Statement, `pg_temp."second_s0"`)
	require.NotContains(t, repeated[1].Statement, "literal_s0")
	require.Nil(t, translation.Stages[1].Query.CommonTableExpressions)
}

func TestMultipartReadPrefixPreservesModifiers(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (seed:User) WITH seed ORDER BY seed.name DESC SKIP 1 LIMIT 1 CREATE (n:User {name: seed.name}) WITH n RETURN n`)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)
	require.Len(t, translation.Stages, 2)
	prefix := translation.Stages[0].Query.CommonTableExpressions.Expressions[0].Query
	require.Len(t, prefix.OrderBy, 1)
	require.NotNil(t, prefix.Offset)
	require.NotNil(t, prefix.Limit)
	require.Nil(t, translation.Stages[0].Query.Limit)
	statements, err := TranslatedStatements(translation, "prefix_order_")
	require.NoError(t, err)
	require.Contains(t, statements[0].Statement, " desc")
	require.Contains(t, statements[0].Statement, "offset 1")
	require.Equal(t, 1, strings.Count(statements[0].Statement, "limit 1"))
}

func TestMultipartOrderByResolvesEntityAliasesAgainstInput(t *testing.T) {
	query, err := frontend.ParseCypher(frontend.NewContext(), `UNWIND [1, 2, 3] AS x CREATE (n:User {x: x}) WITH n AS item ORDER BY item.x DESC LIMIT 1 SET item.seen = true WITH item RETURN item.x`)
	require.NoError(t, err)
	translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
	require.NoError(t, err)
	require.Len(t, translation.Stages, 3)
	ordering, err := format.Expression(translation.Stages[0].Query.OrderBy[0].Expression, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Regexp(t, `\(s\d+\.n0\)\.properties`, ordering.Statement)
}

func TestMultipartLiteralWithPreservesInputRows(t *testing.T) {
	for _, tc := range []struct {
		query  string
		stage  int
		prefix bool
	}{
		{query: `UNWIND [1, 2] AS x CREATE (n:User) WITH 1 AS value RETURN count(value)`},
		{query: `UNWIND [1, 1] AS x CREATE (n:User) WITH 1 AS value RETURN count(value)`},
		{query: `UNWIND [] AS x CREATE (n:User) WITH 1 AS value RETURN count(value)`},
		{query: `MATCH (n:User) DELETE n WITH 1 AS value RETURN count(value)`},
		{query: `CREATE (n:User) WITH n WITH 1 AS value RETURN count(value)`, stage: 1},
		{query: `MATCH (n:User) WITH 1 AS value CREATE (m:User) WITH m RETURN m`, prefix: true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)
			translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)
			require.Greater(t, len(translation.Stages), tc.stage)
			boundary := translation.Stages[tc.stage].Query
			if tc.prefix {
				boundary = boundary.CommonTableExpressions.Expressions[0].Query
			}
			projection := boundary.Body.(pgsql.Select)
			require.Len(t, projection.From, 1, "literal WITH must retain the input row stream")
		})
	}
}

func TestMultipartWithAliasPredicateFiltersBeforeNextMutation(t *testing.T) {
	for _, modifiers := range []string{"", "ORDER BY value DESC LIMIT 1"} {
		query, err := frontend.ParseCypher(frontend.NewContext(), `UNWIND [1, 2, 3] AS x CREATE (n:User {x: x}) WITH n, n.x AS value `+modifiers+` WHERE value < 3 SET n.seen = true WITH n RETURN n.x`)
		require.NoError(t, err)
		translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
		require.NoError(t, err)
		require.Len(t, translation.Stages, 3)
		boundary := translation.Stages[0]
		output := boundary.Query.Body.(pgsql.Select)
		require.NotNil(t, output.Where, "computed alias predicates belong to their own WITH boundary")
		require.Nil(t, boundary.Query.Limit, "WITH filtering must follow its pagination")
		require.Empty(t, boundary.Query.OrderBy)
		ctes := boundary.Query.CommonTableExpressions.Expressions
		projectionQuery := ctes[len(ctes)-1].Query
		if modifiers != "" {
			require.NotNil(t, projectionQuery.Limit)
			require.Len(t, projectionQuery.OrderBy, 1)
		}
		require.Len(t, output.Projection, 2, "the private predicate column must not escape the WITH boundary")
		references, err := ExtractSyntaxNodeReferences(output.Where)
		require.NoError(t, err)
		require.Contains(t, references.Slice(), boundary.Binding)
		for _, stage := range translation.Stages[1:] {
			require.Nil(t, stage.Query.Body.(pgsql.Select).Where, "the WITH predicate must not move past the next write")
		}
	}
}

func TestMultipartWithPropertyAliasPredicateRetainsComparisonTypes(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		for _, predicate := range []string{"value < 3", "3 > value"} {
			t.Run(string(mode)+"/"+predicate, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), `UNWIND [1, 2, 3] AS x CREATE (n:User {x: x}) WITH n, n.x AS value ORDER BY value DESC LIMIT 1 WHERE `+predicate+` SET n.seen = true WITH n RETURN n.x`)
				require.NoError(t, err)
				translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
				require.NoError(t, err)
				statements, err := TranslatedStatements(translation, "predicate_")
				require.NoError(t, err)
				require.Regexp(t, `cypher_compare\(.*properties -> E'x'`, statements[0].Statement, "the substituted property must retain its runtime type")
				require.Regexp(t, `properties -> E'x'\) as i\d+`, statements[0].Statement, "typing the predicate must preserve the projected JSON value")
			})
		}
	}
}

func TestTranslatedStatementsQuoteAndValidateTemporaryNames(t *testing.T) {
	translation := Result{Stages: []QueryStage{
		{Binding: "s0", Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(1, pgsql.Int)}}}},
		{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(2, pgsql.Int)}}}},
	}}
	formatted, err := TranslatedStatements(translation, `review"; select 1; --`)
	require.NoError(t, err)
	require.Contains(t, formatted[0].Statement, `pg_temp."review""; select 1; --s0"`)
	require.Contains(t, formatted[1].Statement, `pg_temp."review""; select 1; --s0"`)
	formatted, err = TranslatedStatements(translation, "CaseSensitive_")
	require.NoError(t, err)
	require.Contains(t, formatted[1].Statement, `pg_temp."CaseSensitive_s0"`)
	for _, prefix := range []string{"", "bad\x00", strings.Repeat("x", 63)} {
		_, err := TranslatedStatements(translation, prefix)
		require.Error(t, err)
	}
}

func TestMultipartMutationsReadThePreviousBoundary(t *testing.T) {
	for _, tc := range []struct {
		query  string
		stages int
	}{
		{query: `CREATE (n:User) WITH n SET n.name = 'done' WITH n RETURN n`, stages: 3},
		{query: `MATCH (n:User) WITH n DELETE n WITH 1 AS x RETURN x`, stages: 2},
	} {
		t.Run(tc.query, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)
			translation, err := Translate(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID)
			require.NoError(t, err)
			require.Len(t, translation.Stages, tc.stages)
			for _, stage := range translation.Stages {
				if stage.Binding == "" {
					continue
				}
				formatted, err := format.Statement(stage.Query, format.NewOutputBuilder())
				require.NoError(t, err)
				require.NotContains(t, formatted.Statement, "from "+stage.Binding.String()+" ", "a mutation cannot read its own future output boundary")
				for _, cte := range stage.Query.CommonTableExpressions.Expressions {
					if _, deletion := cte.Query.Body.(pgsql.Delete); deletion {
						projection := stage.Query.Body.(pgsql.Select)
						for _, from := range projection.From {
							require.NotEqual(t, cte.Alias.Name, from.Source)
						}
					}
				}
			}
		})
	}
}
