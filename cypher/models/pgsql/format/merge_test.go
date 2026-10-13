package format_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/stretchr/testify/require"
)

func TestMergeReturningInCTE(t *testing.T) {
	source := &pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{&pgsql.AliasedExpression{Expression: pgsql.NewLiteral(1, pgsql.Int4), Alias: pgsql.AsOptionalIdentifier("id")}}}}}
	merge := pgsql.Merge{Into: true, Table: pgsql.TableReference{Name: pgsql.Identifier("target").AsCompoundIdentifier(), Binding: pgsql.AsOptionalIdentifier("t")}, SourceQuery: source, Source: pgsql.TableReference{Binding: pgsql.AsOptionalIdentifier("s")}, JoinTarget: pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{"t", "id"}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{"s", "id"}), Actions: []pgsql.MergeAction{pgsql.MergeDoNothing{Matched: true, Predicate: pgsql.NewLiteral(true, pgsql.Boolean)}, pgsql.UnmatchedAction{Columns: []pgsql.Identifier{"id"}, Values: pgsql.Values{Values: []pgsql.Expression{pgsql.CompoundIdentifier{"s", "id"}}}}}, Returning: pgsql.Projection{pgsql.CompoundIdentifier{"s", "id"}, pgsql.FunctionCall{Function: pgsql.FunctionMergeAction}}}
	formatted, err := format.Statement(pgsql.Query{CommonTableExpressions: &pgsql.With{Expressions: []pgsql.CommonTableExpression{{Alias: pgsql.TableAlias{Name: "m"}, Query: pgsql.Query{Body: merge}}}}, Body: pgsql.Select{Projection: pgsql.Projection{pgsql.WildcardIdentifier}, From: []pgsql.FromClause{{Source: pgsql.Identifier("m")}}}}, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Equal(t, "with m as (merge into target t using (select 1 as id) as s on t.id = s.id when matched and true then do nothing when not matched then insert (id) values (s.id) returning s.id, merge_action()) select * from m;", formatted.Statement)
	merge.Actions = []pgsql.MergeAction{pgsql.MergeDoNothing{Matched: false}}
	formatted, err = format.Statement(merge, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Contains(t, formatted.Statement, "when not matched then do nothing")
}

func TestEmptyWindowFormatting(t *testing.T) {
	formatted, err := format.Expression(pgsql.FunctionCall{Function: "row_number", Over: &pgsql.Window{}, CastType: pgsql.Int8}, format.NewOutputBuilder())
	require.NoError(t, err)
	require.Equal(t, "row_number() over ()::int8", formatted.Statement)
	_, err = format.Expression(pgsql.FunctionCall{Function: "row_number", Over: &pgsql.Window{PartitionBy: []pgsql.Expression{pgsql.Identifier("id")}}}, format.NewOutputBuilder())
	require.ErrorContains(t, err, "only empty SQL windows")
}
