package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

// Literal null predicates previously underflowed the expression operand stack:
// the IS/IS NOT completion consumed both operands but only pushed a result for
// property-lookup left sides, so the next pop panicked with index out of range.
// Every shape here must translate and render ordinary SQL in both optimizer modes.
func TestLiteralNullPredicatesTranslate(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, statement string
			}{
				{"canonical null is null", `RETURN null IS NULL AS value`, `select null is null as value;`},
				{"null is not null", `RETURN null IS NOT NULL AS value`, `select null is not null as value;`},
				{"integer is null", `RETURN 1 IS NULL AS value`, `select 1 is null as value;`},
				{"string is not null", `RETURN 'a' IS NOT NULL AS value`, `select @__strlit0::text is not null as value;`},
				{"boolean is null", `RETURN true IS NULL AS value`, `select true is null as value;`},
				{"implicit alias", `RETURN null IS NULL`, `select null is null as "null is null";`},
				{
					"unwind continuation",
					`UNWIND [1] AS x RETURN null IS NULL AS value`,
					`select null is null as value from unnest(array [1]::int8[]) as i0;`,
				},
				{
					"with bound null",
					`WITH null AS n RETURN n IS NULL AS value`,
					`with s0 as (select null as i0) select s0.i0 is null as value from s0;`,
				},
				{
					"with bound null negated form",
					`WITH null AS n RETURN n IS NOT NULL AS value`,
					`with s0 as (select null as i0) select s0.i0 is not null as value from s0;`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err, "%s must translate without a panic or error", tc.query)

					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Equal(t, tc.statement, formatted.Statement)
				})
			}
		})
	}
}

// Literal null predicates in WHERE contexts were silently dropped, so a false
// predicate such as `1 IS NULL` incorrectly matched every row. The assembled
// predicates must now be retained as constraints.
func TestLiteralNullPredicatesRetainedInWhere(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, fragment string
			}{
				{"true literal null predicate", `MATCH (n:MemberOf) WHERE null IS NULL RETURN n`, `(null is null)`},
				{"false literal null predicate", `MATCH (n:MemberOf) WHERE 1 IS NULL RETURN n`, `(1 is null)`},
				{"true negated literal", `MATCH (n:MemberOf) WHERE 'a' IS NOT NULL RETURN n`, `is not null`},
				{"entity operand", `MATCH (n:MemberOf) WHERE n IS NULL RETURN n`, `(n0 is null)`},
				{
					"conjoined with property predicate",
					`MATCH (n:MemberOf) WHERE n.objectid IS NOT NULL AND null IS NULL RETURN n`,
					`null is null and`,
				},
				{
					"unwind where continuation",
					`UNWIND [1, 2] AS x WITH x WHERE x IS NULL RETURN x`,
					`(i0 is null)`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)

					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, tc.fragment)
				})
			}
		})
	}
}

// Property-lookup null predicates keep their dedicated JSONB rewrite, which
// distinguishes missing keys from stored JSON nulls.
func TestPropertyNullPredicateRewritesUnchanged(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name, query, fragment string
			}{
				{
					"property is null",
					`MATCH (n:MemberOf) WHERE n.name IS NULL RETURN n`,
					`(not n0.properties ? E'name' or (n0.properties -> E'name') = (@__strlit0::text)::jsonb)`,
				},
				{
					"property is not null",
					`MATCH (n:MemberOf) WHERE n.name IS NOT NULL RETURN n`,
					`(n0.properties ? E'name' and not (n0.properties -> E'name') = (@__strlit0::text)::jsonb)`,
				},
				{
					"projected property null predicate",
					`MATCH (n:MemberOf) RETURN n.name IS NULL AS value`,
					`(not (s0.n0).properties ? E'name' or ((s0.n0).properties -> E'name') = (@__strlit0::text)::jsonb)`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
					require.NoError(t, err)

					result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)

					formatted, err := Translated(result)
					require.NoError(t, err)
					require.Contains(t, formatted.Statement, tc.fragment)
				})
			}
		})
	}
}

// An empty operand stack is a producer bug that must surface as a translation
// error rather than an unchecked panic that escapes through the driver's
// translation cache.
func TestPopOperandEmptyStackReturnsError(t *testing.T) {
	builder := NewExpressionTreeBuilder()

	operand, err := builder.PopOperand(nil)
	require.Nil(t, operand)
	require.EqualError(t, err, "expression operand stack is empty")

	require.True(t, builder.IsEmpty())
	require.Equal(t, 0, builder.Depth())
}

// IS and IS NOT null predicates apply to values of any type and always produce
// a boolean result; operand type compatibility must not be required.
func TestNullPredicateTypeInference(t *testing.T) {
	nullLiteral := pgsql.NullLiteral()

	for _, operator := range []pgsql.Operator{pgsql.OperatorIs, pgsql.OperatorIsNot} {
		expression := pgsql.NewBinaryExpression(nullLiteral, operator, nullLiteral)
		dataType, err := InferExpressionType(expression)
		require.NoError(t, err)
		require.Equal(t, pgsql.Boolean, dataType, "operator %s", operator)
	}
}
