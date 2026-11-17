package translate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// budgetedBuilderParameterPrefix isolates generated test bindings while keeping
// the original and parameter-rewritten builder forms comparable.
const budgetedBuilderParameterPrefix = "__compact_builder_"

// budgetedBuilderQuery groups the AST and request-local bindings supplied by a
// builder query so tests cannot accidentally pair unrelated inputs.
type budgetedBuilderQuery struct {
	// Query contains the caller-owned builder AST.
	Query *cypher.RegularQuery

	// Parameters maps generated symbols to the builder's literal values.
	Parameters map[string]any
}

// budgetedBuilderKindCase describes a positive kind domain and its mapping result.
type budgetedBuilderKindCase struct {
	// name identifies the behavior shown by the subtest.
	name string

	// filter supplies the builder's optional relationship-kind criterion.
	filter graph.Criteria

	// kinds lists the expected normalized kind names in stable order.
	kinds graph.Kinds

	// missing indicates that at least one kind name is absent from the mapper.
	missing bool
}

// budgetedBuilderResidualCase describes a predicate that must retain legacy lowering.
type budgetedBuilderResidualCase struct {
	// name identifies the semantic boundary shown by the subtest.
	name string

	// filter supplies the relationship predicate that cannot be fully relocated.
	filter graph.Criteria

	// returnRelationship requires the observable variable-length relationship list.
	returnRelationship bool
}

// budgetedBuilderKindQuery constructs BHE's builder shape and retains its inputs
// for subsequent normalization, immutability, and translation assertions.
func budgetedBuilderKindQuery(t *testing.T, filter graph.Criteria, returnRelationship bool, limit int) budgetedBuilderQuery {
	t.Helper()

	criteria := []graph.Criteria{
		query.Equals(query.StartID(), graph.ID(10)),
		query.Equals(query.EndID(), graph.ID(20)),
	}
	if filter != nil {
		criteria = append(criteria, filter)
	}

	projection := query.Returning(query.Path())
	if returnRelationship {
		projection = query.Returning(query.Path(), query.Relationship())
	}

	builder := query.NewBuilderWithCriteria(query.Where(query.And(criteria...)), projection)
	if limit > 0 {
		builder.Apply(query.Limit(limit))
	}

	regularQuery, err := builder.Build(true)
	require.NoError(t, err)

	namer := query.NewParameterNamerWithPrefix(budgetedBuilderParameterPrefix)
	require.NoError(t, walk.Cypher(regularQuery, namer))

	return budgetedBuilderQuery{
		Query:      regularQuery,
		Parameters: namer.Parameters,
	}
}

// TestBudgetedAllShortestBuilderKindDomains verifies BHE lists, Hunter OR chunks, and
// intersections reach the compact executor without mutating cached inputs.
func TestBudgetedAllShortestBuilderKindDomains(t *testing.T) {
	var (
		kindA       = graph.StringKind("CompactA")
		kindB       = graph.StringKind("CompactB")
		kindC       = graph.StringKind("CompactC")
		missingKind = graph.StringKind("CompactMissing")
	)

	for _, testCase := range []budgetedBuilderKindCase{
		{
			name: "untyped",
		},
		{
			name:   "BHE kind list",
			filter: query.KindIn(query.Relationship(), kindA, kindB),
			kinds:  graph.Kinds{kindA, kindB},
		},
		{
			name:   "Hunter positive OR chunks",
			filter: query.Or(query.KindIn(query.Relationship(), kindA, kindB), query.KindIn(query.Relationship(), kindB, kindC)),
			kinds:  graph.Kinds{kindA, kindB, kindC},
		},
		{
			name:   "positive conjunction intersection",
			filter: query.And(query.KindIn(query.Relationship(), kindA, kindB), query.KindIn(query.Relationship(), kindB, kindC)),
			kinds:  graph.Kinds{kindB},
		},
		{
			name:    "all missing",
			filter:  query.KindIn(query.Relationship(), missingKind),
			kinds:   graph.Kinds{missingKind},
			missing: true,
		},
		{
			name:    "mixed known and missing",
			filter:  query.KindIn(query.Relationship(), kindA, missingKind),
			kinds:   graph.Kinds{kindA, missingKind},
			missing: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var (
				builderQuery = budgetedBuilderKindQuery(t, testCase.filter, false, 0)
				regularQuery = builderQuery.Query
				before       = cypher.Copy(regularQuery)
			)

			require.True(t, UsesBudgetedAllShortestPaths(regularQuery))
			require.Equal(t, before, regularQuery, "eligibility must not mutate the builder/cache AST")
			if testCase.filter != nil {
				normalized, changed := normalizeBudgetedAllShortestKinds(regularQuery)
				require.True(t, changed)

				relationship, ok := normalized.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0].PatternElements[1].AsRelationshipPattern()
				require.True(t, ok)
				require.Equal(t, testCase.kinds.Strings(), relationship.Kinds.Strings())
			}
			owned := cypher.Copy(regularQuery)
			require.NoError(t, walk.Cypher(owned, query.NewParameterRewriterWithPrefix(budgetedBuilderParameterPrefix)))
			require.True(t, UsesBudgetedAllShortestPaths(owned))

			var (
				mapper  = pgutil.NewInMemoryKindMapper()
				kindAID = mapper.Put(kindA)
				kindBID = mapper.Put(kindB)
				kindCID = mapper.Put(kindC)
			)

			translation, err := Translate(context.Background(), owned, mapper, builderQuery.Parameters, 1)
			require.NoError(t, err)
			require.Equal(t, testCase.missing, translation.HasMissingKinds)

			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted.Statement, "all_shortest_paths_dag_v2")
			require.Equal(t, optimize.ShortestPathExecutorASPA1DAGV2,
				translation.Optimization.LoweringPlan.ShortestPathExecutor[0].SelectedExecutor)
			switch testCase.name {
			case "BHE kind list":
				require.Contains(t, formatted.Statement, fmt.Sprintf("array [%d, %d]::int2[]", kindAID, kindBID))
			case "Hunter positive OR chunks":
				require.Contains(t, formatted.Statement, fmt.Sprintf("array [%d, %d, %d]::int2[]", kindAID, kindBID, kindCID))
			case "positive conjunction intersection":
				require.Contains(t, formatted.Statement, fmt.Sprintf("array [%d]::int2[]", kindBID))
			case "all missing":
				require.Contains(t, formatted.Statement, "and false")
			case "mixed known and missing":
				require.Contains(t, formatted.Statement, fmt.Sprintf("array [%d]::int2[]", kindAID))
			}
			require.Equal(t, before, regularQuery)
		})
	}
}

// TestBudgetedAllShortestBuilderRetainsUnsupportedPredicates verifies unsupported observations
// preserve their legacy SQL or translation error.
func TestBudgetedAllShortestBuilderRetainsUnsupportedPredicates(t *testing.T) {
	var (
		kindA = graph.StringKind("CompactA")
		kindB = graph.StringKind("CompactB")
	)

	for _, testCase := range []budgetedBuilderResidualCase{
		{
			name:   "empty kinds",
			filter: query.KindIn(query.Relationship()),
		},
		{
			name:   "empty intersection",
			filter: query.And(query.KindIn(query.Relationship(), kindA), query.KindIn(query.Relationship(), kindB)),
		},
		{
			name:               "returned relationship list",
			filter:             query.KindIn(query.Relationship(), kindA),
			returnRelationship: true,
		},
		{
			name:   "relationship property",
			filter: query.And(query.KindIn(query.Relationship(), kindA), query.Equals(query.RelationshipProperty("active"), true)),
		},
		{
			name:   "mixed OR",
			filter: query.Or(query.KindIn(query.Relationship(), kindA), query.Equals(query.RelationshipProperty("active"), true)),
		},
		{
			name:   "negated kinds",
			filter: query.Not(query.KindIn(query.Relationship(), kindA)),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var (
				builderQuery = budgetedBuilderKindQuery(t, testCase.filter, testCase.returnRelationship, 0)
				regularQuery = builderQuery.Query
				before       = cypher.Copy(regularQuery)
			)

			require.False(t, UsesBudgetedAllShortestPaths(regularQuery))

			mapper := pgutil.NewInMemoryKindMapper()
			mapper.Put(kindA)
			mapper.Put(kindB)
			automatic, automaticErr := Translate(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1)
			legacy, legacyErr := TranslateWithOptions(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1,
				Options{
					DisableBudgetedAllShortestPaths: true,
				})
			if legacyErr != nil {
				require.EqualError(t, automaticErr, legacyErr.Error())
				require.Equal(t, before, regularQuery)
				return
			}
			require.NoError(t, automaticErr)

			automaticSQL, err := Translated(automatic)
			require.NoError(t, err)

			legacySQL, err := Translated(legacy)
			require.NoError(t, err)
			require.Equal(t, legacySQL.Statement, automaticSQL.Statement)
			require.Equal(t, legacy.Parameters, automatic.Parameters)
			require.NotContains(t, automaticSQL.Statement, "all_shortest_paths_dag_v2")
			require.Equal(t, before, regularQuery)
		})
	}
}

// TestBudgetedAllShortestBuilderKeepsLimitOutsideDiscovery verifies row limits apply only after
// complete shortest-path discovery.
func TestBudgetedAllShortestBuilderKeepsLimitOutsideDiscovery(t *testing.T) {
	kind := graph.StringKind("CompactA")
	builderQuery := budgetedBuilderKindQuery(t, query.KindIn(query.Relationship(), kind), false, 3)
	regularQuery := builderQuery.Query
	mapper := pgutil.NewInMemoryKindMapper()
	mapper.Put(kind)
	translation, err := Translate(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1)
	require.NoError(t, err)

	formatted, err := Translated(translation)
	require.NoError(t, err)
	require.Contains(t, formatted.Statement, "all_shortest_paths_dag_v2")
	require.Equal(t, 1, strings.Count(formatted.Statement, " limit "))
	require.True(t, strings.HasSuffix(strings.TrimSpace(formatted.Statement), "limit 3;"))
}

// TestBudgetedAllShortestBuilderIntersectsExistingPatternKinds verifies relocated criteria
// preserve existing kinds and empty-domain semantics.
func TestBudgetedAllShortestBuilderIntersectsExistingPatternKinds(t *testing.T) {
	var (
		kindA = graph.StringKind("CompactA")
		kindB = graph.StringKind("CompactB")
		kindC = graph.StringKind("CompactC")
	)

	regularQuery := budgetedBuilderKindQuery(t, query.KindIn(query.Relationship(), kindB, kindC), false, 0).Query
	relationship, ok := regularQuery.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0].PatternElements[1].AsRelationshipPattern()
	require.True(t, ok)
	relationship.Kinds = graph.Kinds{kindA, kindB}
	normalized, changed := normalizeBudgetedAllShortestKinds(regularQuery)
	require.True(t, changed)

	resolved, ok := normalized.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Pattern[0].PatternElements[1].AsRelationshipPattern()
	require.True(t, ok)
	require.Equal(t, graph.Kinds{kindB}, resolved.Kinds)
	require.Equal(t, graph.Kinds{kindA, kindB}, relationship.Kinds, "normalization must retain the caller's original kind list")

	relationship.Kinds = graph.Kinds{kindA}
	unchanged, changed := normalizeBudgetedAllShortestKinds(regularQuery)
	require.False(t, changed, "empty intersection must never become an untyped wildcard")
	require.Same(t, regularQuery, unchanged)
	require.False(t, UsesBudgetedAllShortestPaths(regularQuery))
}

// TestBudgetedAllShortestBuilderNormalizationKeepsLegacyPolicyBoundaries verifies normalization
// cannot broaden explicit policy or canary eligibility.
func TestBudgetedAllShortestBuilderNormalizationKeepsLegacyPolicyBoundaries(t *testing.T) {
	kind := graph.StringKind("CompactA")
	builderQuery := budgetedBuilderKindQuery(t, query.KindIn(query.Relationship(), kind), false, 0)
	regularQuery := builderQuery.Query
	before := cypher.Copy(regularQuery)
	mapper := pgutil.NewInMemoryKindMapper()
	mapper.Put(kind)
	legacy, err := TranslateWithOptions(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1,
		Options{
			DisableBudgetedAllShortestPaths: true,
		})
	require.NoError(t, err)

	legacySQL, err := Translated(legacy)
	require.NoError(t, err)
	require.Contains(t, legacySQL.Statement, "unidirectional_asp_harness_v2")

	production, err := TranslateWithProductionOptions(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1,
		ProductionOptions{
			SelectorVersion:     "legacy-builder-policy",
			DisableInlineASPDAG: true,
		})
	require.NoError(t, err)

	productionSQL, err := Translated(production)
	require.NoError(t, err)
	require.Equal(t, legacySQL.Statement, productionSQL.Statement)
	require.Equal(t, legacy.Parameters, production.Parameters)
	_, err = TranslateForTool(context.Background(), regularQuery, mapper, builderQuery.Parameters, 1,
		ToolOptions{
			ForceShortestPathExecutor: optimize.ShortestPathExecutorASPA1DAG,
		})
	require.ErrorContains(t, err, "no structurally eligible")
	require.Equal(t, before, regularQuery)
}
