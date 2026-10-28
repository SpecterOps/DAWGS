package translate

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

// The aggregate traversal count lowering accepted MATCH WHERE clauses carrying
// pattern predicates and then panicked while walking them with a predicate
// translator that has no query parts. The lowering must reject those shapes
// with a normal translation error. DAWGS-036 repairs generic predicate lifetime;
// this test retains the specialized lowering's explicit rejection until its
// fallback is separately qualified.
func TestAggregateTraversalCountPatternPredicateReturnsTranslationError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "source where",
			query: `MATCH (s:User) WHERE (s)-[:MemberOf]->() MATCH (s)-[:MemberOf*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
		{
			name:  "terminal where",
			query: `MATCH (s:User) MATCH (s)-[:MemberOf*1..3]->(t) WHERE (t)-[:MemberOf]->() WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
		{
			name:  "negated source where",
			query: `MATCH (s:User) WHERE NOT (s)-[:MemberOf]->() MATCH (s)-[:MemberOf*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
		{
			name:  "disjunct source where",
			query: `MATCH (s:User) WHERE (s)-[:MemberOf]->() OR s.hasspn = true MATCH (s)-[:MemberOf*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)

			translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: OptimizerEnabled})
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported aggregate traversal count predicate: pattern predicates in MATCH WHERE are not supported")

			// The lowering never runs with optimization disabled; the generic
			// translation path uses the predicate lifetime repair from DAWGS-036.
			translation, err = TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: OptimizerDisabled})
			require.NoError(t, err)
			require.Empty(t, translation.Optimization.PlannedLowerings)
		})
	}
}

// The lowering must keep accepting the shapes it already translates correctly:
// plain aggregate traversal counts and property-only WHERE constraints.
func TestAggregateTraversalCountWithoutPatternPredicateStillLowers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "plain shape",
			query: `MATCH (s:User) MATCH (s)-[:MemberOf*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
		{
			name:  "source property predicate",
			query: `MATCH (s:User) WHERE s.hasspn = true MATCH (s)-[:MemberOf*1..3]->(t) WITH s, count(t) AS c RETURN s, c ORDER BY c DESC LIMIT 100`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			query, err := frontend.ParseCypher(frontend.NewContext(), tc.query)
			require.NoError(t, err)

			translation, err := TranslateWithOptions(context.Background(), query, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: OptimizerEnabled})
			require.NoError(t, err)

			formatted, err := Translated(translation)
			require.NoError(t, err)
			require.Contains(t, formatted.Statement, "candidate_sources")
		})
	}
}

// containsPatternPredicate must see through negation and compound predicates
// and leave ordinary WHERE expressions alone.
func TestContainsPatternPredicateDetection(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		where    string
		expected bool
	}{
		{name: "bare pattern predicate", where: `(s)-[:MemberOf]->()`, expected: true},
		{name: "negated pattern predicate", where: `NOT (s)-[:MemberOf]->()`, expected: true},
		{name: "disjunction with pattern predicate", where: `(s)-[:MemberOf]->() OR s.hasspn = true`, expected: true},
		{name: "property comparison", where: `s.hasspn = true`, expected: false},
		{name: "compound comparisons", where: `s.hasspn = true AND s.enabled = false`, expected: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (s:User) WHERE `+tc.where+` RETURN s`)
			require.NoError(t, err)

			require.Equal(t, tc.expected, containsPatternPredicate(query.SingleQuery.SinglePartQuery.ReadingClauses[0].Match.Where))
		})
	}

	require.False(t, containsPatternPredicate(nil))
}

// The producer boundary must report a missing query part instead of panicking
// when a pattern predicate is walked outside a translated query.
func TestPreparePatternPredicateWithoutQueryPartReturnsError(t *testing.T) {
	t.Parallel()

	translator := NewTranslator(context.Background(), pgutil.NewInMemoryKindMapper(), nil, DefaultGraphID)
	require.Error(t, translator.preparePatternPredicate(cypher.NewPatternPredicate()))
}
