package v2_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/query/v2"
	"github.com/stretchr/testify/require"
)

const (
	kindPredicateError = "kind predicate requires at least one kind"
	kindAssignmentErr  = "kind assignment requires at least one kind"
	kindRemovalError   = "kind removal requires at least one kind"
)

// TestEmptyKindPredicatesAreRejectedAtBuild pins the DAWGS-086 repair: a kind predicate
// without kinds must fail Build instead of silently dropping the restriction (the
// relationship case formerly rendered `match ()-[r]->() return r`, matching every
// relationship) or emitting blank WHERE output (the node case formerly rendered
// `match (n) where  return n`).
func TestEmptyKindPredicatesAreRejectedAtBuild(t *testing.T) {
	kindA := graph.StringKind("A")

	cases := map[string]struct {
		builder      v2.QueryBuilder
		expectedErr  string
		formerOutput string
	}{
		"relationship kind in": {
			builder:      v2.New().Where(v2.KindIn(v2.Relationship())).Return(v2.Relationship()),
			expectedErr:  kindPredicateError,
			formerOutput: "match ()-[r]->() return r",
		},
		"relationship kind": {
			builder:     v2.New().Where(v2.Kind(v2.Relationship())).Return(v2.Relationship()),
			expectedErr: kindPredicateError,
		},
		"node kind in": {
			builder:      v2.New().Where(v2.KindIn(v2.Node())).Return(v2.Node()),
			expectedErr:  kindPredicateError,
			formerOutput: "match (n) where  return n",
		},
		"node kind": {
			builder:     v2.New().Where(v2.Kind(v2.Node())).Return(v2.Node()),
			expectedErr: kindPredicateError,
		},
		"start node kind in": {
			builder:     v2.New().Where(v2.KindIn(v2.Start())).Return(v2.Relationship()),
			expectedErr: kindPredicateError,
		},
		"end node kind in": {
			builder:     v2.New().Where(v2.KindIn(v2.End())).Return(v2.Relationship()),
			expectedErr: kindPredicateError,
		},
		"negated node kind in": {
			builder:     v2.New().Where(v2.Not(v2.KindIn(v2.Node()))).Return(v2.Node()),
			expectedErr: kindPredicateError,
		},
		"empty kind in nested in conjunction": {
			builder: v2.New().Where(
				v2.And(v2.KindIn(v2.Node()), v2.Kind(v2.Node(), kindA)),
			).Return(v2.Node()),
			expectedErr: kindPredicateError,
		},
		"empty kind in nested in disjunction": {
			builder: v2.New().Where(
				v2.Or(v2.KindIn(v2.Node()), v2.Kind(v2.Node(), kindA)),
			).Return(v2.Node()),
			expectedErr: kindPredicateError,
		},
		"empty relationship kind in beside populated matcher": {
			builder: v2.New().Where(
				v2.KindIn(v2.Relationship()),
				v2.Kind(v2.Relationship(), kindA),
			).Return(v2.Relationship()),
			expectedErr: kindPredicateError,
		},
		"hand-built kind matcher without kinds": {
			builder: v2.New().Where(
				&cypher.KindMatcher{Reference: cypher.NewVariableWithSymbol("n")},
			).Return(v2.Node()),
			expectedErr: kindPredicateError,
		},
		"empty kind assignment via compat helper": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.AddKinds(v2.Node(), graph.Kinds{}),
			).Return(v2.Node()),
			expectedErr: kindAssignmentErr,
		},
		"empty kind assignment via continuation": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.Node().Kinds().Add(graph.Kinds{}),
			).Return(v2.Node()),
			expectedErr: kindAssignmentErr,
		},
		"empty kind removal via compat helper": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.DeleteKinds(v2.Node(), graph.Kinds{}),
			).Return(v2.Node()),
			expectedErr: kindRemovalError,
		},
		"empty kind removal via continuation": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.Node().Kinds().Remove(graph.Kinds{}),
			).Return(v2.Node()),
			expectedErr: kindRemovalError,
		},
		"empty kind assignment beside populated assignment": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.AddKind(v2.Node(), kindA),
				v2.AddKinds(v2.Node(), graph.Kinds{}),
			).Return(v2.Node()),
			expectedErr: kindAssignmentErr,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.ErrorContains(t, err, testCase.expectedErr)
			require.Nil(t, preparedQuery)
		})
	}
}

// TestPopulatedKindPredicatesStillRender verifies the repair leaves populated kind
// predicates, pattern folding, kind assignments and kind removals unchanged.
func TestPopulatedKindPredicatesStillRender(t *testing.T) {
	kindA := graph.StringKind("A")
	kindB := graph.StringKind("B")

	cases := map[string]struct {
		builder        v2.QueryBuilder
		expectedCypher string
	}{
		"relationship kind folds into pattern": {
			builder:        v2.New().Where(v2.KindIn(v2.Relationship(), kindA)).Return(v2.Relationship()),
			expectedCypher: "match ()-[r:A]->() return r",
		},
		"relationship kind via compat kind helper": {
			builder:        v2.New().Where(v2.Kind(v2.Relationship(), kindA)).Return(v2.Relationship()),
			expectedCypher: "match ()-[r:A]->() return r",
		},
		"node kind renders in where": {
			builder:        v2.New().Where(v2.KindIn(v2.Node(), kindA)).Return(v2.Node()),
			expectedCypher: "match (n) where n:A return n",
		},
		"node kind beside property predicate": {
			builder: v2.New().Where(
				v2.KindIn(v2.Node(), kindA),
				v2.Node().Property("name").Equals("x"),
			).Return(v2.Node()),
			expectedCypher: "match (n) where n:A and n.name = $p0 return n",
		},
		"negated populated kind": {
			builder:        v2.New().Where(v2.Not(v2.KindIn(v2.Node(), kindA))).Return(v2.Node()),
			expectedCypher: "match (n) where not n:A return n",
		},
		"populated kind assignment": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.AddKinds(v2.Node(), graph.Kinds{kindB}),
			).Return(v2.Node()),
			expectedCypher: "match (n) where n:A set n:B return n",
		},
		"populated kind removal": {
			builder: v2.New().Where(
				v2.Kind(v2.Node(), kindA),
			).Update(
				v2.DeleteKinds(v2.Node(), graph.Kinds{kindB}),
			).Return(v2.Node()),
			expectedCypher: "match (n) where n:A remove n:B return n",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			preparedQuery, err := testCase.builder.Build()
			require.NoError(t, err)

			rendered, err := format.RegularQuery(preparedQuery.Query, false)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedCypher, rendered)
		})
	}
}
