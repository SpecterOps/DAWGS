package test

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func normalizeSQL(sqlQuery string) string {
	return strings.Join(strings.Fields(strings.ToLower(sqlQuery)), " ")
}

func assertInboundPatternPredicateShape(t *testing.T, sqlQuery string) {
	t.Helper()

	normalized := normalizeSQL(sqlQuery)

	requiredFragments := []string{
		"select count(*) > 0 from s1",
		"from edge e0 join node n1",
		"(s0.n0).id = e0.end_id",
		"n1.kind_ids operator (pg_catalog.@>) array [1]::int2[]",
		"e0.kind_id = any (array [3]::int2[])",
	}

	for _, fragment := range requiredFragments {
		if !strings.Contains(normalized, fragment) {
			t.Fatalf("expected SQL to contain fragment %q but it did not:\n%s", fragment, sqlQuery)
		}
	}

	forbiddenFragments := []string{
		"from s0 join edge e0 on (s0.n0).id = e0.end_id",
	}

	for _, fragment := range forbiddenFragments {
		if strings.Contains(normalized, fragment) {
			t.Fatalf("expected SQL to avoid fragment %q but it was present:\n%s", fragment, sqlQuery)
		}
	}
}

func buildInboundNodeKind1EdgeKind1PatternPredicate(symbol string) *cypher.PatternPredicate {
	patternPredicate := cypher.NewPatternPredicate()

	patternPredicate.AddElement(&cypher.NodePattern{
		Variable: cypher.NewVariableWithSymbol(symbol),
	})

	patternPredicate.AddElement(&cypher.RelationshipPattern{
		Kinds:     graph.Kinds{EdgeKind1},
		Direction: graph.DirectionInbound,
	})

	patternPredicate.AddElement(&cypher.NodePattern{
		Kinds: graph.Kinds{NodeKind1},
	})

	return patternPredicate
}

func TestTranslate_PatternPredicateInboundShape_CypherFrontend(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(
		frontend.NewContext(),
		"match (g:NodeKind2) where not ((g)<-[:EdgeKind1]-(:NodeKind1)) return g",
	)
	if err != nil {
		t.Fatalf("failed to parse cypher query: %v", err)
	}

	translatedQuery, err := translate.Translate(context.Background(), regularQuery, newKindMapper(), nil)
	if err != nil {
		t.Fatalf("failed to translate cypher query: %v", err)
	}

	formattedQuery, err := translate.Translated(translatedQuery)
	if err != nil {
		t.Fatalf("failed to format translated SQL query: %v", err)
	}

	assertInboundPatternPredicateShape(t, formattedQuery)
}

func TestTranslate_PatternPredicateInboundShape_GraphFrontend(t *testing.T) {
	builder := query.NewBuilderWithCriteria(
		query.Where(query.And(
			query.Kind(query.Node(), NodeKind2),
			query.Not(buildInboundNodeKind1EdgeKind1PatternPredicate(query.NodeSymbol)),
		)),
		query.Returning(query.Node()),
	)

	rawQuery, err := builder.Build(false)
	if err != nil {
		t.Fatalf("failed to build graph frontend query: %v", err)
	}

	translatedQuery, err := translate.Translate(context.Background(), rawQuery, newKindMapper(), nil)
	if err != nil {
		t.Fatalf("failed to translate graph frontend query: %v", err)
	}

	formattedQuery, err := translate.Translated(translatedQuery)
	if err != nil {
		t.Fatalf("failed to format translated SQL query: %v", err)
	}

	assertInboundPatternPredicateShape(t, formattedQuery)
}
