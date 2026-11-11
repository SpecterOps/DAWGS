package test

import (
	"context"
	"slices"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
	v2 "github.com/specterops/dawgs/query/v2"
)

var (
	// NodeKind1 is the first stable node-kind name used by builder translation fixtures.
	NodeKind1 = graph.StringKind("NodeKind1")

	// NodeKind2 is the second stable node-kind name used by builder translation fixtures.
	NodeKind2 = graph.StringKind("NodeKind2")

	// EdgeKind1 is the first stable relationship-kind name used by builder translation fixtures.
	EdgeKind1 = graph.StringKind("EdgeKind1")

	// EdgeKind2 is the second stable relationship-kind name used by builder translation fixtures.
	EdgeKind2 = graph.StringKind("EdgeKind2")
)

func TestQuery_KindGeneratesInclusiveKindMatcher(t *testing.T) {
	mapper := newKindMapper()

	queries := []v2.QueryBuilder{
		v2.New().Where(v2.KindIn(v2.Node(), NodeKind1)).Return(v2.Node()),
		v2.New().Where(v2.Kind(v2.Node(), NodeKind2)).Return(v2.Node()),
	}

	for _, queryBuilder := range queries {
		builtQuery, err := queryBuilder.Build()
		if err != nil {
			t.Fatalf("could not build query: %v", err)
		}

		translatedQuery, err := translate.Translate(context.Background(), builtQuery.Query, mapper, builtQuery.Parameters, translate.DefaultGraphID)
		if err != nil {
			t.Fatalf("could not translate query: %#v: %v", builtQuery, err)
		}

		walk.PgSQL(translatedQuery.Statement, walk.NewSimpleVisitor(func(node pgsql.SyntaxNode, visitorHandler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case *pgsql.BinaryExpression:
				switch leftTyped := typedNode.LOperand.(type) {
				case pgsql.CompoundIdentifier:
					if slices.Equal(leftTyped, pgsql.AsCompoundIdentifier("n0", "kind_ids")) && typedNode.Operator != pgsql.OperatorPGArrayOverlap {
						t.Errorf("query did not generate an array overlap operator (&&): %#v", builtQuery)
					}
				}
			}
		}))
	}
}
