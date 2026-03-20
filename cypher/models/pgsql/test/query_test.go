package test

import (
	"context"
	"slices"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/query"
)

func TestQuery_KindGeneratesInclusiveKindMatcher(t *testing.T) {
	mapper := newKindMapper()

	queries := []*cypher.Where{
		query.Where(query.KindIn(query.Node(), NodeKind1)),
		query.Where(query.Kind(query.Node(), NodeKind2)),
	}

	for _, nodeQuery := range queries {
		builder := query.NewBuilderWithCriteria(nodeQuery)
		builtQuery, err := builder.Build(false)
		if err != nil {
			t.Errorf("could not build query: %v", err)
		}

		translatedQuery, err := translate.Translate(context.Background(), builtQuery, mapper, nil)
		if err != nil {
			t.Errorf("could not translate query: %#v: %v", builtQuery, err)
		}

		walk.PgSQL(translatedQuery.Statement, walk.NewSimpleVisitor(func(node pgsql.SyntaxNode, visitorHandler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case *pgsql.BinaryExpression:
				switch leftTyped := typedNode.LOperand.(type) {
				case pgsql.CompoundIdentifier:
					if slices.Equal(leftTyped, pgsql.AsCompoundIdentifier("n0", "kind_ids")) && typedNode.Operator != pgsql.OperatorPGArrayOverlap {
						t.Errorf("query did not generate an array overlap operator (&&): %#v", nodeQuery)
					}
				}
			}
		}))
	}
}
