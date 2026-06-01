package walk_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

func BenchmarkCypherWalkLargeProjection(b *testing.B) {
	projection := &cypher.Projection{}
	for idx := 0; idx < 512; idx++ {
		projection.Items = append(projection.Items, &cypher.ProjectionItem{
			Expression: cypher.NewVariableWithSymbol(fmt.Sprintf("n%d", idx)),
		})
	}

	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(cypher.SyntaxNode, walk.VisitorHandler) {})

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := walk.Cypher(projection, visitor); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCypherWalkLargeMapLiteral(b *testing.B) {
	mapLiteral := cypher.NewMapLiteral()
	for idx := 0; idx < 512; idx++ {
		mapLiteral[fmt.Sprintf("k%03d", idx)] = cypher.NewVariableWithSymbol(fmt.Sprintf("v%d", idx))
	}

	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(cypher.SyntaxNode, walk.VisitorHandler) {})

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := walk.Cypher(mapLiteral, visitor); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCypherStructuralWalkLongPattern(b *testing.B) {
	patternPart := cypher.NewPatternPart()
	patternPart.Variable = cypher.NewVariableWithSymbol("path")
	patternPart.AddPatternElements(&cypher.NodePattern{
		Variable: cypher.NewVariableWithSymbol("n0"),
	})
	for idx := 0; idx < 128; idx++ {
		patternPart.AddPatternElements(
			&cypher.RelationshipPattern{
				Variable: cypher.NewVariableWithSymbol(fmt.Sprintf("r%d", idx)),
			},
			&cypher.NodePattern{
				Variable: cypher.NewVariableWithSymbol(fmt.Sprintf("n%d", idx+1)),
			},
		)
	}

	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(cypher.SyntaxNode, walk.VisitorHandler) {})

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := walk.CypherStructural(patternPart, visitor); err != nil {
			b.Fatal(err)
		}
	}
}
