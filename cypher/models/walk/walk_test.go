package walk_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	visitor := walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
	})

	// Walk through all positive test cases to ensure that the walker can visit the involved types
	for _, testCase := range test.LoadFixture(t, test.PositiveTestCases).RunnableCases() {
		if testCase.Type == test.TypeStringMatch {
			parseContext := frontend.NewContext()

			if details, err := test.UnmarshallTestCaseDetails[test.StringMatchTest](testCase); err != nil {
				t.Fatalf("Error unmarshalling test case details: %v", err)
			} else if queryModel, err := frontend.ParseCypher(parseContext, details.Query); err != nil {
				t.Fatalf("Parser errors: %s", err.Error())
			} else {
				require.Nil(t, walk.Cypher(queryModel, visitor))
			}
		}
	}
}
