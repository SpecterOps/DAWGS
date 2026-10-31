package analyzer_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/analyzer"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestQueryFitness(t *testing.T) {
	if updateCases, varSet := os.LookupEnv("CYSQL_UPDATE_CASES"); varSet && strings.ToLower(strings.TrimSpace(updateCases)) == "true" {
		if err := test.UpdatePositiveTestCasesFitness(); err != nil {
			fmt.Printf("Error updating cases: %v\n", err)
		}
	}

	// Walk through all positive test cases to ensure that the walker can handle all supported types
	for _, caseLoad := range []string{test.PositiveTestCases, test.MutationTestCases} {
		for _, testCase := range test.LoadFixture(t, caseLoad).RunnableCases() {
			t.Run(testCase.Name, func(t *testing.T) {
				// Only bother with the string match tests
				if testCase.Type == test.TypeStringMatch {
					parseContext := frontend.NewContext()

					if details, err := test.UnmarshallTestCaseDetails[test.StringMatchTest](testCase); err != nil {
						t.Fatalf("Error unmarshalling test case details: %v", err)
					} else if queryModel, err := frontend.ParseCypher(parseContext, details.Query); err != nil {
						t.Fatalf("Parser errors: %s", err.Error())
					} else if details.ExpectedFitness != nil {
						complexity, analyzerErr := analyzer.QueryComplexity(queryModel)

						if analyzerErr != nil {
							require.Nilf(t, analyzerErr, "Unexpected error: %s", analyzerErr.Error())
						}

						require.Equal(t, *details.ExpectedFitness, complexity.RelativeFitness)
					}
				}
			})
		}
	}
}

// Property lookups over non-variable atoms, such as temporal component access,
// previously failed complexity measurement with an error; they must measure as
// ordinary non-selective comparisons instead.
func TestQueryComplexityAcceptsNonVariablePropertyLookupAtoms(t *testing.T) {
	for _, query := range []string{
		`match (n:NodeKind1) where datetime($ts).epochmillis > 0 return n`,
		`match (n:NodeKind1) where 0 < date(n.d).year return n`,
		`return datetime('2020-01-01').epochmillis as value`,
	} {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
		require.NoError(t, err, query)

		_, err = analyzer.QueryComplexity(parsed)
		require.NoError(t, err, query)
	}
}
