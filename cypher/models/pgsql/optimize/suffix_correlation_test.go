package optimize

import (
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestSuffixSearchRejectsExternalPredicateDependencies(t *testing.T) {
	for _, predicate := range []string{"terminal.name = wanted", "head.name = wanted", "boundary.name = wanted", "middle.name = wanted", "terminal.name = head.name", "terminal.name <> head.name", "root.name = wanted", "wanted = 'absent'", "false"} {
		t.Run(predicate, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (root:ExpansionRoot) WITH root, 'wanted' AS wanted MATCH path = (root)-[:Expand*0..16]->(boundary)-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(middle:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) WHERE `+predicate+` RETURN path`)
			require.NoError(t, err)
			plan, err := Optimize(query)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
			decision := plan.LoweringPlan.ExpansionSearchStrategy[0]
			require.False(t, decision.StructurallyEligible)
			require.False(t, decision.StaticallyEligible)
			require.Equal(t, ExpansionSearchFallbackCorrelatedSuffix, decision.FallbackReason)
		})
	}
}

func TestSuffixSearchRejectsInlineCorrelationAndRepeatedBindings(t *testing.T) {
	const base = `MATCH (root:ExpansionRoot) WITH root, 'wanted' AS wanted MATCH path = (root)-[:Expand*0..16]->(boundary)-[:EnterSuffix]->(head:SuffixHead)-[:ContinueSuffix]->(middle:SuffixMiddle)-[:CompleteSuffix]->(terminal:SuffixTerminal) RETURN path`
	for _, queryText := range []string{
		strings.Replace(base, "terminal:SuffixTerminal", "terminal:SuffixTerminal {name: wanted}", 1),
		strings.Replace(base, "terminal:SuffixTerminal", "terminal:SuffixTerminal {name: head.name}", 1),
		strings.Replace(base, "(boundary)", "(boundary {name: wanted})", 1),
		strings.Replace(base, "MATCH path = (root)", "MATCH path = (root {name: 'absent'})", 1),
		strings.Replace(base, "MATCH path = (root)", "MATCH path = (root:SuffixTerminal)", 1),
		strings.Replace(base, "terminal:SuffixTerminal", "head:SuffixTerminal", 1),
		strings.Replace(base, "terminal:SuffixTerminal", "boundary:SuffixTerminal", 1),
	} {
		t.Run(queryText, func(t *testing.T) {
			query, err := frontend.ParseCypher(frontend.NewContext(), queryText)
			require.NoError(t, err)
			plan, err := Optimize(query)
			require.NoError(t, err)
			require.Len(t, plan.LoweringPlan.ExpansionSearchStrategy, 1)
			require.False(t, plan.LoweringPlan.ExpansionSearchStrategy[0].StructurallyEligible)
		})
	}
}
