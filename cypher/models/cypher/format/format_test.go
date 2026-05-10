package format_test

import (
	"bytes"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"

	"github.com/specterops/dawgs/cypher/test"
)

func TestCypherEmitter_StripLiterals(t *testing.T) {
	var (
		buffer            = &bytes.Buffer{}
		regularQuery, err = frontend.ParseCypher(frontend.DefaultCypherContext(), "match (n {value: 'PII'}) where n.other = 'more pii' and n.number = 411 return n.name, n")
		emitter           = format.Emitter{
			StripLiterals: true,
		}
	)

	require.Nil(t, err)
	require.Nil(t, emitter.Write(regularQuery, buffer))
	require.Equal(t, "match (n {value: $STRIPPED}) where n.other = $STRIPPED and n.number = $STRIPPED return n.name, n", buffer.String())
}

func TestCypherEmitter_RelationshipDirections(t *testing.T) {
	testCases := []struct {
		name      string
		direction graph.Direction
		expected  string
	}{
		{
			name:      "outbound",
			direction: graph.DirectionOutbound,
			expected:  "match (a)-[r]->(b) return r",
		},
		{
			name:      "inbound",
			direction: graph.DirectionInbound,
			expected:  "match (a)<-[r]-(b) return r",
		},
		{
			name:      "both",
			direction: graph.DirectionBoth,
			expected:  "match (a)-[r]-(b) return r",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			regularQuery, singlePartQuery := cypher.NewRegularQueryWithSingleQuery()
			match := singlePartQuery.NewReadingClause().NewMatch(false)
			match.NewPatternPart().AddPatternElements(
				&cypher.NodePattern{
					Variable: cypher.NewVariableWithSymbol("a"),
				},
				&cypher.RelationshipPattern{
					Variable:  cypher.NewVariableWithSymbol("r"),
					Direction: testCase.direction,
				},
				&cypher.NodePattern{
					Variable: cypher.NewVariableWithSymbol("b"),
				},
			)

			singlePartQuery.NewProjection(false).AddItem(cypher.NewProjectionItemWithExpr(cypher.NewVariableWithSymbol("r")))

			rendered, err := format.RegularQuery(regularQuery, false)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, rendered)
		})
	}
}

func TestCypherEmitter_HappyPath(t *testing.T) {
	test.LoadFixture(t, test.MutationTestCases).Run(t)
	test.LoadFixture(t, test.PositiveTestCases).Run(t)
}

func TestCypherEmitter_NegativeCases(t *testing.T) {
	test.LoadFixture(t, test.NegativeTestCases).Run(t)
}

func TestNewStringLiteral_Escaping(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "backslash should be escaped",
			input:    `TEST\PS1-PSV$@`,
			expected: `'TEST\\PS1-PSV$@'`,
		},
		{
			name:     "single quote should be escaped",
			input:    `O'Brien`,
			expected: `'O\'Brien'`,
		},
		{
			name:     "both backslash and single quote",
			input:    `path\to\file's location`,
			expected: `'path\\to\\file\'s location'`,
		},
		{
			name:     "multiple backslashes",
			input:    `C:\Windows\System32`,
			expected: `'C:\\Windows\\System32'`,
		},
		{
			name:     "no special characters",
			input:    `simple_value`,
			expected: `'simple_value'`,
		},
		{
			name:     "empty string",
			input:    ``,
			expected: `''`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			literal := cypher.NewStringLiteral(tc.input)
			require.NotNil(t, literal)
			require.Equal(t, tc.expected, literal.Value)
		})
	}
}

func TestNewStringLiteral_InQuery(t *testing.T) {
	// Test that escaped string literals work correctly in actual Cypher queries
	testCases := []struct {
		name          string
		propertyKey   string
		value         string
		expectedQuery string
	}{
		{
			name:          "backslash in objectid",
			propertyKey:   "objectid",
			value:         `TEST\PS1-PSV$@`,
			expectedQuery: `match (n {objectid: 'TEST\\PS1-PSV$@'}) return n`,
		},
		{
			name:          "single quote in name",
			propertyKey:   "name",
			value:         `O'Brien`,
			expectedQuery: `match (n {name: 'O\'Brien'}) return n`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build a query using NewStringLiteral
			literal := cypher.NewStringLiteral(tc.value)

			// Create a simple query structure
			query := &cypher.RegularQuery{
				SingleQuery: &cypher.SingleQuery{
					SinglePartQuery: &cypher.SinglePartQuery{
						ReadingClauses: []*cypher.ReadingClause{
							{
								Match: &cypher.Match{
									Pattern: []*cypher.PatternPart{
										{
											PatternElements: []*cypher.PatternElement{
												{
													Element: &cypher.NodePattern{
														Variable: &cypher.Variable{Symbol: "n"},
														Properties: cypher.MapLiteral{
															tc.propertyKey: literal,
														},
													},
												},
											},
										},
									},
								},
							},
						},
						Return: &cypher.Return{
							Projection: &cypher.Projection{
								Items: []cypher.Expression{
									&cypher.ProjectionItem{
										Expression: &cypher.Variable{Symbol: "n"},
									},
								},
							},
						},
					},
				},
			}

			// Format the query
			buffer := &bytes.Buffer{}
			emitter := format.NewCypherEmitter(false)
			err := emitter.Write(query, buffer)

			require.Nil(t, err)
			require.Equal(t, tc.expectedQuery, buffer.String())
		})
	}
}
