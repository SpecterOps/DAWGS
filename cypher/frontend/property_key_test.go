package frontend_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/stretchr/testify/require"
)

// TestParsePropertyLookupStoresRawPropertyKeyNames verifies that lookup tokens are decoded before storage in the AST.
func TestParsePropertyLookupStoresRawPropertyKeyNames(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "RETURN n.match, n.`a-aaa`, n.`has``tick`, n.`   `")
	require.NoError(t, err)

	var symbols []string
	err = walk.CypherStructural(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if propertyLookup, typeOK := node.(*cypher.PropertyLookup); typeOK {
			symbols = append(symbols, propertyLookup.Symbol)
		}
	}))
	require.NoError(t, err)

	require.Equal(t, []string{"match", "a-aaa", "has`tick", "   "}, symbols)
}

// TestParsePropertyLookupStoresQuotePropertyKeyNames verifies that quote characters survive property-key parsing unchanged.
func TestParsePropertyLookupStoresQuotePropertyKeyNames(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "RETURN n.`'`, n.`\"`")
	require.NoError(t, err)

	var symbols []string
	err = walk.CypherStructural(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if propertyLookup, typeOK := node.(*cypher.PropertyLookup); typeOK {
			symbols = append(symbols, propertyLookup.Symbol)
		}
	}))
	require.NoError(t, err)

	require.Equal(t, []string{"'", "\""}, symbols)
}

// TestParsePropertyLookupStoresUnicodePropertyKeyNames verifies the Unicode classes accepted in raw property keys.
func TestParsePropertyLookupStoresUnicodePropertyKeyNames(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "RETURN n.\u2118, n.a\u00b7, n.a\u0301, n.a\u093e, n.a$, n.`a\u20dd`")
	require.NoError(t, err)

	var symbols []string
	err = walk.CypherStructural(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if propertyLookup, typeOK := node.(*cypher.PropertyLookup); typeOK {
			symbols = append(symbols, propertyLookup.Symbol)
		}
	}))
	require.NoError(t, err)

	require.Equal(t, []string{"\u2118", "a\u00b7", "a\u0301", "a\u093e", "a$", "a\u20dd"}, symbols)
}

// TestParseMapLiteralStoresRawPropertyKeyNames verifies that map keys are decoded before storage in the AST.
func TestParseMapLiteralStoresRawPropertyKeyNames(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "RETURN {match: 1, `a-aaa`: 2, `has``tick`: 3, ``: 4, `   `: 5}")
	require.NoError(t, err)

	var keys []string
	err = walk.CypherStructural(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if mapItem, typeOK := node.(*cypher.MapItem); typeOK {
			keys = append(keys, mapItem.Key)
		}
	}))
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"match", "a-aaa", "has`tick", "", "   "}, keys)
}

// TestParseMapLiteralStoresQuotePropertyKeyNames verifies that quote characters survive map-key parsing unchanged.
func TestParseMapLiteralStoresQuotePropertyKeyNames(t *testing.T) {
	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), "RETURN {`'`: 1, `\"`: 2}")
	require.NoError(t, err)

	var keys []string
	err = walk.CypherStructural(regularQuery, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, _ walk.VisitorHandler) {
		if mapItem, typeOK := node.(*cypher.MapItem); typeOK {
			keys = append(keys, mapItem.Key)
		}
	}))
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"'", "\""}, keys)
}

// TestParseRejectsEmptyPropertyKeyNames verifies that empty escaped keys are rejected in every property-key position.
func TestParseRejectsEmptyPropertyKeyNames(t *testing.T) {
	testCases := []struct {
		// name labels the property-key syntax under test.
		name string
		// query contains an empty escaped key in the named syntax position.
		query string
	}{
		{name: "property lookup", query: "RETURN n.``"},
		{name: "set property", query: "MATCH (n) SET n.`` = 'value'"},
		{name: "remove property", query: "MATCH (n) REMOVE n.``"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := frontend.ParseCypher(frontend.NewContext(), testCase.query)
			require.ErrorContains(t, err, cypher.ErrEmptyPropertyKeyName.Error())
		})
	}
}
