package cypher_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

func TestCanEmitBarePropertyKeyName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{name: "simple", input: "name", expected: true},
		{name: "underscore", input: "object_id", expected: true},
		{name: "reserved word allowed in property key position", input: "match", expected: true},
		{name: "other id start", input: "\u2118", expected: true},
		{name: "other id continue", input: "a\u00b7", expected: true},
		{name: "nonspacing mark part", input: "a\u0301", expected: true},
		{name: "spacing mark part", input: "a\u093e", expected: true},
		{name: "currency symbol part", input: "a$", expected: true},
		{name: "empty", input: "", expected: false},
		{name: "dash", input: "a-aaa", expected: false},
		{name: "starts digit", input: "1name", expected: false},
		{name: "starts currency symbol", input: "$a", expected: false},
		{name: "literal backtick", input: "has`tick", expected: false},
		{name: "enclosing mark part", input: "a\u20dd", expected: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cypher.CanEmitBarePropertyKeyName(testCase.input))
		})
	}
}

func TestEscapePropertyKeyName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "simple", input: "name", expected: "name"},
		{name: "reserved word allowed in property key position", input: "match", expected: "match"},
		{name: "other id start", input: "\u2118", expected: "\u2118"},
		{name: "other id continue", input: "a\u00b7", expected: "a\u00b7"},
		{name: "nonspacing mark part", input: "a\u0301", expected: "a\u0301"},
		{name: "spacing mark part", input: "a\u093e", expected: "a\u093e"},
		{name: "currency symbol part", input: "a$", expected: "a$"},
		{name: "enclosing mark part", input: "a\u20dd", expected: "`a\u20dd`"},
		{name: "dash", input: "a-aaa", expected: "`a-aaa`"},
		{name: "embedded backtick", input: "has`tick", expected: "`has``tick`"},
		{name: "starts backtick", input: "`starts-tick", expected: "```starts-tick`"},
		{name: "wrapped backticks", input: "`super-wrapped`", expected: "```super-wrapped```"},
		{name: "single backtick", input: "`", expected: "````"},
		{name: "single quote", input: "'", expected: "`'`"},
		{name: "double quote", input: "\"", expected: "`\"`"},
		{name: "whitespace-only", input: "   ", expected: "`   `"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cypher.EscapePropertyKeyName(testCase.input))
		})
	}
}

func TestValidatePropertyKeyName(t *testing.T) {
	require.NoError(t, cypher.ValidatePropertyKeyName("   "))
	require.ErrorIs(t, cypher.ValidatePropertyKeyName(""), cypher.ErrEmptyPropertyKeyName)
}

func TestUnescapePropertyKeyName(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "simple", input: "name", expected: "name"},
		{name: "dash", input: "`a-aaa`", expected: "a-aaa"},
		{name: "embedded backtick", input: "`has``tick`", expected: "has`tick"},
		{name: "starts backtick", input: "```starts-tick`", expected: "`starts-tick"},
		{name: "wrapped backticks", input: "```super-wrapped```", expected: "`super-wrapped`"},
		{name: "single backtick", input: "````", expected: "`"},
		{name: "single quote", input: "`'`", expected: "'"},
		{name: "double quote", input: "`\"`", expected: "\""},
		{name: "empty", input: "``", expected: ""},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cypher.UnescapePropertyKeyName(testCase.input))
		})
	}
}
