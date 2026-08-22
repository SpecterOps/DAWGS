package cypher_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/stretchr/testify/require"
)

// TestCanEmitBarePropertyKeyName verifies the Unicode and punctuation rules for unescaped property keys.
func TestCanEmitBarePropertyKeyName(t *testing.T) {
	testCases := []struct {
		// name labels the property-key form under test.
		name string
		// input is the decoded property-key name.
		input string
		// expected indicates whether input may be rendered without backticks.
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

// TestEscapePropertyKeyName verifies canonical quoting and embedded-backtick escaping for property keys.
func TestEscapePropertyKeyName(t *testing.T) {
	testCases := []struct {
		// name labels the property-key form under test.
		name string
		// input is the decoded property-key name.
		input string
		// expected is the canonical property-key token.
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

// TestValidatePropertyKeyName verifies that only empty decoded property-key names are invalid.
func TestValidatePropertyKeyName(t *testing.T) {
	require.NoError(t, cypher.ValidatePropertyKeyName("   "))
	require.ErrorIs(t, cypher.ValidatePropertyKeyName(""), cypher.ErrEmptyPropertyKeyName)
}

// TestUnescapePropertyKeyName verifies decoding of quoted keys and doubled backticks.
func TestUnescapePropertyKeyName(t *testing.T) {
	testCases := []struct {
		// name labels the property-key token under test.
		name string
		// input is the rendered property-key token.
		input string
		// expected is the decoded property-key name.
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
