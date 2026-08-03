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
		{name: "empty", input: "", expected: false},
		{name: "dash", input: "a-aaa", expected: false},
		{name: "starts digit", input: "1name", expected: false},
		{name: "literal backtick", input: "has`tick", expected: false},
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
		{name: "dash", input: "a-aaa", expected: "`a-aaa`"},
		{name: "embedded backtick", input: "has`tick", expected: "`has``tick`"},
		{name: "starts backtick", input: "`starts-tick", expected: "```starts-tick`"},
		{name: "wrapped backticks", input: "`super-wrapped`", expected: "```super-wrapped```"},
		{name: "single backtick", input: "`", expected: "````"},
		{name: "empty", input: "", expected: "``"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cypher.EscapePropertyKeyName(testCase.input))
		})
	}
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
		{name: "empty", input: "``", expected: ""},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, cypher.UnescapePropertyKeyName(testCase.input))
		})
	}
}
