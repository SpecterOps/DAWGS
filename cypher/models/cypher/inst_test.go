package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validateParsing(t *testing.T, expected Operator, operatorStr string) {
	parsed, err := ParseOperator(operatorStr)

	require.Nil(t, err)
	require.Equal(t, expected, parsed)
}

func TestParseOperator(t *testing.T) {
	validateParsing(t, OperatorAdd, "+")
	validateParsing(t, OperatorSubtract, "-")
	validateParsing(t, OperatorMultiply, "*")
	validateParsing(t, OperatorDivide, "/")
	validateParsing(t, OperatorModulo, "%")
	validateParsing(t, OperatorPowerOf, "^")
	validateParsing(t, OperatorEquals, "=")
	validateParsing(t, OperatorRegexMatch, "=~")
	validateParsing(t, OperatorNotEquals, "<>")
	validateParsing(t, OperatorGreaterThan, ">")
	validateParsing(t, OperatorGreaterThanOrEqualTo, ">=")
	validateParsing(t, OperatorLessThan, "<")
	validateParsing(t, OperatorLessThanOrEqualTo, "<=")
	validateParsing(t, OperatorStartsWith, "starts with")
	validateParsing(t, OperatorEndsWith, "ends with")
	validateParsing(t, OperatorContains, "contains")
	validateParsing(t, OperatorIn, "in")
	validateParsing(t, OperatorIs, "is")
	validateParsing(t, OperatorIsNot, "is not")
	validateParsing(t, OperatorNot, "not")

	parsedInvalid, err := ParseOperator("!@#(*)$(*!)(%")

	require.NotNil(t, err)
	require.Equal(t, OperatorInvalid, parsedInvalid)
}
