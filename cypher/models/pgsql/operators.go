package pgsql

import "slices"

type Operator string

func (s Operator) IsIn(others ...Operator) bool {
	for _, other := range others {
		if s == other {
			return true
		}
	}

	return false
}

func (s Operator) AsExpression() Expression {
	return s
}

func (s Operator) String() string {
	return string(s)
}

func (s Operator) NodeType() string {
	return "operator"
}

func OperatorIsIn(operator Expression, matchers ...Expression) bool {
	return slices.Contains(matchers, operator)
}

func OperatorIsBoolean(operator Expression) bool {
	return OperatorIsIn(operator,
		OperatorAnd,
		OperatorOr,
		OperatorNot,
		OperatorEquals,
		OperatorNotEquals,
		OperatorGreaterThan,
		OperatorGreaterThanOrEqualTo,
		OperatorLessThan,
		OperatorLessThanOrEqualTo)
}

func OperatorIsPropertyLookup(operator Expression) bool {
	return OperatorIsIn(operator,
		OperatorPropertyLookup,
		OperatorJSONField,
		OperatorJSONTextField,
	)
}

func OperatorIsComparator(operator Expression) bool {
	return OperatorIsIn(operator,
		OperatorEquals, OperatorNotEquals, OperatorGreaterThan, OperatorGreaterThanOrEqualTo, OperatorLessThan,
		OperatorLessThanOrEqualTo, OperatorArrayOverlap, OperatorLike, OperatorILike, OperatorPGArrayOverlap,
		OperatorRegexMatch, OperatorSimilarTo, OperatorPGArrayLHSContainsRHS)
}

const (
	UnsetOperator                 Operator = ""
	OperatorUnion                 Operator = "union"
	OperatorConcatenate           Operator = "||"
	OperatorArrayOverlap          Operator = "&&"
	OperatorEquals                Operator = "="
	OperatorNotEquals             Operator = "!="
	OperatorGreaterThan           Operator = ">"
	OperatorGreaterThanOrEqualTo  Operator = ">="
	OperatorLessThan              Operator = "<"
	OperatorLessThanOrEqualTo     Operator = "<="
	OperatorLike                  Operator = "like"
	OperatorILike                 Operator = "ilike"
	OperatorPGArrayOverlap        Operator = "operator (pg_catalog.&&)"
	OperatorPGArrayLHSContainsRHS Operator = "operator (pg_catalog.@>)"
	OperatorAnd                   Operator = "and"
	OperatorOr                    Operator = "or"
	OperatorNot                   Operator = "not"
	OperatorJSONBFieldExists      Operator = "?"
	OperatorJSONField             Operator = "->"
	OperatorJSONTextField         Operator = "->>"
	OperatorAdd                   Operator = "+"
	OperatorSubtract              Operator = "-"
	OperatorMultiply              Operator = "*"
	OperatorDivide                Operator = "/"
	OperatorIn                    Operator = "in"
	OperatorIs                    Operator = "is"
	OperatorIsNot                 Operator = "is not"
	OperatorSimilarTo             Operator = "similar to"
	OperatorRegexMatch            Operator = "~"
	OperatorAssignment            Operator = "="
	OperatorAdditionAssignment    Operator = "+="

	OperatorCypherRegexMatch Operator = "=~"
	OperatorCypherStartsWith Operator = "starts with"
	OperatorCypherContains   Operator = "contains"
	OperatorCypherEndsWith   Operator = "ends with"
	OperatorCypherNotEquals  Operator = "<>"

	OperatorPropertyLookup Operator = "property_lookup"
	OperatorKindAssignment Operator = "kind_assignment"
)
