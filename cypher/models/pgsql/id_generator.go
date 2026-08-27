package pgsql

import (
	"strconv"
)

// IdentifierGenerator is a map that creates a unique identifier for each call with a given
// data type. This ensures that renamed identifiers in queries do not conflict with each other.
type IdentifierGenerator map[DataType]int

func (s IdentifierGenerator) NewIdentifier(dataType DataType) (Identifier, error) {
	var prefixStr string

	switch dataType {
	case ExpansionPattern:
		prefixStr = "ex"
	case ExpansionPath:
		prefixStr = "ep"
	case PathComposite:
		prefixStr = "pc"
	case NodeComposite:
		prefixStr = "n"
	case EdgeComposite:
		prefixStr = "e"
	case PathEdge:
		dataType = EdgeComposite
		prefixStr = "e"
	case Scope:
		prefixStr = "s"
	case ParameterIdentifier:
		prefixStr = "pi"
	default:
		// Make this data type the unknown generic
		dataType = UnknownDataType
		prefixStr = "i"
	}

	var (
		nextID    = s[dataType]
		nextIDStr = strconv.Itoa(nextID)
	)

	// Increment the ID
	s[dataType] = nextID + 1

	return Identifier(prefixStr + nextIDStr), nil
}

func NewIdentifierGenerator() IdentifierGenerator {
	return IdentifierGenerator{}
}
