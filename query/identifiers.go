package query

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
)

func Variable(name string) *cypher.Variable {
	return &cypher.Variable{
		Symbol: name,
	}
}

func Identity(entity cypher.Expression) *cypher.FunctionInvocation {
	return &cypher.FunctionInvocation{
		Name:      "id",
		Arguments: []cypher.Expression{entity},
	}
}

const (
	PathSymbol      = "p"
	NodeSymbol      = "n"
	EdgeSymbol      = "r"
	EdgeStartSymbol = "s"
	EdgeEndSymbol   = "e"
)

func Node() *cypher.Variable {
	return Variable(NodeSymbol)
}

func NodeID() *cypher.FunctionInvocation {
	return Identity(Node())
}

func Relationship() *cypher.Variable {
	return Variable(EdgeSymbol)
}

func RelationshipID() *cypher.FunctionInvocation {
	return Identity(Relationship())
}

func Start() *cypher.Variable {
	return Variable(EdgeStartSymbol)
}

func StartID() *cypher.FunctionInvocation {
	return Identity(Start())
}

func End() *cypher.Variable {
	return Variable(EdgeEndSymbol)
}

func EndID() *cypher.FunctionInvocation {
	return Identity(End())
}

func Path() *cypher.Variable {
	return Variable(PathSymbol)
}
