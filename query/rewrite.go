package query

import (
	"strconv"

	"github.com/specterops/dawgs/cypher/models/walk"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

type ParameterRewriter struct {
	walk.Visitor[cypher.SyntaxNode]

	Parameters     map[string]any
	parameterIndex int
}

func NewParameterRewriter() *ParameterRewriter {
	return &ParameterRewriter{
		Visitor:        walk.NewVisitor[cypher.SyntaxNode](),
		Parameters:     map[string]any{},
		parameterIndex: 0,
	}
}

func (s *ParameterRewriter) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Parameter:
		var (
			nextParameterIndex    = s.parameterIndex
			nextParameterIndexStr = "p" + strconv.Itoa(nextParameterIndex)
		)

		// Increment the parameter index first
		s.parameterIndex++

		// Record the parameter in our map and then bind the symbol in the model
		s.Parameters[nextParameterIndexStr] = typedNode.Value
		typedNode.Symbol = nextParameterIndexStr
	}
}
