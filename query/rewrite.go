package query

import (
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type ParameterRewriter struct {
	walk.Visitor[cypher.SyntaxNode]

	Parameters     map[string]any
	parameterIndex int
	prefix         string
}

// ParameterNamer deterministically assigns names to parameters without
// mutating the Cypher tree. It is useful to adapters that need a stable query
// identity before deciding whether an owned copy is necessary.
type ParameterNamer struct {
	walk.Visitor[cypher.SyntaxNode]

	Parameters     map[string]any
	Symbols        []string
	parameterIndex int
	prefix         string
}

func NewParameterRewriter() *ParameterRewriter {
	return &ParameterRewriter{
		Visitor:        walk.NewVisitor[cypher.SyntaxNode](),
		Parameters:     map[string]any{},
		parameterIndex: 0,
		prefix:         "p",
	}
}

// NewParameterRewriterWithPrefix returns a rewriter whose generated names are
// scoped to prefix. Adapters use this to distinguish builder-owned values from
// caller supplied Cypher parameters.
func NewParameterRewriterWithPrefix(prefix string) *ParameterRewriter {
	rewriter := NewParameterRewriter()
	rewriter.prefix = prefix
	return rewriter
}

// NewParameterNamerWithPrefix returns a read-only parameter namer whose
// generated names are scoped to prefix.
func NewParameterNamerWithPrefix(prefix string) *ParameterNamer {
	return &ParameterNamer{
		Visitor:        walk.NewVisitor[cypher.SyntaxNode](),
		Parameters:     map[string]any{},
		Symbols:        []string{},
		parameterIndex: 0,
		prefix:         prefix,
	}
}

func (s *ParameterRewriter) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Parameter:
		var (
			nextParameterIndex    = s.parameterIndex
			nextParameterIndexStr = s.prefix + strconv.Itoa(nextParameterIndex)
		)

		// Increment the parameter index first
		s.parameterIndex++

		// Record the parameter in our map and then bind the symbol in the model
		s.Parameters[nextParameterIndexStr] = typedNode.Value
		typedNode.Symbol = nextParameterIndexStr
	}
}

func (s *ParameterNamer) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Parameter:
		name := s.prefix + strconv.Itoa(s.parameterIndex)
		s.parameterIndex++
		s.Parameters[name] = typedNode.Value
		s.Symbols = append(s.Symbols, name)
	}
}
