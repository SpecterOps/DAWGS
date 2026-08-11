package optimize

import (
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

type sourceReferenceCollector struct {
	walk.VisitorHandler

	referencedIdentifiers        map[string]struct{}
	matchPatternDeclarationRefs  map[string]int
	matchPatternDeclarations     map[*cypher.PatternPart]struct{}
	matchPatternDeclarationDepth int
}

type fieldRequirementCollector struct {
	walk.VisitorHandler

	queryPartIndex int
	ordinal        int
	patternDepth   int
	propertyDepth  int
	functionStack  []*cypher.FunctionInvocation
	bindingKinds   map[string]string
	patternUses    map[string]int
	decisions      map[string]*FieldRequirementDecision
}

func newFieldRequirementCollector(queryPartIndex int) *fieldRequirementCollector {
	return &fieldRequirementCollector{
		VisitorHandler: walk.NewCancelableErrorHandler(),
		queryPartIndex: queryPartIndex,
		bindingKinds:   map[string]string{},
		patternUses:    map[string]int{},
		decisions:      map[string]*FieldRequirementDecision{},
	}
}

func (s *fieldRequirementCollector) add(symbol string, internal bool, fields ...FieldRequirement) {
	if symbol == "" {
		return
	}

	s.ordinal++
	decision, found := s.decisions[symbol]
	if !found {
		decision = &FieldRequirementDecision{
			QueryPartIndex: s.queryPartIndex,
			Symbol:         symbol,
		}
		s.decisions[symbol] = decision
	}

	useFields := append([]FieldRequirement(nil), fields...)
	decision.Uses = append(decision.Uses, FieldRequirementUse{
		Ordinal:  s.ordinal,
		Fields:   useFields,
		Internal: internal,
	})
	decision.LastUse = s.ordinal

	present := make(map[FieldRequirement]struct{}, len(decision.Fields))
	for _, field := range decision.Fields {
		present[field] = struct{}{}
	}
	for _, field := range fields {
		if _, found := present[field]; !found {
			decision.Fields = append(decision.Fields, field)
			present[field] = struct{}{}
		}
	}
}

func patternVariableSymbol(variable *cypher.Variable) string {
	if variable == nil {
		return ""
	}
	return variable.Symbol
}

func (s *fieldRequirementCollector) addFullBinding(symbol, kind string) {
	switch kind {
	case "path":
		s.add(symbol, false, FieldRequirementFullPath)
	case "relationship":
		s.add(symbol, false, FieldRequirementFullEntity, FieldRequirementRelationshipIDs)
	default:
		s.add(symbol, false, FieldRequirementFullEntity)
	}
}

func (s *fieldRequirementCollector) addGreedyProjectionBindings() {
	symbols := make([]string, 0, len(s.bindingKinds))
	for symbol := range s.bindingKinds {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	for _, symbol := range symbols {
		s.addFullBinding(symbol, s.bindingKinds[symbol])
	}
}

func (s *fieldRequirementCollector) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.PatternPart:
		s.patternDepth++
		if symbol := patternVariableSymbol(typedNode.Variable); symbol != "" {
			s.bindingKinds[symbol] = "path"
			s.add(symbol, true, FieldRequirementOrderedPathEdgeIDs)
		}

	case *cypher.NodePattern:
		if symbol := patternVariableSymbol(typedNode.Variable); symbol != "" {
			s.bindingKinds[symbol] = "node"
			s.patternUses[symbol]++
			if s.patternUses[symbol] > 1 {
				// Reused pattern bindings are consumed by bound-endpoint joins.
				// Those joins still expect the entity representation; scalar-ID
				// rehydration is a separate lowering capability.
				s.add(symbol, true, FieldRequirementFullEntity)
			}
			if len(typedNode.Kinds) > 0 {
				s.add(symbol, true, FieldRequirementEntityID, FieldRequirementKinds)
			}
			if typedNode.Properties != nil {
				s.add(symbol, true, FieldRequirementEntityID, FieldRequirementProperties)
			}
		}

	case *cypher.RelationshipPattern:
		if symbol := patternVariableSymbol(typedNode.Variable); symbol != "" {
			s.bindingKinds[symbol] = "relationship"
			s.patternUses[symbol]++
			if s.patternUses[symbol] > 1 {
				s.add(symbol, true, FieldRequirementFullEntity)
			}
			s.add(symbol, true, FieldRequirementRelationshipIDs)
			if len(typedNode.Kinds) > 0 {
				s.add(symbol, true, FieldRequirementKinds)
			}
			if typedNode.Properties != nil {
				s.add(symbol, true, FieldRequirementProperties)
			}
		}

	case *cypher.PropertyLookup:
		s.propertyDepth++

	case *cypher.FunctionInvocation:
		s.functionStack = append(s.functionStack, typedNode)

	case *cypher.Variable:
		if s.patternDepth > 0 {
			return
		}
		if typedNode.Symbol == cypher.TokenLiteralAsterisk {
			s.addGreedyProjectionBindings()
			return
		}

		if s.propertyDepth > 0 {
			s.add(typedNode.Symbol, false, FieldRequirementEntityID, FieldRequirementProperties)
			return
		}

		if len(s.functionStack) > 0 {
			switch strings.ToLower(s.functionStack[len(s.functionStack)-1].Name) {
			case cypher.IdentityFunction:
				s.add(typedNode.Symbol, false, FieldRequirementEntityID)
				return
			case cypher.NodeLabelsFunction, cypher.EdgeTypeFunction:
				s.add(typedNode.Symbol, false, FieldRequirementKinds)
				return
			case cypher.PathLengthFunction:
				s.add(typedNode.Symbol, false, FieldRequirementOrderedPathEdgeIDs)
				return
			case cypher.NodesFunction, cypher.RelationshipsFunction:
				s.add(typedNode.Symbol, false, FieldRequirementFullPath)
				return
			}
		}

		s.addFullBinding(typedNode.Symbol, s.bindingKinds[typedNode.Symbol])
	}
}

func (s *fieldRequirementCollector) Visit(cypher.SyntaxNode) {}

func (s *fieldRequirementCollector) Exit(node cypher.SyntaxNode) {
	switch node.(type) {
	case *cypher.PatternPart:
		s.patternDepth--
	case *cypher.PropertyLookup:
		s.propertyDepth--
	case *cypher.FunctionInvocation:
		s.functionStack = s.functionStack[:len(s.functionStack)-1]
	}
}

func collectFieldRequirements(queryPartIndex int, root cypher.SyntaxNode) ([]FieldRequirementDecision, error) {
	if root == nil {
		return nil, nil
	}

	collector := newFieldRequirementCollector(queryPartIndex)
	if err := walk.Cypher(root, collector); err != nil {
		return nil, err
	}

	symbols := make([]string, 0, len(collector.decisions))
	for symbol := range collector.decisions {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	decisions := make([]FieldRequirementDecision, 0, len(symbols))
	for _, symbol := range symbols {
		decisions = append(decisions, *collector.decisions[symbol])
	}
	return decisions, nil
}

func newSourceReferenceCollector() *sourceReferenceCollector {
	return &sourceReferenceCollector{
		VisitorHandler:              walk.NewCancelableErrorHandler(),
		referencedIdentifiers:       map[string]struct{}{},
		matchPatternDeclarationRefs: map[string]int{},
		matchPatternDeclarations:    map[*cypher.PatternPart]struct{}{},
	}
}

func (s *sourceReferenceCollector) addVariable(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.referencedIdentifiers[variable.Symbol] = struct{}{}
	}
}

func (s *sourceReferenceCollector) addMatchPatternDeclaration(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.matchPatternDeclarationRefs[variable.Symbol] += 1
	}
}

func (s *sourceReferenceCollector) collectRepeatedMatchPatternDeclarations() {
	for identifier, numDeclarations := range s.matchPatternDeclarationRefs {
		if numDeclarations > 1 {
			s.referencedIdentifiers[identifier] = struct{}{}
		}
	}
}

func (s *sourceReferenceCollector) isMatchPatternDeclaration(patternPart *cypher.PatternPart) bool {
	_, isDeclaration := s.matchPatternDeclarations[patternPart]
	return isDeclaration
}

func (s *sourceReferenceCollector) Enter(node cypher.SyntaxNode) {
	switch typedNode := node.(type) {
	case *cypher.Match:
		for _, patternPart := range typedNode.Pattern {
			s.matchPatternDeclarations[patternPart] = struct{}{}
		}

	case *cypher.PatternPart:
		if s.isMatchPatternDeclaration(typedNode) {
			s.addMatchPatternDeclaration(typedNode.Variable)
			s.matchPatternDeclarationDepth += 1
		} else {
			s.addVariable(typedNode.Variable)
		}

	case *cypher.NodePattern:
		if s.matchPatternDeclarationDepth == 0 {
			s.addVariable(typedNode.Variable)
		} else {
			s.addMatchPatternDeclaration(typedNode.Variable)
		}

	case *cypher.RelationshipPattern:
		if s.matchPatternDeclarationDepth == 0 {
			s.addVariable(typedNode.Variable)
		} else {
			s.addMatchPatternDeclaration(typedNode.Variable)
		}

	case *cypher.PropertyLookup:
		if variable, isVariable := typedNode.Atom.(*cypher.Variable); isVariable {
			s.addVariable(variable)
		}

	case *cypher.Variable:
		if s.matchPatternDeclarationDepth == 0 {
			s.addVariable(typedNode)
		}
	}
}

func (s *sourceReferenceCollector) Visit(cypher.SyntaxNode) {}

func (s *sourceReferenceCollector) Exit(node cypher.SyntaxNode) {
	if patternPart, isPatternPart := node.(*cypher.PatternPart); isPatternPart && s.isMatchPatternDeclaration(patternPart) {
		s.matchPatternDeclarationDepth -= 1
	}
}

func collectReferencedSourceIdentifiers(root cypher.SyntaxNode) (map[string]struct{}, error) {
	if root == nil {
		return map[string]struct{}{}, nil
	}

	collector := newSourceReferenceCollector()
	if err := walk.Cypher(root, collector); err != nil {
		return nil, err
	}

	collector.collectRepeatedMatchPatternDeclarations()
	return collector.referencedIdentifiers, nil
}

func referencesSourceIdentifier(references map[string]struct{}, symbol string) bool {
	if _, referencesAll := references[cypher.TokenLiteralAsterisk]; referencesAll {
		return true
	}

	if symbol == "" {
		return false
	}

	_, referenced := references[symbol]
	return referenced
}
