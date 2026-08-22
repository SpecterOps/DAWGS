package optimize

import (
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// sourceReferenceCollector tracks referenced identifiers and repeated pattern declarations during syntax walking.
type sourceReferenceCollector struct {
	// VisitorHandler supplies cancellation and error propagation for the syntax walk.
	walk.VisitorHandler

	// referencedIdentifiers contains bindings consumed outside their defining pattern declarations.
	referencedIdentifiers map[string]struct{}
	// matchPatternDeclarationRefs counts match-pattern declarations by binding symbol.
	matchPatternDeclarationRefs map[string]int
	// matchPatternDeclarations identifies pattern parts whose variables are declarations rather than reads.
	matchPatternDeclarations map[*cypher.PatternPart]struct{}
	// matchPatternDeclarationDepth tracks nesting beneath the declaration currently being visited.
	matchPatternDeclarationDepth int
}

// fieldRequirementCollector accumulates ordered representation requirements for each Cypher binding.
type fieldRequirementCollector struct {
	// VisitorHandler supplies cancellation and error propagation for the syntax walk.
	walk.VisitorHandler

	// queryPartIndex identifies the query part whose binding uses are being collected.
	queryPartIndex int
	// ordinal orders binding uses in traversal order.
	ordinal int
	// patternDepth tracks whether the visitor is currently inside a pattern declaration.
	patternDepth int
	// propertyDepth tracks nested property lookups so their base binding is classified once.
	propertyDepth int
	// functionStack identifies the function consuming a visited expression.
	functionStack []*cypher.FunctionInvocation
	// bindingKinds maps each symbol to its path, relationship, or node representation.
	bindingKinds map[string]string
	// patternUses counts pattern occurrences of each binding.
	patternUses map[string]int
	// decisions accumulates representation requirements by binding symbol.
	decisions map[string]*FieldRequirementDecision
}

// newFieldRequirementCollector initializes requirement tracking for one query part.
func newFieldRequirementCollector(queryPartIndex int) *fieldRequirementCollector {
	return &fieldRequirementCollector{
		VisitorHandler: walk.NewCancelableErrorHandler(),
		queryPartIndex: queryPartIndex,
		bindingKinds:   map[string]string{},
		patternUses:    map[string]int{},
		decisions:      map[string]*FieldRequirementDecision{},
	}
}

// add records one ordered use and merges its required fields into the binding decision.
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

// patternVariableSymbol returns a pattern variable's symbol or an empty string when no variable is present.
func patternVariableSymbol(variable *cypher.Variable) string {
	if variable == nil {
		return ""
	}
	return variable.Symbol
}

// addFullBinding records the complete representation required for a path, relationship, or node binding.
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

// addGreedyProjectionBindings marks every visible binding for full materialization in deterministic symbol order.
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

// Enter records representation requirements before visiting a syntax node's children.
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

// Visit performs no leaf-specific work because Enter classifies every relevant node.
func (s *fieldRequirementCollector) Visit(cypher.SyntaxNode) {}

// Exit unwinds pattern, property, and function nesting after visiting a syntax node's children.
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

// collectFieldRequirements walks root and returns normalized representation needs for its bindings.
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

// newSourceReferenceCollector initializes empty reference and match-declaration tracking for a syntax walk.
func newSourceReferenceCollector() *sourceReferenceCollector {
	return &sourceReferenceCollector{
		VisitorHandler:              walk.NewCancelableErrorHandler(),
		referencedIdentifiers:       map[string]struct{}{},
		matchPatternDeclarationRefs: map[string]int{},
		matchPatternDeclarations:    map[*cypher.PatternPart]struct{}{},
	}
}

// addVariable records a referenced variable unless it is part of the declaration currently being traversed.
func (s *sourceReferenceCollector) addVariable(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.referencedIdentifiers[variable.Symbol] = struct{}{}
	}
}

// addMatchPatternDeclaration counts a non-empty variable declared inside a pattern expression so repeated declarations can be retained as references.
func (s *sourceReferenceCollector) addMatchPatternDeclaration(variable *cypher.Variable) {
	if variable != nil && variable.Symbol != "" {
		s.matchPatternDeclarationRefs[variable.Symbol] += 1
	}
}

// collectRepeatedMatchPatternDeclarations marks multiply declared match symbols as source references.
func (s *sourceReferenceCollector) collectRepeatedMatchPatternDeclarations() {
	for identifier, numDeclarations := range s.matchPatternDeclarationRefs {
		if numDeclarations > 1 {
			s.referencedIdentifiers[identifier] = struct{}{}
		}
	}
}

// isMatchPatternDeclaration reports whether node is a variable declaration belonging to a match pattern.
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

// collectReferencedSourceIdentifiers returns identifiers used outside a declaring match pattern or declared repeatedly within one.
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

// referencesSourceIdentifier reports whether references contains symbol or the wildcard source marker.
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
