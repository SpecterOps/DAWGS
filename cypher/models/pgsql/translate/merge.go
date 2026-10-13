package translate

import (
	"fmt"
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

// mergeEntity records boundness at clause entry, independently of the bindings
// introduced while translating the rest of the pattern.
type mergeEntity struct {
	binding         *BoundIdentifier
	bound           bool
	kinds           graph.Kinds
	properties      pgsql.Identifier
	left, right     *BoundIdentifier
	direction       graph.Direction
	matchProperties TranslatedProperties
	keys            []string
	keyColumns      map[string]pgsql.Identifier
	finalKeyColumns map[string]pgsql.Identifier
	keysChanged     bool
}

type mergePlan struct {
	entities    []*mergeEntity
	input       *Frame
	guard       *Frame
	singleInput bool
	inputRow    *BoundIdentifier
	created     *BoundIdentifier
	resultRow   *BoundIdentifier
	path        *BoundIdentifier
}

func mergeAlias(expression pgsql.Expression, id pgsql.Identifier) pgsql.SelectItem {
	return &pgsql.AliasedExpression{Expression: expression, Alias: pgsql.AsOptionalIdentifier(id)}
}

func mergeField(frame *Frame, binding *BoundIdentifier, field pgsql.Identifier) pgsql.Expression {
	return pgsql.RowColumnReference{Identifier: pgsql.CompoundIdentifier{frame.Binding.Identifier, binding.Identifier}, Column: field}
}

func mergeColumns(dataType pgsql.DataType) []pgsql.Identifier {
	if dataType == pgsql.NodeComposite {
		return []pgsql.Identifier{pgsql.ColumnID, pgsql.ColumnKindIDs, pgsql.ColumnProperties}
	}
	return []pgsql.Identifier{pgsql.ColumnID, pgsql.ColumnStartID, pgsql.ColumnEndID, pgsql.ColumnKindID, pgsql.ColumnProperties}
}

func (s *Translator) flushCollectedMutations() error {
	part := s.query.CurrentPart()
	if part.mutations.Creations.Len() == 0 && part.mutations.EdgeCreations.Len() == 0 && part.mutations.Updates.Len() == 0 {
		return nil
	}
	count := part.numUpdatingClauses
	part.numUpdatingClauses = 1
	err := s.translateUpdates()
	part.numUpdatingClauses = count
	part.mutations = NewMutations()
	return err
}

func (s *Translator) flushMerge() error {
	if s.pendingMerge == nil {
		return nil
	}
	merge := s.pendingMerge
	s.pendingMerge = nil
	return s.translateMerge(merge)
}

// mergeStage carries a pipeline into a new read CTE. Writes are emitted only
// after all branch actions have been folded into candidate composite values.
func (s *Translator) mergeStage(projection pgsql.Projection, from []pgsql.FromClause, where pgsql.Expression) (*Frame, error) {
	frame, err := s.scope.PushFrame()
	if err != nil {
		return nil, err
	}
	s.addCTE(frame, pgsql.Select{Projection: projection, From: from, Where: where})
	for _, id := range frame.Known().Slice() {
		binding, _ := s.scope.Lookup(id)
		binding.MaterializedBy(frame)
	}
	return frame, nil
}

func (s *Translator) mergeCarry(frame *Frame) (pgsql.Projection, error) {
	return buildCarryProjection(s.scope, frame.Known(), frame)
}

func (s *Translator) translateMerge(merge *cypher.Merge) error {
	pattern := merge.PatternPart
	if pattern == nil || len(pattern.PatternElements) == 0 {
		return fmt.Errorf("MERGE requires a pattern")
	}
	if pattern.ShortestPathPattern || pattern.AllShortestPathsPattern {
		return fmt.Errorf("MERGE does not support shortest paths")
	}
	plan := mergePlan{}
	s.query.CurrentPart().containsMerge = true
	entry := s.scope.CurrentFrame()
	sourceFrame := entry
	if entry != nil && entry == s.query.CurrentPart().Frame && !entry.Synthetic {
		sourceFrame = entry.Previous
	}
	var projection pgsql.Projection
	if entry != nil {
		var err error
		projection, err = buildCarryProjection(s.scope, entry.Known(), sourceFrame)
		if err != nil {
			return err
		}
		// Paths can still be represented by their dependencies at clause entry.
		// Build their value before carrying them as columns through the pipeline.
		for idx, identifier := range entry.Known().Slice() {
			binding, _ := s.scope.Lookup(identifier)
			if binding.DataType == pgsql.PathComposite && binding.LastProjection == nil {
				expression, err := expressionForPathComposite(binding, s.scope)
				if err != nil {
					return err
				}
				projection[idx] = mergeAlias(expression, identifier)
			}
		}
	}
	// Current-part UNWIND aliases come from lateral sources, rather than columns
	// of the preceding MATCH/WITH frame.
	for _, clause := range s.query.CurrentPart().unwindClauses {
		for idx, item := range projection {
			if alias, ok := item.(*pgsql.AliasedExpression); ok && alias.Alias.Value == clause.Binding.Identifier {
				projection[idx] = mergeAlias(clause.Binding.Identifier, clause.Binding.Identifier)
			}
		}
	}
	known := pgsql.NewIdentifierSet()
	if entry != nil {
		known = entry.Known()
	}
	entitiesByID := map[pgsql.Identifier]*mergeEntity{}
	for _, element := range pattern.PatternElements {
		var kinds graph.Kinds
		var properties cypher.Expression
		dataType := pgsql.NodeComposite
		direction := graph.DirectionOutbound
		switch typed := element.Element.(type) {
		case *cypher.NodePattern:
			kinds = typed.Kinds
			properties = typed.Properties
		case *cypher.RelationshipPattern:
			dataType = pgsql.EdgeComposite
			kinds = typed.Kinds
			properties = typed.Properties
			direction = typed.Direction
			if len(kinds) != 1 || typed.Range != nil {
				return fmt.Errorf("MERGE relationships require exactly one type and no range")
			}
		default:
			return fmt.Errorf("invalid MERGE pattern element %T", element.Element)
		}
		binding, err := s.bindPatternExpression(element.Element, dataType)
		if err != nil {
			return err
		}
		if binding.Binding.DataType != dataType {
			return fmt.Errorf("invalid MERGE binding %s: expected %s", binding.Binding.Aliased(), dataType)
		}
		bound := known.Contains(binding.Binding.Identifier)
		if (bound || binding.AlreadyBound) && dataType == pgsql.NodeComposite && (len(kinds) > 0 || properties != nil) {
			return fmt.Errorf("MERGE cannot redeclare labels or properties on a declared node %s", binding.Binding.Aliased())
		}
		if dataType == pgsql.EdgeComposite && binding.AlreadyBound {
			return fmt.Errorf("MERGE cannot redeclare a bound relationship")
		}
		entity := &mergeEntity{binding: binding.Binding, bound: bound, kinds: kinds, direction: direction}
		if err := s.translateMergeProperties(properties); err != nil {
			return err
		}
		entity.matchProperties = s.query.CurrentPart().ConsumeProperties()
		// A property value copied from another entity must retain its JSON type.
		// Keep casts used by arithmetic and other typed expressions intact.
		for _, value := range entity.matchProperties.Map {
			if lookup, ok := expressionToPropertyLookupBinaryExpression(value); ok {
				lookup.Operator = pgsql.OperatorJSONField
			}
		}
		props, err := s.buildPropertiesObject(entity.matchProperties)
		if err == nil {
			references, refErr := ExtractSyntaxNodeReferences(props)
			if refErr != nil {
				return refErr
			}
			for _, id := range references.Slice() {
				if definition, ok := s.scope.Lookup(id); ok && definition.DataType.MatchesOneOf(pgsql.NodeComposite, pgsql.EdgeComposite) && !known.Contains(id) {
					return fmt.Errorf("MERGE properties reference an unbound entity %s", definition.Aliased())
				}
			}
		}
		if err != nil {
			return err
		}
		propertyBinding, err := s.scope.DefineNew(pgsql.JSONB)
		if err != nil {
			return err
		}
		entity.properties = propertyBinding.Identifier
		entity.keyColumns = map[string]pgsql.Identifier{}
		entity.finalKeyColumns = map[string]pgsql.Identifier{}
		for key := range entity.matchProperties.Map {
			entity.keys = append(entity.keys, key)
		}
		sort.Strings(entity.keys)
		if entity.matchProperties.Parameter != nil {
			projection = append(projection, mergeAlias(pgsql.FunctionCall{Function: "cypher_merge_properties", Parameters: []pgsql.Expression{mergeJSONValue(props)}, CastType: pgsql.JSONB}, entity.properties))
		} else {
			// Fixed values are validated and carried individually. The creation
			// branch assembles the object only when it needs a property bag.
			for _, key := range entity.keys {
				column, err := s.scope.DefineNew(pgsql.JSONB)
				if err != nil {
					return err
				}
				entity.keyColumns[key] = column.Identifier
				value := mergeJSONValue(entity.matchProperties.Map[key])
				projection = append(projection, mergeAlias(pgsql.FunctionCall{Function: "cypher_merge_value", Parameters: []pgsql.Expression{value}, CastType: pgsql.JSONB}, column.Identifier))
			}
		}
		if dataType == pgsql.NodeComposite {
			if len(plan.entities) > 0 && plan.entities[len(plan.entities)-1].binding.DataType == pgsql.EdgeComposite {
				plan.entities[len(plan.entities)-1].right = entity.binding
			}
		} else {
			if len(plan.entities) == 0 {
				return fmt.Errorf("MERGE relationship is missing a left endpoint")
			}
			entity.left = plan.entities[len(plan.entities)-1].binding
		}
		plan.entities = append(plan.entities, entity)
		entitiesByID[entity.binding.Identifier] = entity
		// Assert before matching so an unknown kind never maps to an empty matcher.
		if _, err := s.kindMapper.AssertKinds(kinds); err != nil {
			return err
		}
	}
	if pattern.Variable != nil {
		if _, bound := s.scope.AliasedLookup(pgsql.Identifier(pattern.Variable.Symbol)); bound {
			return fmt.Errorf("MERGE cannot redeclare a path")
		}
		binding, err := s.scope.DefineNew(pgsql.PathComposite)
		if err != nil {
			return err
		}
		s.scope.Alias(pgsql.Identifier(pattern.Variable.Symbol), binding)
		for _, entity := range plan.entities {
			binding.DependOn(entity.binding)
		}
		plan.path = binding
	}
	row, err := s.scope.DefineNew(pgsql.Int8)
	if err != nil {
		return err
	}
	plan.inputRow = row
	projection = append(projection, mergeAlias(pgsql.FunctionCall{Function: "row_number", Over: &pgsql.Window{}, CastType: pgsql.Int8}, row.Identifier))
	for idx, clause := range s.query.CurrentPart().unwindClauses {
		if array, ok := clause.Expression.(pgsql.ArrayLiteral); ok && len(array.Values) == 0 {
			array.CastType = pgsql.JSONBArray
			s.query.CurrentPart().unwindClauses[idx].Expression = array
		}
	}
	plan.singleInput = (sourceFrame == nil || sourceFrame.Synthetic) && len(s.query.CurrentPart().unwindClauses) == 0
	from := s.createSourceFromClauses(sourceFrame)
	input, err := s.mergeStage(projection, from, nil)
	if err != nil {
		return err
	}
	plan.input = input
	// Clause-entry paths may be visible without having been exported by MATCH.
	// Every carried column is now materialized and available to both branches.
	for _, identifier := range known.Slice() {
		input.Reveal(identifier)
		input.Export(identifier)
		binding, _ := s.scope.Lookup(identifier)
		binding.MaterializedBy(input)
	}
	input.Reveal(row.Identifier)
	input.Export(row.Identifier)
	row.MaterializedBy(input)
	for _, entity := range plan.entities {
		columns := entity.inputColumns()
		for _, column := range columns {
			input.Reveal(column)
			input.Export(column)
			binding, _ := s.scope.Lookup(column)
			binding.MaterializedBy(input)
		}
	}
	s.query.CurrentPart().Model.CommonTableExpressions.Expressions[len(s.query.CurrentPart().Model.CommonTableExpressions.Expressions)-1].Materialized = &pgsql.Materialized{Materialized: true}
	// Match the entire pattern in one read branch. Each table is graph-scoped;
	// repeated node variables reuse the same alias and relationships are unique.
	matchProjection, err := s.mergeCarry(input)
	if err != nil {
		return err
	}
	matchFrom := []pgsql.FromClause{frameReference(input)}
	var constraints pgsql.Expression
	emitted := pgsql.NewIdentifierSet()
	var previousEdges []*BoundIdentifier
	for _, entity := range plan.entities {
		binding := entity.binding
		table := pgsql.TableNode
		if binding.DataType == pgsql.EdgeComposite {
			table = pgsql.TableEdge
		}
		if !emitted.Contains(binding.Identifier) {
			emitted.Add(binding.Identifier)
			matchFrom = append(matchFrom, pgsql.FromClause{Source: pgsql.TableReference{Name: table.AsCompoundIdentifier(), Binding: pgsql.AsOptionalIdentifier(binding.Identifier)}})
			values := []pgsql.Expression{}
			for _, column := range mergeColumns(binding.DataType) {
				values = append(values, pgsql.CompoundIdentifier{binding.Identifier, column})
			}
			if !entity.bound {
				matchProjection = append(matchProjection, mergeAlias(pgsql.CompositeValue{DataType: binding.DataType, Values: values}, binding.Identifier))
			}
			constraints = pgsql.OptionalAnd(constraints, pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnGraphID}, pgsql.OperatorEquals, pgsql.NewLiteral(s.graphID, pgsql.Int4)))
			if entity.bound {
				constraints = pgsql.OptionalAnd(constraints, pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, mergeField(input, binding, pgsql.ColumnID)))
			}
		}
		ids, err := s.kindMapper.AssertKinds(entity.kinds)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			var kindConstraint pgsql.Expression
			if binding.DataType == pgsql.NodeComposite {
				kindConstraint = pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindIDs}, pgsql.OperatorPGArrayLHSContainsRHS, pgsql.NewLiteral(ids, pgsql.Int2Array))
			} else {
				kindConstraint = pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindID}, pgsql.OperatorEquals, pgsql.NewLiteral(ids[0], pgsql.Int2))
			}
			constraints = pgsql.OptionalAnd(constraints, kindConstraint)
		}

		for _, key := range entity.keys {
			lookup := pgsql.NewPropertyLookup(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnProperties}, pgsql.NewLiteral(key, pgsql.Text))
			expected := pgsql.CompoundIdentifier{input.Binding.Identifier, entity.keyColumns[key]}
			// Type eligibility comes from the AST, never the current map contents.
			if _, stringValue := rewriteStringEqualityOperand(entity.matchProperties.Map[key]); stringValue {
				textValue := pgsql.NewBinaryExpression(expected, pgsql.Operator("#>>"), pgsql.NewLiteral([]string{}, pgsql.TextArray))
				constraints = pgsql.OptionalAnd(constraints, buildStringPropertyComparisonPredicate(lookup, textValue, true, pgsql.OperatorEquals))
			} else {
				lookup.Operator = pgsql.OperatorJSONField
				constraints = pgsql.OptionalAnd(constraints, pgsql.NewBinaryExpression(lookup, pgsql.OperatorEquals, expected))
			}
		}
		if entity.matchProperties.Parameter != nil {
			// Dynamic property maps require equality for every field. JSON
			// containment would incorrectly accept array subsets as matches.
			constraints = pgsql.OptionalAnd(constraints, pgsql.ExistsExpression{Negated: true, Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(1, pgsql.Int4)}, From: []pgsql.FromClause{{Source: pgsql.AliasedExpression{Expression: pgsql.FunctionCall{Function: "jsonb_each", Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{input.Binding.Identifier, entity.properties}}}, Alias: pgsql.AsOptionalIdentifier("_merge_property")}}}, Where: pgsql.NewBinaryExpression(pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnProperties}, pgsql.OperatorJSONField, pgsql.CompoundIdentifier{"_merge_property", "key"}), pgsql.Operator("is distinct from"), pgsql.CompoundIdentifier{"_merge_property", "value"})}}}})
		}

		if binding.DataType == pgsql.EdgeComposite {
			if entity.right == nil {
				return fmt.Errorf("MERGE relationship is missing a right endpoint")
			}
			left, err := leftNodeConstraint(binding.Identifier, entity.left.Identifier, entity.direction)
			if err != nil {
				return err
			}
			rightDirection := graph.DirectionInbound
			if entity.direction == graph.DirectionInbound {
				rightDirection = graph.DirectionOutbound
			} else if entity.direction == graph.DirectionBoth {
				rightDirection = graph.DirectionBoth
			}
			right, err := leftNodeConstraint(binding.Identifier, entity.right.Identifier, rightDirection)
			if err != nil {
				return err
			}
			// For undirected patterns pair the endpoints in one direction or the other.
			if entity.direction == graph.DirectionBoth {
				forwardLeft, _ := leftNodeConstraint(binding.Identifier, entity.left.Identifier, graph.DirectionOutbound)
				forwardRight, _ := leftNodeConstraint(binding.Identifier, entity.right.Identifier, graph.DirectionInbound)
				backwardLeft, _ := leftNodeConstraint(binding.Identifier, entity.left.Identifier, graph.DirectionInbound)
				backwardRight, _ := leftNodeConstraint(binding.Identifier, entity.right.Identifier, graph.DirectionOutbound)
				left = pgsql.NewParenthetical(pgsql.NewBinaryExpression(pgsql.OptionalAnd(forwardLeft, forwardRight), pgsql.OperatorOr, pgsql.OptionalAnd(backwardLeft, backwardRight)))
				right = nil
			}
			constraints = pgsql.OptionalAnd(constraints, pgsql.OptionalAnd(left, right))
			for _, other := range previousEdges {
				constraints = pgsql.OptionalAnd(constraints, pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnID}, pgsql.OperatorNotEquals, pgsql.CompoundIdentifier{other.Identifier, pgsql.ColumnID}))
			}
			previousEdges = append(previousEdges, binding)
		}
	}
	match, err := s.mergeStage(matchProjection, matchFrom, constraints)
	if err != nil {
		return err
	}
	for _, entity := range plan.entities {
		match.Reveal(entity.binding.Identifier)
		match.Export(entity.binding.Identifier)
		entity.binding.MaterializedBy(match)
	}
	// Create only the unbound elements, once the complete pattern failed to match.
	s.scope.stack = append(s.scope.stack, input)
	carry, err := s.mergeCarry(input)
	if err != nil {
		return err
	}
	absent := pgsql.ExistsExpression{Negated: true, Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(1, pgsql.Int4)}, From: []pgsql.FromClause{frameReference(match)}, Where: pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{match.Binding.Identifier, row.Identifier}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{input.Binding.Identifier, row.Identifier})}}}}
	create, err := s.mergeStage(carry, []pgsql.FromClause{frameReference(input)}, absent)
	if err != nil {
		return err
	}
	createdIDs := pgsql.NewIdentifierSet()
	creationOrder := []*mergeEntity{}
	for _, entity := range plan.entities {
		if entity.binding.DataType == pgsql.NodeComposite {
			creationOrder = append(creationOrder, entity)
		}
	}
	for _, entity := range plan.entities {
		if entity.binding.DataType == pgsql.EdgeComposite {
			creationOrder = append(creationOrder, entity)
		}
	}
	for _, entity := range creationOrder {
		binding := entity.binding
		if entity.bound || createdIDs.Contains(binding.Identifier) {
			continue
		}
		createdIDs.Add(binding.Identifier)
		projection, err := s.mergeCarry(create)
		if err != nil {
			return err
		}
		table := pgsql.TableNode
		if binding.DataType == pgsql.EdgeComposite {
			table = pgsql.TableEdge
		}
		values := []pgsql.Expression{sequenceValue(table)}
		ids, err := s.kindMapper.AssertKinds(entity.kinds)
		if err != nil {
			return err
		}
		if binding.DataType == pgsql.NodeComposite {
			values = append(values, pgsql.NewLiteral(ids, pgsql.Int2Array))
		} else {
			left, right := entity.left, entity.right
			if entity.direction == graph.DirectionInbound {
				left, right = right, left
			}
			values = append(values, mergeField(create, left, pgsql.ColumnID), mergeField(create, right, pgsql.ColumnID), pgsql.NewLiteral(ids[0], pgsql.Int2))
		}
		// Property columns are private pipeline fields; carry them through creations.
		values = append(values, entity.propertiesAt(create))
		projection = append(projection, mergeAlias(pgsql.CompositeValue{DataType: binding.DataType, Values: values}, binding.Identifier))
		create, err = s.mergeStage(projection, []pgsql.FromClause{frameReference(create)}, nil)
		if err != nil {
			return err
		}
		create.Reveal(binding.Identifier)
		create.Export(binding.Identifier)
		binding.MaterializedBy(create)
	}
	flag, err := s.scope.DefineNew(pgsql.Boolean)
	if err != nil {
		return err
	}
	plan.created = flag
	candidateIDs := match.Known()
	// Map columns explicitly: creation and match preserve every incoming row.
	matchItems := pgsql.Projection{}
	createItems := pgsql.Projection{}
	for _, id := range candidateIDs.Slice() {
		matchItems = append(matchItems, mergeAlias(pgsql.CompoundIdentifier{match.Binding.Identifier, id}, id))
		createItems = append(createItems, mergeAlias(pgsql.CompoundIdentifier{create.Binding.Identifier, id}, id))
	}
	matchItems = append(matchItems, mergeAlias(pgsql.NewLiteral(false, pgsql.Boolean), flag.Identifier))
	createItems = append(createItems, mergeAlias(pgsql.NewLiteral(true, pgsql.Boolean), flag.Identifier))
	candidate, err := s.scope.PushFrame()
	if err != nil {
		return err
	}
	s.addCTE(candidate, pgsql.SetOperation{Operator: pgsql.OperatorUnion, All: true, LOperand: pgsql.Select{Projection: matchItems, From: []pgsql.FromClause{frameReference(match)}}, ROperand: pgsql.Select{Projection: createItems, From: []pgsql.FromClause{frameReference(create)}}})
	candidate.Visible = candidateIDs.Copy().Add(flag.Identifier)
	candidate.Exported = candidate.Visible.Copy()
	for _, id := range candidate.Known().Slice() {
		binding, _ := s.scope.Lookup(id)
		binding.MaterializedBy(candidate)
	}
	resultRow, err := s.scope.DefineNew(pgsql.Int8)
	if err != nil {
		return err
	}
	plan.resultRow = resultRow
	projection, err = s.mergeCarry(candidate)
	if err != nil {
		return err
	}
	projection = append(projection, mergeAlias(pgsql.FunctionCall{Function: "row_number", Over: &pgsql.Window{}, CastType: pgsql.Int8}, resultRow.Identifier))
	candidate, err = s.mergeStage(projection, []pgsql.FromClause{frameReference(candidate)}, nil)
	if err != nil {
		return err
	}
	candidate.Reveal(resultRow.Identifier)
	candidate.Export(resultRow.Identifier)
	resultRow.MaterializedBy(candidate)
	updates := map[pgsql.Identifier]pgsql.Expression{}
	singleNodePattern := len(plan.entities) == 1 && !plan.entities[0].bound && plan.entities[0].binding.DataType == pgsql.NodeComposite
	// Each SET clause is a read projection, preserving clause order without
	// attempting to update the same stored row from sibling write CTEs.
	for _, action := range merge.MergeActions {
		if action.Set == nil || (!action.OnCreate && !action.OnMatch) {
			return fmt.Errorf("invalid MERGE action")
		}
		{
			s.query.CurrentPart().mutations = NewMutations()
			if err := walk.Cypher(action.Set, s); err != nil {
				return err
			}
			var condition pgsql.Expression = pgsql.CompoundIdentifier{candidate.Binding.Identifier, flag.Identifier}
			if action.OnCreate && action.OnMatch {
				condition = pgsql.NewLiteral(true, pgsql.Boolean)
			} else if action.OnMatch {
				condition = &pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: condition}
			}
			patches := map[pgsql.Identifier]pgsql.Identifier{}
			projection, err = s.mergeCarry(candidate)
			if err != nil {
				return err
			}
			for _, update := range s.query.CurrentPart().mutations.Updates.Values() {
				if update.PropertyAssignments.Len() == 0 {
					continue
				}
				column, err := s.scope.DefineNew(pgsql.JSONB)
				if err != nil {
					return err
				}
				patches[update.TargetBinding.Identifier] = column.Identifier
				for _, assignment := range update.PropertyAssignments.Values() {
					if entity := entitiesByID[update.TargetBinding.Identifier]; entity != nil && action.OnCreate {
						if _, exists := entity.keyColumns[assignment.Field]; exists {
							entity.keysChanged = true
						}
					}
				}
				if entity := entitiesByID[update.TargetBinding.Identifier]; entity != nil && entity.keysChanged && len(entity.finalKeyColumns) == 0 {
					// Only creation-key mutations need a separate final tuple.
					for _, key := range entity.keys {
						column, err := s.scope.DefineNew(pgsql.JSONB)
						if err != nil {
							return err
						}
						entity.finalKeyColumns[key] = column.Identifier
						projection = append(projection, mergeAlias(pgsql.CompoundIdentifier{candidate.Binding.Identifier, entity.keyColumns[key]}, column.Identifier))
					}
				}
				patch := mergePropertyPatch(update.PropertyAssignments.Values())
				projection = append(projection, mergeAlias(pgsql.Case{Conditions: []pgsql.Expression{condition}, Then: []pgsql.Expression{patch}, Else: pgsql.FunctionCall{Function: pgsql.FunctionJSONBBuildObject, CastType: pgsql.JSONB}}, column.Identifier))
			}
			if len(patches) > 0 {
				candidate, err = s.mergeStage(projection, []pgsql.FromClause{frameReference(candidate)}, nil)
				if err != nil {
					return err
				}
				s.query.CurrentPart().Model.CommonTableExpressions.Expressions[len(s.query.CurrentPart().Model.CommonTableExpressions.Expressions)-1].Materialized = &pgsql.Materialized{Materialized: true}
				for _, column := range patches {
					candidate.Reveal(column)
					candidate.Export(column)
					binding, _ := s.scope.Lookup(column)
					binding.MaterializedBy(candidate)
				}
				for _, entity := range plan.entities {
					for _, column := range entity.finalKeyColumns {
						candidate.Reveal(column)
						candidate.Export(column)
						binding, _ := s.scope.Lookup(column)
						binding.MaterializedBy(candidate)
					}
				}
				condition = pgsql.CompoundIdentifier{candidate.Binding.Identifier, flag.Identifier}
				if action.OnCreate && action.OnMatch {
					condition = pgsql.NewLiteral(true, pgsql.Boolean)
				} else if action.OnMatch {
					condition = pgsql.NewUnaryExpression(pgsql.OperatorNot, condition)
				}
			}
			projection, err = s.mergeCarry(candidate)
			if err != nil {
				return err
			}
			for _, update := range s.query.CurrentPart().mutations.Updates.Values() {
				binding := update.TargetBinding
				if binding.DataType == pgsql.EdgeComposite && len(update.KindAssignments) > 0 {
					return fmt.Errorf("MERGE SET labels require a node binding")
				}
				if binding.DataType != pgsql.NodeComposite && binding.DataType != pgsql.EdgeComposite {
					return fmt.Errorf("invalid MERGE SET binding %s", binding.Aliased())
				}
				values := []pgsql.Expression{}
				for _, column := range mergeColumns(binding.DataType) {
					var value pgsql.Expression = mergeField(candidate, binding, column)
					switch column {
					case pgsql.ColumnProperties:
						if update.PropertyAssignments.Len() > 0 {
							patch := pgsql.CompoundIdentifier{candidate.Binding.Identifier, patches[binding.Identifier]}
							value = pgsql.FunctionCall{Function: "cypher_apply_property_patch", Parameters: []pgsql.Expression{value, patch}, CastType: pgsql.JSONB}
						}
					case pgsql.ColumnKindIDs:
						if len(update.KindAssignments) > 0 {
							ids, err := s.kindMapper.AssertKinds(update.KindAssignments)
							if err != nil {
								return err
							}
							value = pgsql.FunctionCall{Function: pgsql.FunctionIntArrayUnique, Parameters: []pgsql.Expression{pgsql.FunctionCall{Function: pgsql.FunctionIntArraySort, Parameters: []pgsql.Expression{pgsql.NewBinaryExpression(value, pgsql.OperatorConcatenate, pgsql.NewLiteral(ids, pgsql.Int2Array))}, CastType: pgsql.Int2Array}}, CastType: pgsql.Int2Array}
						}
					}
					values = append(values, value)
				}
				modified := pgsql.CompositeValue{DataType: binding.DataType, Values: values}
				choice := pgsql.Case{Conditions: []pgsql.Expression{condition}, Then: []pgsql.Expression{modified}, Else: pgsql.CompoundIdentifier{candidate.Binding.Identifier, binding.Identifier}}
				for idx, item := range projection {
					if aliased, ok := item.(*pgsql.AliasedExpression); ok && aliased.Alias.Value == binding.Identifier {
						projection[idx] = mergeAlias(choice, binding.Identifier)
					}
				}
				if entity := entitiesByID[binding.Identifier]; entity != nil && patches[binding.Identifier] != "" {
					patch := pgsql.CompoundIdentifier{candidate.Binding.Identifier, patches[binding.Identifier]}
					for _, key := range entity.keys {
						column := entity.finalKeyColumns[key]
						if column == "" {
							continue
						}
						choice := pgsql.Case{Conditions: []pgsql.Expression{pgsql.NewBinaryExpression(patch, pgsql.Operator("?"), pgsql.NewLiteral(key, pgsql.Text))}, Then: []pgsql.Expression{pgsql.NewBinaryExpression(patch, pgsql.OperatorJSONField, pgsql.NewLiteral(key, pgsql.Text))}, Else: pgsql.CompoundIdentifier{candidate.Binding.Identifier, column}}
						for idx, item := range projection {
							if alias, ok := item.(*pgsql.AliasedExpression); ok && alias.Alias.Value == column {
								projection[idx] = mergeAlias(choice, column)
							}
						}
					}
				}
				// Save a frame-independent branch predicate for the native write.
				var writeCondition pgsql.Expression = pgsql.CompoundIdentifier{"_merge_source", flag.Identifier}
				if action.OnCreate && action.OnMatch {
					writeCondition = pgsql.NewLiteral(true, pgsql.Boolean)
				} else if action.OnMatch {
					writeCondition = &pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: writeCondition}
				}
				if existing := updates[binding.Identifier]; existing != nil {
					updates[binding.Identifier] = pgsql.NewParenthetical(pgsql.NewBinaryExpression(existing, pgsql.OperatorOr, writeCondition))
				} else {
					updates[binding.Identifier] = writeCondition
				}
				if entitiesByID[binding.Identifier] == nil {
					entity := &mergeEntity{binding: binding, bound: true}
					plan.entities = append(plan.entities, entity)
					entitiesByID[binding.Identifier] = entity
				}
			}
			for _, column := range patches {
				candidate.Visible.Remove(column)
				candidate.Exported.Remove(column)
				filtered := pgsql.Projection{}
				for _, item := range projection {
					if alias, ok := item.(*pgsql.AliasedExpression); !ok || alias.Alias.Value != column {
						filtered = append(filtered, item)
					}
				}
				projection = filtered
			}
			candidate, err = s.mergeStage(projection, []pgsql.FromClause{frameReference(candidate)}, nil)
			if err != nil {
				return err
			}
			s.query.CurrentPart().Model.CommonTableExpressions.Expressions[len(s.query.CurrentPart().Model.CommonTableExpressions.Expressions)-1].Materialized = &pgsql.Materialized{Materialized: true}
		}
	}
	s.query.CurrentPart().mutations = NewMutations()
	// Validate the complete candidate set before any native write. In particular,
	// different bindings can resolve to the same stored target. Never discard an
	// input action by choosing an arbitrary DISTINCT row.
	candidate, err = s.validateMergeCandidates(candidate, &plan, updates, singleNodePattern)
	if err != nil {
		return err
	}
	// Native MERGE writes each target once. DO NOTHING matches are preserved by
	// the separate candidate branch and never require a dummy UPDATE.
	for _, entity := range plan.entities {
		for _, column := range append(entity.inputColumns(), mapColumns(entity.finalKeyColumns)...) {
			candidate.Visible.Remove(column)
			candidate.Exported.Remove(column)
		}
	}
	finalProjection, err := s.mergeCarry(candidate)
	if err != nil {
		return err
	}
	finalFrom := []pgsql.FromClause{frameReference(candidate)}
	written := pgsql.NewIdentifierSet()
	anchored := false
	for _, entity := range plan.entities {
		binding := entity.binding
		if written.Contains(binding.Identifier) || (entity.bound && updates[binding.Identifier] == nil) {
			continue
		}
		written.Add(binding.Identifier)
		writeFrame, err := s.scope.PushFrame()
		if err != nil {
			return err
		}
		target := pgsql.Identifier("_merge_target")
		sourceBinding := *binding
		sourceBinding.Identifier = binding.Identifier
		sourceFrame := &Frame{Binding: &BoundIdentifier{Identifier: "_merge_source"}}
		table := pgsql.TableNode
		if binding.DataType == pgsql.EdgeComposite {
			table = pgsql.TableEdge
		}
		actions := []pgsql.MergeAction{}
		if condition := updates[binding.Identifier]; condition != nil {
			assignments := []pgsql.Assignment{pgsql.NewBinaryExpression(pgsql.ColumnProperties, pgsql.OperatorAssignment, mergeField(sourceFrame, &sourceBinding, pgsql.ColumnProperties))}
			if binding.DataType == pgsql.NodeComposite {
				assignments = append(assignments, pgsql.NewBinaryExpression(pgsql.ColumnKindIDs, pgsql.OperatorAssignment, mergeField(sourceFrame, &sourceBinding, pgsql.ColumnKindIDs)))
			}
			actions = append(actions, pgsql.MatchedUpdate{Predicate: condition, Assignments: assignments})
		}
		actions = append(actions, pgsql.MergeDoNothing{Matched: true})
		if !entity.bound {
			columns := append([]pgsql.Identifier{pgsql.ColumnGraphID}, mergeColumns(binding.DataType)...)
			values := []pgsql.Expression{pgsql.NewLiteral(s.graphID, pgsql.Int4)}
			for _, column := range mergeColumns(binding.DataType) {
				values = append(values, mergeField(sourceFrame, &sourceBinding, column))
			}
			actions = append(actions, pgsql.UnmatchedAction{Columns: columns, Values: pgsql.Values{Values: values}})
		} else {
			actions = append(actions, pgsql.MergeDoNothing{Matched: false})
		}
		targetValues := []pgsql.Expression{}
		for _, column := range mergeColumns(binding.DataType) {
			targetValues = append(targetValues, pgsql.CompoundIdentifier{target, column})
		}
		native := pgsql.Merge{Into: true, Table: pgsql.TableReference{Name: table.AsCompoundIdentifier(), Binding: pgsql.AsOptionalIdentifier(target)}, Source: pgsql.TableReference{Name: candidate.Binding.Identifier.AsCompoundIdentifier(), Binding: pgsql.AsOptionalIdentifier("_merge_source")}, JoinTarget: pgsql.OptionalAnd(pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{target, pgsql.ColumnID}, pgsql.OperatorEquals, mergeField(sourceFrame, binding, pgsql.ColumnID)), pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{target, pgsql.ColumnGraphID}, pgsql.OperatorEquals, pgsql.NewLiteral(s.graphID, pgsql.Int4))), Actions: actions, Returning: pgsql.Projection{mergeAlias(pgsql.CompoundIdentifier{"_merge_source", resultRow.Identifier}, resultRow.Identifier), mergeAlias(pgsql.CompositeValue{DataType: binding.DataType, Values: targetValues}, binding.Identifier)}}
		var effective pgsql.Expression = updates[binding.Identifier]
		if !entity.bound {
			created := pgsql.CompoundIdentifier{"_merge_source", flag.Identifier}
			if effective == nil {
				effective = created
			} else {
				effective = pgsql.NewParenthetical(pgsql.NewBinaryExpression(effective, pgsql.OperatorOr, created))
			}
		}
		writeProjection := pgsql.Projection{mergeAlias(pgsql.CompoundIdentifier{"_merge_source", binding.Identifier}, binding.Identifier), mergeAlias(pgsql.CompoundIdentifier{"_merge_source", resultRow.Identifier}, resultRow.Identifier), mergeAlias(pgsql.CompoundIdentifier{"_merge_source", flag.Identifier}, flag.Identifier)}
		var writeSource pgsql.SetExpression = pgsql.Select{Projection: writeProjection, From: []pgsql.FromClause{mergeTable(candidate, "_merge_source")}, Where: effective}
		if !entity.bound && !anchored {
			// An unbound entity's INSERT action prevents PostgreSQL from pruning
			// the sentinel. Its DO NOTHING predicate demands the shared guard.
			anchored = true
			marker := pgsql.Identifier("_merge_anchor")
			writeProjection = append(writeProjection, mergeAlias(pgsql.NewLiteral(false, pgsql.Boolean), marker))
			real := pgsql.Select{Projection: writeProjection, From: []pgsql.FromClause{mergeTable(candidate, "_merge_source")}, Where: effective}
			sentinel := pgsql.Select{Projection: pgsql.Projection{mergeAlias(pgsql.Literal{Null: true, CastType: binding.DataType}, binding.Identifier), mergeAlias(pgsql.Literal{Null: true, CastType: pgsql.Int8}, resultRow.Identifier), mergeAlias(pgsql.NewLiteral(false, pgsql.Boolean), flag.Identifier), mergeAlias(pgsql.CompoundIdentifier{plan.guard.Binding.Identifier, "_merge_valid"}, marker)}, From: []pgsql.FromClause{frameReference(plan.guard)}}
			writeSource = pgsql.SetOperation{Operator: pgsql.OperatorUnion, All: true, LOperand: real, ROperand: sentinel}
			native.Actions = append([]pgsql.MergeAction{pgsql.MergeDoNothing{Matched: false, Predicate: pgsql.CompoundIdentifier{"_merge_source", marker}}}, native.Actions...)
		}
		native.SourceQuery = &pgsql.Subquery{Query: pgsql.Query{Body: writeSource}}
		s.addCTE(writeFrame, native)
		finalFrom[0].Joins = append(finalFrom[0].Joins, pgsql.Join{Table: writeFrame.Binding.Identifier, JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeLeftOuter, Constraint: pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{writeFrame.Binding.Identifier, resultRow.Identifier}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{candidate.Binding.Identifier, resultRow.Identifier})}})
		for idx, item := range finalProjection {
			if aliased, ok := item.(*pgsql.AliasedExpression); ok && aliased.Alias.Value == binding.Identifier {
				finalProjection[idx] = mergeAlias(pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{writeFrame.Binding.Identifier, binding.Identifier}, pgsql.CompoundIdentifier{candidate.Binding.Identifier, binding.Identifier}}, CastType: binding.DataType}, binding.Identifier)
			}
		}
	}
	output, err := s.mergeStage(finalProjection, finalFrom, nil)
	if err != nil {
		return err
	}
	// Private bookkeeping never becomes a user-visible RETURN * binding.
	output.Visible.Remove(row.Identifier).Remove(resultRow.Identifier).Remove(flag.Identifier)
	output.Exported.Remove(row.Identifier).Remove(resultRow.Identifier).Remove(flag.Identifier)
	for _, entity := range plan.entities {
		output.Visible.Remove(entity.properties)
		output.Exported.Remove(entity.properties)
	}
	if plan.path != nil {
		output.Reveal(plan.path.Identifier)
		output.Export(plan.path.Identifier)
	}
	return nil
}

func (s *Translator) translateMergeProperties(properties cypher.Expression) error {
	s.query.CurrentPart().properties = NewTranslatedProperties()
	if properties == nil {
		return nil
	}
	if err := walk.Cypher(properties, s); err != nil {
		return err
	}
	for _, binding := range s.scope.definitions {
		if binding.Parameter != nil && !binding.Parameter.CastType.IsKnown() && s.translation.Parameters[binding.Parameter.Identifier.String()] == nil {
			binding.Parameter.CastType = pgsql.JSONB
		}
	}
	return nil
}

// mergePropertyPatch keeps each jsonb_build_object call within PostgreSQL's
// default 100-argument limit. Concatenate patches before applying them so every
// RHS still observes the incoming clause frame and the base bag is updated once.
func mergePropertyPatch(assignments []PropertyAssignment) pgsql.Expression {
	const propertiesPerObject = 50
	var patch pgsql.Expression
	for start := 0; start < len(assignments); start += propertiesPerObject {
		object := pgsql.FunctionCall{Function: pgsql.FunctionJSONBBuildObject, CastType: pgsql.JSONB}
		for _, assignment := range assignments[start:min(start+propertiesPerObject, len(assignments))] {
			object.Parameters = append(object.Parameters, pgsql.NewLiteral(assignment.Field, pgsql.Text), mergeJSONValue(assignment.ValueExpression))
		}
		if patch == nil {
			patch = object
		} else {
			patch = pgsql.NewBinaryExpression(patch, pgsql.OperatorConcatenate, object)
		}
	}
	return patch
}

// mergeJSONValue preserves JSON types and leaves SQL NULL visible to validation
// and patch removal. Parameter casts belong to the reusable compiled structure.
func mergeJSONValue(rhs pgsql.Expression) pgsql.Expression {
	if parameter, ok := rhs.(*pgsql.Parameter); ok && !parameter.CastType.IsKnown() {
		parameter.CastType = pgsql.JSONB
	}
	if lookup, ok := expressionToPropertyLookupBinaryExpression(rhs); ok {
		lookup.Operator = pgsql.OperatorJSONField
	}
	if literal, ok := rhs.(pgsql.Literal); ok && literal.Null {
		return pgsql.Literal{Null: true, CastType: pgsql.JSONB}
	}
	// Inlined strings have PostgreSQL's unknown type. The polymorphic to_jsonb
	// argument needs its text cast even when parameters are materialized.
	if _, textValue := rewriteStringEqualityOperand(rhs); textValue {
		rhs = pgsql.NewTypeCast(rhs, pgsql.Text)
	}
	return pgsql.FunctionCall{Function: pgsql.FunctionToJSONB, Parameters: []pgsql.Expression{rhs}, CastType: pgsql.JSONB}
}

func (e *mergeEntity) inputColumns() []pgsql.Identifier {
	if e.matchProperties.Parameter != nil {
		return []pgsql.Identifier{e.properties}
	}
	columns := make([]pgsql.Identifier, 0, len(e.keys))
	for _, key := range e.keys {
		columns = append(columns, e.keyColumns[key])
	}
	return columns
}

func (e *mergeEntity) propertiesAt(frame *Frame) pgsql.Expression {
	if e.matchProperties.Parameter != nil {
		return pgsql.CompoundIdentifier{frame.Binding.Identifier, e.properties}
	}
	object := pgsql.FunctionCall{Function: pgsql.FunctionJSONBBuildObject, CastType: pgsql.JSONB}
	for _, key := range e.keys {
		object.Parameters = append(object.Parameters, pgsql.NewLiteral(key, pgsql.Text), pgsql.CompoundIdentifier{frame.Binding.Identifier, e.keyColumns[key]})
	}
	return object
}

func mergeExists(selectQuery pgsql.Select) pgsql.Expression {
	return pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{Body: selectQuery}}}
}

func mergeCountDistinct(value pgsql.Expression) pgsql.FunctionCall {
	return pgsql.FunctionCall{Function: "count", Distinct: true, Parameters: []pgsql.Expression{value}, CastType: pgsql.Int8}
}

func mergeTable(frame *Frame, alias pgsql.Identifier) pgsql.FromClause {
	return pgsql.FromClause{Source: pgsql.TableReference{Name: frame.Binding.Identifier.AsCompoundIdentifier(), Binding: pgsql.AsOptionalIdentifier(alias)}}
}

func (s *Translator) validateMergeCandidates(candidate *Frame, plan *mergePlan, updates map[pgsql.Identifier]pgsql.Expression, singleNodePattern bool) (*Frame, error) {
	one := pgsql.NewLiteral(1, pgsql.Int4)
	falseValue := pgsql.NewLiteral(false, pgsql.Boolean)
	// The aggregate consumes every input value even if no match, write source,
	// or final output row demands it. Empty upstream inputs are successful.
	var demanded pgsql.Expression
	for _, entity := range plan.entities {
		for _, column := range entity.inputColumns() {
			demanded = pgsql.OptionalAnd(demanded, pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{plan.input.Binding.Identifier, column}, pgsql.OperatorIsNot, pgsql.Literal{Null: true, CastType: pgsql.JSONB}))
		}
	}
	if demanded == nil {
		demanded = pgsql.NewLiteral(true, pgsql.Boolean)
	}
	inputValid := pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{pgsql.FunctionCall{Function: "bool_and", Parameters: []pgsql.Expression{demanded}, CastType: pgsql.Boolean}, pgsql.NewLiteral(true, pgsql.Boolean)}, CastType: pgsql.Boolean}}, From: []pgsql.FromClause{frameReference(plan.input)}}}}
	// A standalone single-node MERGE has one input. Its matches have unique
	// target IDs, and at most one creation can occur. This proof does not
	// depend on parameter contents and excludes carried action targets.
	if plan.singleInput && singleNodePattern && len(plan.entities) == 1 {
		return s.guardMergeCandidates(candidate, plan, inputValid, falseValue, falseValue, falseValue)
	}
	// UNION ALL retains distinct actions, including different bindings resolving
	// to one stored ID. No properties enter target grouping.
	source := &Frame{Binding: &BoundIdentifier{Identifier: "_merge_source"}}
	created := pgsql.CompoundIdentifier{source.Binding.Identifier, plan.created.Identifier}
	var actions pgsql.SetExpression
	seen := pgsql.NewIdentifierSet()
	for _, entity := range plan.entities {
		binding := entity.binding
		if seen.Contains(binding.Identifier) {
			continue
		}
		seen.Add(binding.Identifier)
		var writes pgsql.Expression = updates[binding.Identifier]
		if !entity.bound {
			if writes == nil {
				writes = created
			} else {
				writes = pgsql.NewParenthetical(pgsql.NewBinaryExpression(writes, pgsql.OperatorOr, created))
			}
		}
		if writes == nil {
			continue
		}
		projection := pgsql.Projection{mergeAlias(pgsql.NewLiteral(s.graphID, pgsql.Int4), "graph_id"), mergeAlias(pgsql.NewLiteral(binding.DataType.String(), pgsql.Text), "entity_type"), mergeAlias(mergeField(source, binding, pgsql.ColumnID), "target_id")}
		selectQuery := pgsql.Select{Projection: projection, From: []pgsql.FromClause{mergeTable(candidate, source.Binding.Identifier)}, Where: writes}
		if actions == nil {
			actions = selectQuery
		} else {
			actions = pgsql.SetOperation{Operator: pgsql.OperatorUnion, All: true, LOperand: actions, ROperand: selectQuery}
		}
	}
	var repeated pgsql.Expression = falseValue
	if actions != nil {
		actionFrame, err := s.scope.PushFrame()
		if err != nil {
			return nil, err
		}
		s.addCTE(actionFrame, actions)
		groups := []pgsql.Expression{pgsql.CompoundIdentifier{actionFrame.Binding.Identifier, "graph_id"}, pgsql.CompoundIdentifier{actionFrame.Binding.Identifier, "entity_type"}, pgsql.CompoundIdentifier{actionFrame.Binding.Identifier, "target_id"}}
		repeated = mergeExists(pgsql.Select{Projection: pgsql.Projection{one}, From: []pgsql.FromClause{frameReference(actionFrame)}, GroupBy: groups, Having: pgsql.NewBinaryExpression(pgsql.FunctionCall{Function: "count", Parameters: []pgsql.Expression{one}, CastType: pgsql.Int8}, pgsql.OperatorGreaterThan, one)})
	}
	// A narrow absence relation separates input identity from result fanout.
	absent, err := s.scope.PushFrame()
	if err != nil {
		return nil, err
	}
	absentProjection := pgsql.Projection{mergeAlias(pgsql.CompoundIdentifier{source.Binding.Identifier, plan.inputRow.Identifier}, "input_id")}
	if singleNodePattern {
		entity := plan.entities[0]
		if entity.matchProperties.Parameter != nil {
			absentProjection = append(absentProjection, mergeAlias(entity.propertiesAt(source), "original"), mergeAlias(mergeField(source, entity.binding, pgsql.ColumnProperties), "final"))
		} else {
			for idx, key := range entity.keys {
				absentProjection = append(absentProjection, mergeAlias(pgsql.CompoundIdentifier{source.Binding.Identifier, entity.keyColumns[key]}, pgsql.Identifier(fmt.Sprintf("original_%d", idx))))
				if entity.keysChanged {
					absentProjection = append(absentProjection, mergeAlias(pgsql.CompoundIdentifier{source.Binding.Identifier, entity.finalKeyColumns[key]}, pgsql.Identifier(fmt.Sprintf("final_%d", idx))))
				}
			}
		}
	}
	s.addCTE(absent, pgsql.Select{Projection: absentProjection, From: []pgsql.FromClause{mergeTable(candidate, source.Binding.Identifier)}, Where: created})
	absentCount := pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{mergeCountDistinct(pgsql.CompoundIdentifier{absent.Binding.Identifier, "input_id"})}, From: []pgsql.FromClause{frameReference(absent)}}}}
	multiple := pgsql.NewBinaryExpression(absentCount, pgsql.OperatorGreaterThan, one)
	var patterns, overlap pgsql.Expression = multiple, falseValue
	if singleNodePattern {
		patterns = falseValue
		entity := plan.entities[0]
		switch {
		case entity.matchProperties.Parameter == nil && len(entity.keys) == 0:
			overlap = multiple
		case entity.matchProperties.Parameter == nil && !entity.keysChanged:
			groups := []pgsql.Expression{}
			for idx := range entity.keys {
				groups = append(groups, pgsql.CompoundIdentifier{absent.Binding.Identifier, pgsql.Identifier(fmt.Sprintf("original_%d", idx))})
			}
			overlap = mergeExists(pgsql.Select{Projection: pgsql.Projection{one}, From: []pgsql.FromClause{frameReference(absent)}, GroupBy: groups, Having: pgsql.NewBinaryExpression(mergeCountDistinct(pgsql.CompoundIdentifier{absent.Binding.Identifier, "input_id"}), pgsql.OperatorGreaterThan, one)})
		default:
			a, b := pgsql.Identifier("_merge_original"), pgsql.Identifier("_merge_final")
			constraint := pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{a, "input_id"}, pgsql.OperatorNotEquals, pgsql.CompoundIdentifier{b, "input_id"})
			var equal pgsql.Expression
			if entity.matchProperties.Parameter != nil {
				unequal := pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{"_merge_key", "value"}, pgsql.Operator("is distinct from"), pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{b, "final"}, pgsql.OperatorJSONField, pgsql.CompoundIdentifier{"_merge_key", "key"}))
				equal = pgsql.ExistsExpression{Negated: true, Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{one}, From: []pgsql.FromClause{{Source: pgsql.AliasedExpression{Expression: pgsql.FunctionCall{Function: "jsonb_each", Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{a, "original"}}}, Alias: pgsql.AsOptionalIdentifier("_merge_key")}}}, Where: unequal}}}}
			} else {
				for idx := range entity.keys {
					equal = pgsql.OptionalAnd(equal, pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{a, pgsql.Identifier(fmt.Sprintf("original_%d", idx))}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{b, pgsql.Identifier(fmt.Sprintf("final_%d", idx))}))
				}
			}
			overlap = mergeExists(pgsql.Select{Projection: pgsql.Projection{one}, From: []pgsql.FromClause{mergeTable(absent, a), mergeTable(absent, b)}, Where: pgsql.OptionalAnd(constraint, equal)})
		}
	}
	return s.guardMergeCandidates(candidate, plan, inputValid, repeated, patterns, overlap)
}

func (s *Translator) guardMergeCandidates(candidate *Frame, plan *mergePlan, inputValid, repeated, patterns, overlap pgsql.Expression) (*Frame, error) {
	falseValue := pgsql.NewLiteral(false, pgsql.Boolean)
	guard, err := s.scope.PushFrame()
	if err != nil {
		return nil, err
	}
	check := pgsql.Identifier("_merge_valid")
	s.addCTE(guard, pgsql.Select{Projection: pgsql.Projection{mergeAlias(pgsql.FunctionCall{Function: "cypher_merge_assert", Parameters: []pgsql.Expression{inputValid, repeated, patterns, overlap}, CastType: pgsql.Boolean}, check)}})
	s.query.CurrentPart().Model.CommonTableExpressions.Expressions[len(s.query.CurrentPart().Model.CommonTableExpressions.Expressions)-1].Materialized = &pgsql.Materialized{Materialized: true}
	plan.guard = guard
	needsAnchor := true
	for _, entity := range plan.entities {
		if !entity.bound {
			needsAnchor = false
			break
		}
	}
	if needsAnchor {
		// PostgreSQL prunes a DO NOTHING-only MERGE. A conditional INSERT keeps the
		// source demanded, but the assertion either returns true or raises, so this
		// anchor never writes a row (and never consumes an entity sequence value).
		anchor, err := s.scope.PushFrame()
		if err != nil {
			return nil, err
		}
		s.addCTE(anchor, pgsql.Merge{Into: true, Table: pgsql.TableReference{Name: pgsql.TableNode.AsCompoundIdentifier()}, Source: pgsql.TableReference{Name: guard.Binding.Identifier.AsCompoundIdentifier()}, JoinTarget: falseValue, Actions: []pgsql.MergeAction{pgsql.UnmatchedAction{Predicate: pgsql.NewUnaryExpression(pgsql.OperatorNot, pgsql.CompoundIdentifier{guard.Binding.Identifier, check}), Columns: []pgsql.Identifier{pgsql.ColumnGraphID}, Values: pgsql.Values{Values: []pgsql.Expression{pgsql.Literal{Null: true, CastType: pgsql.Int4}}}}}})
	}

	projection, err := s.mergeCarry(candidate)
	if err != nil {
		return nil, err
	}
	return s.mergeStage(projection, []pgsql.FromClause{frameReference(candidate), frameReference(guard)}, pgsql.CompoundIdentifier{guard.Binding.Identifier, check})
}

func mapColumns(columns map[string]pgsql.Identifier) []pgsql.Identifier {
	result := make([]pgsql.Identifier, 0, len(columns))
	for _, column := range columns {
		result = append(result, column)
	}
	return result
}
