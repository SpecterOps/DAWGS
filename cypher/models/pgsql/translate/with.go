package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) prepareWith(with *cypher.With) error {
	part := s.query.CurrentPart()
	part.withModifiers = with.Projection != nil && (with.Projection.Order != nil || with.Projection.Skip != nil || with.Projection.Limit != nil)
	if !s.multipartWrites && !part.withModifiers {
		return nil
	}
	constraint, err := s.treeTranslator.ConsumeAllConstraints()
	if err != nil {
		return err
	}
	resolved, err := resolvePathCompositeFieldReferences(s.scope, constraint.Expression)
	if err != nil {
		return err
	}
	if err := RewriteFrameBindings(s.scope, resolved); err != nil {
		return err
	}
	s.query.CurrentPart().withInputConstraints = resolved
	return nil
}

// translateWith closes the current query part, projects WITH items, and opens the scope consumed by the next part.
func (s *Translator) translateWith() error {
	currentPart := s.query.CurrentPart()
	boundaryPredicates := s.multipartWrites || currentPart.withModifiers
	if boundaryPredicates && currentPart.projections.Frame == nil {
		// Literal projections still consume the incoming row stream. The
		// placeholder frame of a leading WITH resolves to no physical source.
		currentPart.projections.Frame = s.scope.CurrentFrame()
	}
	if currentPart.HasDeletions() {
		if err := s.buildDeletions(s.scope); err != nil {
			return err
		}
		// DELETE has no RETURNING relation. WITH continues with the input
		// rows while the top-level data-modifying CTE performs the deletion.
		deletionFrames := map[*Frame]struct{}{}
		for _, deletion := range currentPart.mutations.Deletions.Values() {
			deletionFrames[deletion.Frame] = struct{}{}
		}
		for currentPart.projections.Frame != nil {
			if _, deletionFrame := deletionFrames[currentPart.projections.Frame]; !deletionFrame {
				break
			}
			currentPart.projections.Frame = currentPart.projections.Frame.Previous
		}
	}

	if !currentPart.HasProjections() {
		currentPart.Frame.Exported.Clear()
	} else {
		var (
			projectedItems = pgsql.NewIdentifierSet()
			materialized   []*BoundIdentifier

			// aggregatedItems contains a set of symbols of projected aggregate functions.
			aggregatedItems = pgsql.NewSymbolTable()

			// groupByItems are the non-aggregate projection expressions that the query must group by.
			groupByItems []pgsql.Expression
		)

		for _, projectionItem := range currentPart.projections.Items {
			if err := RewriteFrameBindings(s.scope, projectionItem.SelectItem); err != nil {
				return err
			} else if _, isIdentifier := projectionItem.SelectItem.(pgsql.Identifier); isIdentifier {
				continue
			} else if resolvedSelectItem, err := resolvePathCompositeFieldReferences(s.scope, projectionItem.SelectItem); err != nil {
				return err
			} else if selectItem, isSelectItem := resolvedSelectItem.(pgsql.SelectItem); !isSelectItem {
				return fmt.Errorf("resolved with projection item is not selectable: %T", resolvedSelectItem)
			} else {
				projectionItem.SelectItem = selectItem
			}
		}

		// If an aggregation function is being used, this invokes an implicit group by of non-function projections
		for _, projectionItem := range currentPart.projections.Items {
			if aggregatedFunctionSymbols, err := GetAggregatedFunctionParameterSymbolsIn(projectionItem.SelectItem); err != nil {
				return err
			} else if !aggregatedFunctionSymbols.IsEmpty() {
				aggregatedItems.AddTable(aggregatedFunctionSymbols)
			}

			if selectItemGroupByExpressions, err := NonAggregateGroupByExpressions(projectionItem.SelectItem); err != nil {
				return err
			} else {
				groupByItems = append(groupByItems, selectItemGroupByExpressions...)
			}
		}

		set := s.scope.CurrentFrame().Known().RemoveSet(aggregatedItems.RootIdentifiers())
		if s.query.CurrentPart().quantifierIdentifiers != nil && s.query.CurrentPart().quantifierIdentifiers.Len() > 0 {
			set = set.MergeSet(s.query.CurrentPart().quantifierIdentifiers)
		}
		if boundaryPredicates {
			currentPart.projections.Constraints = currentPart.withInputConstraints
		} else if projectionConstraint, err := s.treeTranslator.ConsumeConstraintsFromVisibleSet(set); err != nil {
			return err
		} else if resolvedConstraint, err := resolvePathCompositeFieldReferences(s.scope, projectionConstraint.Expression); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, resolvedConstraint); err != nil {
			return err
		} else {
			currentPart.projections.Constraints = resolvedConstraint
		}

		for idx, projectionItem := range currentPart.projections.Items {
			switch typedSelectItem := projectionItem.SelectItem.(type) {
			case pgsql.CompoundIdentifier:
				return fmt.Errorf("compound identifier not supported in with statement")

			case pgsql.Identifier:
				if binding, isBound := s.scope.Lookup(typedSelectItem); !isBound {
					return fmt.Errorf("unable to lookup identifer %s for with statement", typedSelectItem)
				} else {
					var (
						selectItem       pgsql.SelectItem
						projectedBinding = binding
					)

					if projectionItem.Alias.Set {
						if aliasBinding, aliasBound := s.scope.AliasedLookup(projectionItem.Alias.Value); !aliasBound || (aliasBinding.Identifier != binding.Identifier && aliasBinding.LastProjection != nil) {
							var err error

							if projectedBinding, err = s.scope.DefineNew(binding.DataType); err != nil {
								return err
							}

							projectedBinding.Dependencies = binding.Dependencies
							s.scope.Alias(projectionItem.Alias.Value, projectedBinding)
						} else {
							projectedBinding = aliasBinding
						}
					}

					if binding.LastProjection != nil {
						selectItem = pgsql.CompoundIdentifier{
							binding.LastProjection.Binding.Identifier, typedSelectItem,
						}
					} else if projectedBinding.DataType == pgsql.PathComposite {
						builtProjection, err := buildProjection(projectedBinding.Identifier, projectedBinding, s.scope, nil)
						if err != nil {
							return err
						}
						if len(builtProjection) != 1 {
							return fmt.Errorf("expected path projection %s to produce one select item, got %d", projectedBinding.Identifier, len(builtProjection))
						}
						selectItem = builtProjection[0]
					} else {
						// A WITH can project bindings introduced in the same query
						// part before they have a materialized frame back-reference.
						selectItem = typedSelectItem
					}

					// Track this projected item for scope pruning
					projectedItems.Add(projectedBinding.Identifier)

					// Create a new projection that maps the identifier
					currentPart.projections.Items[idx] = &Projection{
						SelectItem: selectItem,
					}
					if projectedBinding.DataType != pgsql.PathComposite || binding.LastProjection != nil {
						currentPart.projections.Items[idx].Alias = pgsql.AsOptionalIdentifier(projectedBinding.Identifier)
					}

					// Delay the back-reference update until every select item has
					// been built. Path projections may depend on node bindings that
					// appear earlier in a greedy WITH projection, and those
					// dependencies must still reference the input frame here.
					materialized = append(materialized, projectedBinding)

					// Reveal and export the identifier in the current multipart query part's frame
					currentPart.Frame.Reveal(projectedBinding.Identifier)
					currentPart.Frame.Export(projectedBinding.Identifier)
				}

			default:
				// If this is not an identifier then check if the alias is specified. If the alias is specified, this
				// is a pure export (left-hand side is some other expression) and a new bound identifier is being
				// introduced.
				if projectionItem.Alias.Set {
					if binding, isBound := s.scope.AliasedLookup(projectionItem.Alias.Value); !isBound {
						return fmt.Errorf("unable to lookup alias %s for with statement", projectionItem.Alias.Value)
					} else {
						// Track this projected item for scope pruning
						projectedItems.Add(binding.Identifier)

						materialized = append(materialized, binding)

						// Reveal and export the identifier in the current multipart query part's frame
						currentPart.Frame.Reveal(binding.Identifier)
						currentPart.Frame.Export(binding.Identifier)

						// Rewrite this projection's alias to use the internal binding
						projectionItem.Alias.Value = binding.Identifier
					}
				}
			}
		}
		if boundaryPredicates {
			constraint, err := s.treeTranslator.ConsumeAllConstraints()
			if err != nil {
				return err
			}
			resolved, err := resolvePathCompositeFieldReferences(s.scope, constraint.Expression)
			if err != nil {
				return err
			}
			if err := RewriteFrameBindings(s.scope, resolved); err != nil {
				return err
			}
			if resolved != nil {
				aliases := map[pgsql.Identifier]pgsql.Expression{}
				for _, projection := range currentPart.projections.Items {
					if projection.Alias.Set {
						aliases[projection.Alias.Value] = projection.SelectItem
					}
				}
				resolved, err = multipartInputPredicate(s.kindMapper, currentPart.Frame, aliases, resolved)
				if err != nil {
					return err
				}
			}
			currentPart.projections.OutputConstraints = resolved
		}
		for _, binding := range materialized {
			binding.MaterializedBy(currentPart.Frame)
		}

		if !aggregatedItems.IsEmpty() {
			currentPart.projections.GroupBy = append(currentPart.projections.GroupBy, groupByItems...)
		}

		if err := s.scope.PruneDefinitions(projectedItems); err != nil {
			return err
		}

		// Prune scope to only what's being exported by the with statement
		currentPart.Frame.Visible = projectedItems.Copy()
		currentPart.Frame.Exported = projectedItems.Copy()
	}

	return nil
}
