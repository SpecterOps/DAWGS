package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateWith() error {
	currentPart := s.query.CurrentPart()

	if !currentPart.HasProjections() {
		currentPart.Frame.Exported.Clear()
	} else {
		var (
			projectedItems = pgsql.NewIdentifierSet()

			// aggregatedItems contains a set of symbols of projected aggregate functions.
			aggregatedItems = pgsql.NewSymbolTable()

			// groupByItems are the non-aggregate projection expressions that the query must group by.
			groupByItems []pgsql.Expression
		)

		for _, projectionItem := range currentPart.projections.Items {
			if err := RewriteFrameBindings(s.scope, projectionItem.SelectItem); err != nil {
				return err
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
		if projectionConstraint, err := s.treeTranslator.ConsumeConstraintsFromVisibleSet(set); err != nil {
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
					var selectItem pgsql.SelectItem
					projectedBinding := binding

					if projectionItem.Alias.Set {
						if aliasBinding, aliasBound := s.scope.AliasedLookup(projectionItem.Alias.Value); !aliasBound || aliasBinding.Identifier != binding.Identifier {
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

					// Assign the frame to the binding's last projection backref
					projectedBinding.MaterializedBy(currentPart.Frame)

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

						// Assign the frame to the binding's last projection backref
						binding.LastProjection = currentPart.Frame

						// Reveal and export the identifier in the current multipart query part's frame
						currentPart.Frame.Reveal(binding.Identifier)
						currentPart.Frame.Export(binding.Identifier)

						// Rewrite this projection's alias to use the internal binding
						projectionItem.Alias.Value = binding.Identifier
					}
				}
			}
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
