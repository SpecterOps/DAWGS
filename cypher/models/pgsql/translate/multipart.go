package translate

import (
	"fmt"
	"strings"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/walk"
)

// QueryStage is one statement in a multipart query with sequential mutations.
// Binding names the output consumed by subsequent stages; the final stage has no binding.
type QueryStage struct {
	Binding pgsql.Identifier
	Query   pgsql.Query
}

func queryPartHasWrites(part *QueryPart) bool {
	return part.mutations != nil && (part.HasMutations() || part.HasDeletions() || part.mutations.Creations.Len() > 0 || part.mutations.EdgeCreations.Len() > 0)
}

func prependQueryCTEs(query pgsql.Query, expressions []pgsql.CommonTableExpression) pgsql.Query {
	if len(expressions) == 0 {
		return query
	}
	with := pgsql.With{}
	if query.CommonTableExpressions != nil {
		with = *query.CommonTableExpressions
	}
	with.Expressions = append(append([]pgsql.CommonTableExpression(nil), expressions...), with.Expressions...)
	query.CommonTableExpressions = &with
	return query
}

// multipartInputExpression resolves output aliases back to the expressions
// available in the SELECT that computes a WITH projection.
func multipartInputExpression(frame *Frame, aliases map[pgsql.Identifier]pgsql.Expression, expression pgsql.Expression) (pgsql.Expression, error) {
	aliasScope := NewScope()
	for alias := range aliases {
		aliasScope.Define(alias, pgsql.UnknownDataType).MaterializedBy(frame)
	}
	wrapped := pgsql.NewParenthetical(expression)
	if err := RewriteFrameBindings(aliasScope, wrapped); err != nil {
		return nil, err
	}
	return rewriteCurrentFrameProjectionReferences(wrapped.Expression, frame.Binding.Identifier, aliases), nil
}

func multipartInputPredicate(kindMapper *contextAwareKindMapper, frame *Frame, aliases map[pgsql.Identifier]pgsql.Expression, expression pgsql.Expression) (pgsql.Expression, error) {
	resolved, err := multipartInputExpression(frame, aliases, expression)
	if err != nil {
		return nil, err
	}
	// Substituting a property projection introduces its JSONB extraction after
	// the predicate's original type inference. Restore comparison casts on a
	// copy so the SELECT value and ORDER BY retain their projection types.
	resolved = rewriteCurrentFrameProjectionReferences(resolved, frame.Binding.Identifier, nil)
	err = walk.PgSQL(resolved, walk.NewSimpleVisitorWithOrder(walk.OrderPostfix, func(node pgsql.SyntaxNode, handler walk.VisitorHandler) {
		switch expression := node.(type) {
		case *pgsql.BinaryExpression:
			handler.SetError(applyBinaryExpressionTypeHints(kindMapper, expression))
		case *pgsql.UnaryExpression:
			handler.SetError(applyUnaryExpressionTypeHints(expression))
		}
	}))
	return resolved, err
}

func multipartOrderBy(part *QueryPart, projection pgsql.Projection) ([]*pgsql.OrderBy, error) {
	aliases := projectionAliasExpressions(projection)
	orderBy := make([]*pgsql.OrderBy, 0, len(part.SortItems))
	for _, item := range part.SortItems {
		cloned := *item
		expression, err := multipartInputExpression(part.Frame, aliases, cloned.Expression)
		if err != nil {
			return nil, err
		}
		cloned.Expression = expression
		orderBy = append(orderBy, &cloned)
	}
	return orderBy, nil
}

func multipartBoundaryQuery(part *QueryPart, query pgsql.Query, projection pgsql.Select) (pgsql.Query, error) {
	orderBy, err := multipartOrderBy(part, projection.Projection)
	if err != nil {
		return pgsql.Query{}, err
	}
	query.Body = projection
	query.OrderBy = orderBy
	query.Offset = part.Skip
	query.Limit = part.Limit
	if part.projections.OutputConstraints == nil {
		return query, nil
	}

	// WITH WHERE follows ordering/pagination. Evaluate its predicate with
	// the projection (including hidden input bindings), carry only the bool
	// through LIMIT, then filter it before the next query part can write.
	predicate := pgsql.Identifier(part.Frame.Binding.Identifier.String() + "_where")
	output := pgsql.Projection{}
	for _, item := range projection.Projection {
		alias, hasAlias := selectItemAlias(item)
		if !hasAlias {
			return pgsql.Query{}, fmt.Errorf("WITH projection has no output alias: %T", item)
		}
		output = append(output, pgsql.CompoundIdentifier{part.Frame.Binding.Identifier, alias})
	}
	projection.Projection = append(append(pgsql.Projection(nil), projection.Projection...), &pgsql.AliasedExpression{
		Expression: part.projections.OutputConstraints,
		Alias:      pgsql.AsOptionalIdentifier(predicate),
	})
	with := pgsql.With{}
	if query.CommonTableExpressions != nil {
		with = *query.CommonTableExpressions
	}
	with.Expressions = append(append([]pgsql.CommonTableExpression(nil), with.Expressions...), pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: part.Frame.Binding.Identifier},
		Query: pgsql.Query{Body: projection, OrderBy: orderBy, Offset: part.Skip, Limit: part.Limit},
	})
	return pgsql.Query{CommonTableExpressions: &with, Body: pgsql.Select{
		Projection: output,
		From:       []pgsql.FromClause{{Source: part.Frame.Binding.Identifier}},
		Where:      pgsql.CompoundIdentifier{part.Frame.Binding.Identifier, predicate},
	}}, nil
}

// buildStagedMultiPartQuery keeps the first write with its read-only inputs,
// then preserves every remaining Cypher query-part boundary as a statement.
func (s *Translator) buildStagedMultiPartQuery(firstWrite int) error {
	var (
		prefix []pgsql.CommonTableExpression
		stages []QueryStage
	)
	for index, part := range s.query.Parts[:len(s.query.Parts)-1] {
		if part.Frame == nil {
			return fmt.Errorf("multipart query part %d has no output frame", index)
		}
		query := *part.Model
		if query.CommonTableExpressions != nil && len(query.CommonTableExpressions.Expressions) == 0 {
			query.CommonTableExpressions = nil
		}
		projection, err := s.buildInlineProjection(part)
		if err != nil {
			return err
		}
		// These expressions were resolved while this part's input scope was
		// active. Rewriting them against the final scope would cross boundaries.
		query, err = multipartBoundaryQuery(part, query, projection)
		if err != nil {
			return err
		}
		if index < firstWrite {
			prefix = append(prefix, pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{Name: part.Frame.Binding.Identifier},
				Query: query,
			})
			continue
		}
		if index == firstWrite {
			query = prependQueryCTEs(query, prefix)
		}
		stages = append(stages, QueryStage{Binding: part.Frame.Binding.Identifier, Query: query})
	}
	stages = append(stages, QueryStage{Query: *s.query.CurrentPart().Model})
	s.translation.Stages = stages
	s.translation.Statement = nil
	return nil
}

// TranslatedStatements formats each statement in execution order. tablePrefix
// must uniquely identify the temporary relations for this execution.
func TranslatedStatements(translation Result, tablePrefix string) ([]format.Formatted, error) {
	if len(translation.Stages) == 0 {
		formatted, err := Translated(translation)
		if err != nil {
			return nil, err
		}
		return []format.Formatted{formatted}, nil
	}
	if tablePrefix == "" {
		return nil, fmt.Errorf("multipart temporary tables require a unique table prefix")
	}
	bindings := map[pgsql.Identifier]struct{}{}
	for index, stage := range translation.Stages {
		if index == len(translation.Stages)-1 {
			if stage.Binding != "" {
				return nil, fmt.Errorf("final multipart stage must not have an output binding")
			}
			continue
		}
		name := tablePrefix + stage.Binding.String()
		if stage.Binding == "" || strings.ContainsRune(name, '\x00') || len(name) > 63 {
			return nil, fmt.Errorf("invalid multipart temporary table name for stage %d", index)
		}
		if _, duplicate := bindings[stage.Binding]; duplicate {
			return nil, fmt.Errorf("duplicate multipart output binding %q", stage.Binding)
		}
		bindings[stage.Binding] = struct{}{}
	}

	var (
		statements []format.Formatted
		inputs     []pgsql.CommonTableExpression
	)
	for _, stage := range translation.Stages {
		query := prependQueryCTEs(stage.Query, inputs)
		formatted, err := format.Statement(query, format.NewOutputBuilder().WithTargetGraph(translation.GraphID))
		if err != nil {
			return nil, err
		}
		if stage.Binding != "" {
			name := tablePrefix + stage.Binding.String()
			quotedName := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
			formatted.Statement = "create temporary table pg_temp." + quotedName + " on commit drop as " + formatted.Statement
			inputs = append(inputs, pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{Name: stage.Binding},
				Query: pgsql.Query{Body: pgsql.Select{
					Projection: pgsql.Projection{pgsql.WildcardIdentifier},
					From:       []pgsql.FromClause{{Source: pgsql.FormattingLiteral("pg_temp." + quotedName)}},
				}},
			})
		}
		statements = append(statements, formatted)
	}
	return statements, nil
}
