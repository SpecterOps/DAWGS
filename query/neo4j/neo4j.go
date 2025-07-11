package neo4j

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/walk"

	cypherBackend "github.com/specterops/dawgs/cypher/models/cypher/format"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

var (
	ErrAmbiguousQueryVariables = errors.New("query mixes node and relationship query variables")
)

type QueryBuilder struct {
	Parameters map[string]any

	query    *cypher.RegularQuery
	order    *cypher.Order
	prepared bool
}

func NewQueryBuilder(singleQuery *cypher.RegularQuery) *QueryBuilder {
	return &QueryBuilder{
		query: cypher.Copy(singleQuery),
	}
}

func NewEmptyQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		query: &cypher.RegularQuery{
			SingleQuery: &cypher.SingleQuery{
				SinglePartQuery: &cypher.SinglePartQuery{},
			},
		},
	}
}

func (s *QueryBuilder) rewriteParameters() error {
	parameterRewriter := query.NewParameterRewriter()

	if err := walk.Cypher(s.query, parameterRewriter); err != nil {
		return err
	}

	s.Parameters = parameterRewriter.Parameters
	return nil
}

func (s *QueryBuilder) Apply(criteria graph.Criteria) {
	switch typedCriteria := criteria.(type) {
	case *cypher.Where:
		if query.GetFirstReadingClause(s.query) == nil {
			s.query.SingleQuery.SinglePartQuery.AddReadingClause(&cypher.ReadingClause{
				Match: cypher.NewMatch(false),
			})
		}

		query.GetFirstReadingClause(s.query).Match.Where = cypher.Copy(typedCriteria)

	case *cypher.Return:
		s.query.SingleQuery.SinglePartQuery.Return = cypher.Copy(typedCriteria)

	case *cypher.Limit:
		if s.query.SingleQuery.SinglePartQuery.Return != nil {
			s.query.SingleQuery.SinglePartQuery.Return.Projection.Limit = cypher.Copy(typedCriteria)
		}

	case *cypher.Skip:
		if s.query.SingleQuery.SinglePartQuery.Return != nil {
			s.query.SingleQuery.SinglePartQuery.Return.Projection.Skip = cypher.Copy(typedCriteria)
		}

	case *cypher.Order:
		s.order = cypher.Copy(typedCriteria)

	case []*cypher.UpdatingClause:
		for _, updatingClause := range typedCriteria {
			s.Apply(updatingClause)
		}

	case *cypher.UpdatingClause:
		s.query.SingleQuery.SinglePartQuery.AddUpdatingClause(cypher.Copy(typedCriteria))

	default:
		panic(fmt.Sprintf("invalid type for dawgs query: %T %+v", criteria, criteria))
	}
}

func (s *QueryBuilder) prepareMatch() error {
	var (
		patternPart = &cypher.PatternPart{}

		singleNodeBound    = false
		creatingSingleNode = false

		startNodeBound       = false
		creatingStartNode    = false
		endNodeBound         = false
		creatingEndNode      = false
		relationshipBound    = false
		creatingRelationship = false

		isRelationshipQuery = false

		bindWalk = walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
			switch typedNode := node.(type) {
			case *cypher.Variable:
				switch typedNode.Symbol {
				case query.NodeSymbol:
					singleNodeBound = true

				case query.EdgeStartSymbol:
					startNodeBound = true
					isRelationshipQuery = true

				case query.EdgeEndSymbol:
					endNodeBound = true
					isRelationshipQuery = true

				case query.EdgeSymbol:
					relationshipBound = true
					isRelationshipQuery = true
				}
			}
		})
	)

	// Zip through updating clauses first
	for _, updatingClause := range s.query.SingleQuery.SinglePartQuery.UpdatingClauses {
		typedUpdatingClause, typeOK := updatingClause.(*cypher.UpdatingClause)

		if !typeOK {
			return fmt.Errorf("unexpected updating clause type %T", typedUpdatingClause)
		}

		switch typedClause := typedUpdatingClause.Clause.(type) {
		case *cypher.Create:
			if err := walk.Cypher(typedClause, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
				switch typedElement := node.(type) {
				case *cypher.NodePattern:
					switch typedElement.Variable.Symbol {
					case query.NodeSymbol:
						creatingSingleNode = true

					case query.EdgeStartSymbol:
						creatingStartNode = true

					case query.EdgeEndSymbol:
						creatingEndNode = true
					}

				case *cypher.RelationshipPattern:
					switch typedElement.Variable.Symbol {
					case query.EdgeSymbol:
						creatingRelationship = true
					}
				}
			})); err != nil {
				return err
			}

		case *cypher.Delete:
			if err := walk.Cypher(typedClause, bindWalk); err != nil {
				return err
			}
		}
	}

	// Is there a where clause?
	if firstReadingClause := query.GetFirstReadingClause(s.query); firstReadingClause != nil && firstReadingClause.Match.Where != nil {
		if err := walk.Cypher(firstReadingClause.Match.Where, bindWalk); err != nil {
			return err
		}
	}

	// Is there a return clause
	if spqReturn := s.query.SingleQuery.SinglePartQuery.Return; spqReturn != nil && spqReturn.Projection != nil {
		// Did we have an order specified?
		if s.order != nil {
			if spqReturn.Projection.Order != nil {
				return fmt.Errorf("order specified twice")
			}

			s.query.SingleQuery.SinglePartQuery.Return.Projection.Order = s.order
		}

		if err := walk.Cypher(s.query.SingleQuery.SinglePartQuery.Return, bindWalk); err != nil {
			return err
		}
	}

	// Validate we're not mixing references
	if isRelationshipQuery && singleNodeBound {
		return ErrAmbiguousQueryVariables
	}

	if singleNodeBound && !creatingSingleNode {
		patternPart.AddPatternElements(&cypher.NodePattern{
			Variable: cypher.NewVariableWithSymbol(query.NodeSymbol),
		})
	}

	if startNodeBound {
		patternPart.AddPatternElements(&cypher.NodePattern{
			Variable: cypher.NewVariableWithSymbol(query.EdgeStartSymbol),
		})
	}

	if isRelationshipQuery {
		if !startNodeBound && !creatingStartNode {
			patternPart.AddPatternElements(&cypher.NodePattern{})
		}

		if !creatingRelationship {
			if relationshipBound {
				patternPart.AddPatternElements(&cypher.RelationshipPattern{
					Variable:  cypher.NewVariableWithSymbol(query.EdgeSymbol),
					Direction: graph.DirectionOutbound,
				})
			} else {
				patternPart.AddPatternElements(&cypher.RelationshipPattern{
					Direction: graph.DirectionOutbound,
				})
			}
		}

		if !endNodeBound && !creatingEndNode {
			patternPart.AddPatternElements(&cypher.NodePattern{})
		}
	}

	if endNodeBound {
		patternPart.AddPatternElements(&cypher.NodePattern{
			Variable: cypher.NewVariableWithSymbol(query.EdgeEndSymbol),
		})
	}

	if firstReadingClause := query.GetFirstReadingClause(s.query); firstReadingClause != nil {
		firstReadingClause.Match.Pattern = []*cypher.PatternPart{patternPart}
	} else if len(patternPart.PatternElements) > 0 {
		s.query.SingleQuery.SinglePartQuery.AddReadingClause(&cypher.ReadingClause{
			Match: &cypher.Match{
				Pattern: []*cypher.PatternPart{
					patternPart,
				},
			},
		})
	}

	return nil
}

func (s *QueryBuilder) compilationErrors() error {
	var modelErrors []error

	walk.Cypher(s.query, walk.NewSimpleVisitor[cypher.SyntaxNode](func(node cypher.SyntaxNode, errorHandler walk.VisitorHandler) {
		if errorNode, typeOK := node.(cypher.Fallible); typeOK {
			if len(errorNode.Errors()) > 0 {
				modelErrors = append(modelErrors, errorNode.Errors()...)
			}
		}
	}))

	return errors.Join(modelErrors...)
}

func (s *QueryBuilder) Prepare() error {
	if s.prepared {
		return nil
	}

	s.prepared = true

	if s.query.SingleQuery.SinglePartQuery == nil {
		return fmt.Errorf("single part query is nil")
	}

	if err := s.compilationErrors(); err != nil {
		return err
	}

	if err := s.prepareMatch(); err != nil {
		return err
	}

	if err := s.rewriteParameters(); err != nil {
		return err
	}

	return walk.Cypher(s.query, NewExpressionListRewriter())
}

func (s *QueryBuilder) PrepareAllShortestPaths() error {
	if err := s.Prepare(); err != nil {
		return err
	} else {
		firstReadingClause := query.GetFirstReadingClause(s.query)

		// Set all pattern parts to search for the shortest paths and bind them
		if len(firstReadingClause.Match.Pattern) > 1 {
			return fmt.Errorf("only expected one pattern")
		}

		// Grab the first pattern part
		patternPart := firstReadingClause.Match.Pattern[0]

		// Bind the path
		patternPart.Variable = cypher.NewVariableWithSymbol(query.PathSymbol)

		// Set the pattern to search for all shortest paths
		patternPart.AllShortestPathsPattern = true

		// Update all relationship PatternElements to expand fully (*..)
		for _, patternElement := range patternPart.PatternElements {
			if relationshipPattern, isRelationshipPattern := patternElement.AsRelationshipPattern(); isRelationshipPattern {
				relationshipPattern.Range = &cypher.PatternRange{}
			}
		}

		return nil
	}
}

func (s *QueryBuilder) Render() (string, error) {
	buffer := &bytes.Buffer{}

	if err := cypherBackend.NewCypherEmitter(false).Write(s.query, buffer); err != nil {
		return "", err
	} else {
		return buffer.String(), nil
	}
}
