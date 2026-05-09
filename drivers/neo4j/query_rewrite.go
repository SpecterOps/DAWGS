package neo4j

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	cypherfmt "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/graph"
)

type patternPropertyParameterRewriter struct {
	parameters          map[string]any
	rewrittenParameters map[string]any
	nextParameterID     int
	rewritten           bool
}

func rewritePatternPropertyParameters(query string, parameters map[string]any) (string, map[string]any, error) {
	if !strings.Contains(query, "$") {
		return query, parameters, nil
	}

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		return query, parameters, nil
	}

	rewriter := &patternPropertyParameterRewriter{
		parameters: parameters,
	}

	if err := rewriter.rewriteRegularQuery(parsed); err != nil {
		return "", nil, err
	}
	if !rewriter.rewritten {
		return query, parameters, nil
	}

	rewrittenQuery, err := cypherfmt.RegularQuery(parsed, false)
	if err != nil {
		return "", nil, err
	}

	return rewrittenQuery, rewriter.rewrittenParameters, nil
}

func (s *patternPropertyParameterRewriter) rewriteRegularQuery(query *cypher.RegularQuery) error {
	if query == nil || query.SingleQuery == nil {
		return nil
	}

	if singlePartQuery := query.SingleQuery.SinglePartQuery; singlePartQuery != nil {
		if err := s.rewriteReadingClauses(singlePartQuery.ReadingClauses); err != nil {
			return err
		}
	}

	if multiPartQuery := query.SingleQuery.MultiPartQuery; multiPartQuery != nil {
		for _, part := range multiPartQuery.Parts {
			if err := s.rewriteReadingClauses(part.ReadingClauses); err != nil {
				return err
			}
		}
		if multiPartQuery.SinglePartQuery != nil {
			if err := s.rewriteReadingClauses(multiPartQuery.SinglePartQuery.ReadingClauses); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *patternPropertyParameterRewriter) rewriteReadingClauses(readingClauses []*cypher.ReadingClause) error {
	for _, readingClause := range readingClauses {
		if readingClause.Match == nil {
			continue
		}

		for _, patternPart := range readingClause.Match.Pattern {
			for _, patternElement := range patternPart.PatternElements {
				if nodePattern, isNodePattern := patternElement.AsNodePattern(); isNodePattern {
					if err := s.rewriteProperties(&nodePattern.Properties); err != nil {
						return err
					}
				} else if relationshipPattern, isRelationshipPattern := patternElement.AsRelationshipPattern(); isRelationshipPattern {
					if err := s.rewriteProperties(&relationshipPattern.Properties); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (s *patternPropertyParameterRewriter) rewriteProperties(properties *cypher.Expression) error {
	if properties == nil || *properties == nil {
		return nil
	}

	cypherProperties, isProperties := (*properties).(*cypher.Properties)
	if !isProperties || cypherProperties.Parameter == nil {
		return nil
	}

	parameterName := cypherProperties.Parameter.Symbol
	parameterValue, hasParameter := s.parameters[parameterName]
	if !hasParameter {
		return fmt.Errorf("pattern property parameter %q was not provided", parameterName)
	}

	propertyMap, err := parameterPropertyMap(parameterValue)
	if err != nil {
		return fmt.Errorf("pattern property parameter %q: %w", parameterName, err)
	}
	if len(propertyMap) == 0 {
		*properties = nil
		s.rewritten = true
		return nil
	}

	literal := cypher.NewMapLiteral()
	keys := make([]string, 0, len(propertyMap))
	for key := range propertyMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		rewrittenParameter := s.nextParameterName()
		s.ensureRewrittenParameters()[rewrittenParameter] = propertyMap[key]
		literal[key] = &cypher.Parameter{Symbol: rewrittenParameter}
	}

	*properties = &cypher.Properties{Map: literal}
	s.rewritten = true
	return nil
}

func (s *patternPropertyParameterRewriter) ensureRewrittenParameters() map[string]any {
	if s.rewrittenParameters == nil {
		s.rewrittenParameters = make(map[string]any, len(s.parameters)+4)
		for key, value := range s.parameters {
			s.rewrittenParameters[key] = value
		}
	}

	return s.rewrittenParameters
}

func (s *patternPropertyParameterRewriter) nextParameterName() string {
	for {
		next := fmt.Sprintf("__dawgs_pattern_property_%d", s.nextParameterID)
		s.nextParameterID++

		if _, exists := s.ensureRewrittenParameters()[next]; !exists {
			return next
		}
	}
}

func parameterPropertyMap(value any) (map[string]any, error) {
	switch typedValue := value.(type) {
	case map[string]any:
		return typedValue, nil
	case graph.Properties:
		return typedValue.MapOrEmpty(), nil
	case *graph.Properties:
		return typedValue.MapOrEmpty(), nil
	}

	reflectedValue := reflect.ValueOf(value)
	if reflectedValue.Kind() != reflect.Map || reflectedValue.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("expected map with string keys but received %T", value)
	}

	propertyMap := make(map[string]any, reflectedValue.Len())
	iter := reflectedValue.MapRange()
	for iter.Next() {
		propertyMap[iter.Key().String()] = iter.Value().Interface()
	}

	return propertyMap, nil
}
