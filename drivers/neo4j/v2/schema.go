package v2

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4j2 "github.com/specterops/dawgs/drivers/neo4j"
	v2 "github.com/specterops/dawgs/v2"

	"github.com/specterops/dawgs/graph"
)

const (
	nativeBTreeIndexProvider  = "native-btree-1.0"
	nativeLuceneIndexProvider = "lucene+native-3.0"

	dropPropertyIndexStatement        = "drop index $name;"
	dropPropertyConstraintStatement   = "drop constraint $name;"
	createPropertyIndexStatement      = "call db.createIndex($name, $labels, $properties, $provider);"
	createPropertyConstraintStatement = "call db.createUniquePropertyConstraint($name, $labels, $properties, $provider);"
)

type neo4jIndex struct {
	v2.Index

	kind graph.Kind
}

type neo4jConstraint struct {
	v2.Constraint

	kind graph.Kind
}

type neo4jSchema struct {
	Indexes     map[string]neo4jIndex
	Constraints map[string]neo4jConstraint
}

func newNeo4jSchema() neo4jSchema {
	return neo4jSchema{
		Indexes:     map[string]neo4jIndex{},
		Constraints: map[string]neo4jConstraint{},
	}
}

func toNeo4jSchema(dbSchema v2.Schema) neo4jSchema {
	neo4jSchemaInst := newNeo4jSchema()

	for _, graphSchema := range dbSchema.Graphs {
		for _, index := range graphSchema.NodeIndexes {
			for _, kind := range graphSchema.Nodes {
				indexName := strings.ToLower(kind.String()) + "_" + strings.ToLower(index.Field) + "_index"

				neo4jSchemaInst.Indexes[indexName] = neo4jIndex{
					Index: v2.Index{
						Name:  indexName,
						Field: index.Field,
						Type:  index.Type,
					},
					kind: kind,
				}
			}
		}

		for _, constraint := range graphSchema.NodeConstraints {
			for _, kind := range graphSchema.Nodes {
				constraintName := strings.ToLower(kind.String()) + "_" + strings.ToLower(constraint.Field) + "_constraint"

				neo4jSchemaInst.Constraints[constraintName] = neo4jConstraint{
					Constraint: v2.Constraint{
						Name:  constraintName,
						Field: constraint.Field,
						Type:  constraint.Type,
					},
					kind: kind,
				}
			}
		}
	}

	return neo4jSchemaInst
}

func parseProviderType(provider string) v2.IndexType {
	switch provider {
	case nativeBTreeIndexProvider:
		return v2.IndexTypeBTree
	case nativeLuceneIndexProvider:
		return v2.IndexTypeTextSearch
	default:
		return v2.IndexTypeUnsupported
	}
}

func indexTypeProvider(indexType v2.IndexType) string {
	switch indexType {
	case v2.IndexTypeBTree:
		return nativeBTreeIndexProvider
	case v2.IndexTypeTextSearch:
		return nativeLuceneIndexProvider
	default:
		return ""
	}
}

type SchemaManager struct {
	internalDriver neo4j.DriverWithContext
}

func NewSchemaManager(internalDriver neo4j.DriverWithContext) *SchemaManager {
	return &SchemaManager{
		internalDriver: internalDriver,
	}
}

func (s *SchemaManager) transaction(ctx context.Context, transactionHandler func(transaction neo4j.ExplicitTransaction) error) error {
	session := s.internalDriver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})

	defer func() {
		if err := session.Close(ctx); err != nil {
			slog.DebugContext(ctx, "failed to close session", slog.String("err", err.Error()))
		}
	}()

	// Acquire a new transaction
	if transaction, err := session.BeginTransaction(ctx); err != nil {
		return err
	} else {
		defer func() {
			if err := transaction.Rollback(ctx); err != nil {
				slog.DebugContext(ctx, "failed to rollback transaction", slog.String("err", err.Error()))
			}
		}()

		if err := transactionHandler(transaction); err != nil {
			return err
		}

		return transaction.Commit(ctx)
	}
}

func (s *SchemaManager) assertIndexes(ctx context.Context, indexesToRemove []string, indexesToAdd map[string]neo4jIndex) error {
	if err := s.transaction(ctx, func(transaction neo4j.ExplicitTransaction) error {
		for _, indexToRemove := range indexesToRemove {
			slog.InfoContext(ctx, fmt.Sprintf("Removing index %s", indexToRemove))

			if result, err := transaction.Run(ctx, strings.Replace(dropPropertyIndexStatement, "$name", indexToRemove, 1), nil); err != nil {
				return err
			} else if _, err := result.Consume(ctx); err != nil {
				return err
			} else if err := result.Err(); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return s.transaction(ctx, func(transaction neo4j.ExplicitTransaction) error {
		for indexName, indexToAdd := range indexesToAdd {
			slog.InfoContext(ctx, fmt.Sprintf("Adding index %s to labels %s on properties %s using %s", indexName, indexToAdd.kind.String(), indexToAdd.Field, indexTypeProvider(indexToAdd.Type)))

			if result, err := transaction.Run(ctx, createPropertyIndexStatement, map[string]interface{}{
				"name":       indexName,
				"labels":     []string{indexToAdd.kind.String()},
				"properties": []string{indexToAdd.Field},
				"provider":   indexTypeProvider(indexToAdd.Type),
			}); err != nil {
				return err
			} else if _, err := result.Consume(ctx); err != nil {
				return err
			} else if err := result.Err(); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *SchemaManager) assertConstraints(ctx context.Context, constraintsToRemove []string, constraintsToAdd map[string]neo4jConstraint) error {
	return s.transaction(ctx, func(transaction neo4j.ExplicitTransaction) error {
		for _, constraintToRemove := range constraintsToRemove {
			if result, err := transaction.Run(ctx, strings.Replace(dropPropertyConstraintStatement, "$name", constraintToRemove, 1), nil); err != nil {
				return err
			} else if _, err := result.Consume(ctx); err != nil {
				return err
			} else if err := result.Err(); err != nil {
				return err
			}
		}

		for constraintName, constraintToAdd := range constraintsToAdd {
			if result, err := transaction.Run(ctx, createPropertyConstraintStatement, map[string]interface{}{
				"name":       constraintName,
				"labels":     []string{constraintToAdd.kind.String()},
				"properties": []string{constraintToAdd.Field},
				"provider":   indexTypeProvider(constraintToAdd.Type),
			}); err != nil {
				return err
			} else if _, err := result.Consume(ctx); err != nil {
				return err
			} else if err := result.Err(); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *SchemaManager) fetchPresentSchema(ctx context.Context) (neo4jSchema, error) {
	var (
		presentSchema = newNeo4jSchema()
		err           = s.transaction(ctx, func(transaction neo4j.ExplicitTransaction) error {
			if result, err := transaction.Run(ctx, "call db.indexes() yield name, uniqueness, provider, labelsOrTypes, properties;", nil); err != nil {
				return err
			} else {
				defer result.Consume(ctx)

				var (
					name       string
					uniqueness string
					provider   string
					labels     []string
					properties []string
				)

				for result.Next(ctx) {
					values := result.Record().Values

					if !neo4j2.NewValueMapper().MapAll(values, []any{&name, &uniqueness, &provider, &labels, &properties}) {
						return errors.New("failed to map present schema")
					}

					// Need this for neo4j 4.4+ which creates a weird index by default
					if len(labels) == 0 {
						continue
					}

					if len(labels) > 1 || len(properties) > 1 {
						return fmt.Errorf("composite index types are currently not supported")
					}

					if uniqueness == "UNIQUE" {
						presentSchema.Constraints[name] = neo4jConstraint{
							Constraint: v2.Constraint{
								Name:  name,
								Field: properties[0],
								Type:  parseProviderType(provider),
							},
							kind: graph.StringKind(labels[0]),
						}
					} else {
						presentSchema.Indexes[name] = neo4jIndex{
							Index: v2.Index{
								Name:  name,
								Field: properties[0],
								Type:  parseProviderType(provider),
							},
							kind: graph.StringKind(labels[0]),
						}
					}
				}

				return result.Err()
			}
		})
	)

	return presentSchema, err
}

func (s *SchemaManager) AssertSchema(ctx context.Context, required v2.Schema) error {
	requiredNeo4jSchema := toNeo4jSchema(required)

	if presentNeo4jSchema, err := s.fetchPresentSchema(ctx); err != nil {
		return err
	} else {
		var (
			indexesToRemove     []string
			constraintsToRemove []string
			indexesToAdd        = map[string]neo4jIndex{}
			constraintsToAdd    = map[string]neo4jConstraint{}
		)

		for presentIndexName := range presentNeo4jSchema.Indexes {
			if _, hasMatchingDefinition := requiredNeo4jSchema.Indexes[presentIndexName]; !hasMatchingDefinition {
				indexesToRemove = append(indexesToRemove, presentIndexName)
			}
		}

		for presentConstraintName := range presentNeo4jSchema.Constraints {
			if _, hasMatchingDefinition := requiredNeo4jSchema.Constraints[presentConstraintName]; !hasMatchingDefinition {
				constraintsToRemove = append(constraintsToRemove, presentConstraintName)
			}
		}

		for requiredIndexName, requiredIndex := range requiredNeo4jSchema.Indexes {
			if presentIndex, hasMatchingDefinition := presentNeo4jSchema.Indexes[requiredIndexName]; !hasMatchingDefinition {
				indexesToAdd[requiredIndexName] = requiredIndex
			} else if requiredIndex.Type != presentIndex.Type {
				indexesToRemove = append(indexesToRemove, requiredIndexName)
				indexesToAdd[requiredIndexName] = requiredIndex
			}
		}

		for requiredConstraintName, requiredConstraint := range requiredNeo4jSchema.Constraints {
			if presentConstraint, hasMatchingDefinition := presentNeo4jSchema.Constraints[requiredConstraintName]; !hasMatchingDefinition {
				constraintsToAdd[requiredConstraintName] = requiredConstraint
			} else if requiredConstraint.Type != presentConstraint.Type {
				constraintsToRemove = append(constraintsToRemove, requiredConstraintName)
				constraintsToAdd[requiredConstraintName] = requiredConstraint
			}
		}

		if err := s.assertConstraints(ctx, constraintsToRemove, constraintsToAdd); err != nil {
			return err
		}

		return s.assertIndexes(ctx, indexesToRemove, indexesToAdd)
	}
}
