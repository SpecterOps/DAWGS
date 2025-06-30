package query

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	v2 "github.com/specterops/dawgs/v2"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg/v2/model"
	"github.com/specterops/dawgs/graph"
)

type Query struct {
	tx pgx.Tx
}

func On(tx pgx.Tx) Query {
	return Query{
		tx: tx,
	}
}

func (s Query) exec(ctx context.Context, statement string, args map[string]any, queryArgs ...any) error {
	if args != nil && len(args) > 0 {
		queryArgs = append(queryArgs, args)
	}

	_, err := s.tx.Exec(ctx, statement, queryArgs...)
	return err
}

func (s Query) query(ctx context.Context, statement string, args map[string]any, queryArgs ...any) (pgx.Rows, error) {
	if args != nil && len(args) > 0 {
		queryArgs = append(queryArgs, pgx.NamedArgs(args))
	}

	return s.tx.Query(ctx, statement, queryArgs...)
}

func (s Query) describeGraphPartition(ctx context.Context, name string) (model.GraphPartition, error) {
	graphPartition := model.NewGraphPartition(name)

	if tableIndexDefinitions, err := s.SelectTableIndexDefinitions(ctx, name); err != nil {
		return graphPartition, err
	} else {
		for _, tableIndexDefinition := range tableIndexDefinitions {
			if captureGroups := pgPropertyIndexRegex.FindStringSubmatch(tableIndexDefinition); captureGroups == nil {
				// If this index does not match our expected column index format then report it as a potential error
				if !pgColumnIndexRegex.MatchString(tableIndexDefinition) {
					return graphPartition, fmt.Errorf("regex mis-match on schema definition: %s", tableIndexDefinition)
				}
			} else {
				indexName := captureGroups[pgIndexRegexGroupName]

				if captureGroups[pgIndexRegexGroupUnique] == pgIndexUniqueStr {
					graphPartition.Constraints[indexName] = v2.Constraint{
						Name:  indexName,
						Field: captureGroups[pgIndexRegexGroupFields],
						Type:  parsePostgresIndexType(captureGroups[pgIndexRegexGroupIndexType]),
					}
				} else {
					graphPartition.Indexes[indexName] = v2.Index{
						Name:  indexName,
						Field: captureGroups[pgIndexRegexGroupFields],
						Type:  parsePostgresIndexType(captureGroups[pgIndexRegexGroupIndexType]),
					}
				}
			}
		}
	}

	return graphPartition, nil
}

func (s Query) SelectKinds(ctx context.Context) (map[graph.Kind]int16, error) {
	var (
		kindID   int16
		kindName string

		kinds = map[graph.Kind]int16{}
	)

	if result, err := s.query(ctx, sqlSelectKinds, nil); err != nil {
		return nil, err
	} else {
		defer result.Close()

		for result.Next() {
			if err := result.Scan(&kindID, &kindName); err != nil {
				return nil, err
			}

			kinds[graph.StringKind(kindName)] = kindID
		}

		return kinds, result.Err()
	}
}

func (s Query) selectGraphPartitions(ctx context.Context, graphID int32) (model.GraphPartitions, error) {
	var (
		nodePartitionName = model.NodePartitionTableName(graphID)
		edgePartitionName = model.EdgePartitionTableName(graphID)
	)

	if nodePartition, err := s.describeGraphPartition(ctx, nodePartitionName); err != nil {
		return model.GraphPartitions{}, err
	} else if edgePartition, err := s.describeGraphPartition(ctx, edgePartitionName); err != nil {
		return model.GraphPartitions{}, err
	} else {
		return model.GraphPartitions{
			Node: nodePartition,
			Edge: edgePartition,
		}, nil
	}
}

func (s Query) selectGraphPartialByName(ctx context.Context, name string) (model.Graph, error) {
	var graphID int32

	if result, err := s.query(ctx, sqlSelectGraphByName, pgx.NamedArgs(map[string]any{
		"name": name,
	})); err != nil {
		return model.Graph{}, err
	} else {
		defer result.Close()

		if !result.Next() {
			if err := result.Err(); err != nil {
				return model.Graph{}, err
			}

			return model.Graph{}, pgx.ErrNoRows
		}

		if err := result.Scan(&graphID); err != nil {
			return model.Graph{}, err
		}

		return model.Graph{
			ID:   graphID,
			Name: name,
		}, result.Err()
	}
}

func (s Query) SelectGraphByName(ctx context.Context, name string) (model.Graph, error) {
	if definition, err := s.selectGraphPartialByName(ctx, name); err != nil {
		return model.Graph{}, err
	} else if graphPartitions, err := s.selectGraphPartitions(ctx, definition.ID); err != nil {
		return model.Graph{}, err
	} else {
		definition.Partitions = graphPartitions
		return definition, nil
	}
}

func (s Query) selectGraphPartials(ctx context.Context) ([]model.Graph, error) {
	var (
		graphID   int32
		graphName string
		graphs    []model.Graph
	)

	if result, err := s.query(ctx, sqlSelectGraphs, nil); err != nil {
		return nil, err
	} else {
		defer result.Close()

		for result.Next() {
			if err := result.Scan(&graphID, &graphName); err != nil {
				return nil, err
			} else {
				graphs = append(graphs, model.Graph{
					ID:   graphID,
					Name: graphName,
				})
			}
		}

		return graphs, result.Err()
	}
}

func (s Query) SelectGraphs(ctx context.Context) (map[string]model.Graph, error) {
	if definitions, err := s.selectGraphPartials(ctx); err != nil {
		return nil, err
	} else {
		indexed := map[string]model.Graph{}

		for _, definition := range definitions {
			if graphPartitions, err := s.selectGraphPartitions(ctx, definition.ID); err != nil {
				return nil, err
			} else {
				definition.Partitions = graphPartitions
				indexed[definition.Name] = definition
			}
		}

		return indexed, nil
	}
}

func (s Query) CreatePropertyIndex(ctx context.Context, indexName, tableName, fieldName string, indexType v2.IndexType) error {
	return s.exec(ctx, formatCreatePropertyIndex(indexName, tableName, fieldName, indexType), nil)
}

func (s Query) CreatePropertyConstraint(ctx context.Context, indexName, tableName, fieldName string, indexType v2.IndexType) error {
	if indexType != v2.IndexTypeBTree {
		return fmt.Errorf("only b-tree indexing is supported for property constraints")
	}

	return s.exec(ctx, formatCreatePropertyConstraint(indexName, tableName, fieldName, indexType), nil)
}

func (s Query) DropIndex(ctx context.Context, indexName string) error {
	return s.exec(ctx, formatDropPropertyIndex(indexName), nil)
}

func (s Query) DropConstraint(ctx context.Context, constraintName string) error {
	return s.exec(ctx, formatDropPropertyConstraint(constraintName), nil)
}

func (s Query) CreateSchema(ctx context.Context) error {
	if err := s.exec(ctx, sqlSchemaUp, nil); err != nil {
		return err
	}

	return nil
}

func (s Query) DropSchema(ctx context.Context) error {
	if err := s.exec(ctx, sqlSchemaDown, nil); err != nil {
		return err
	}

	return nil
}

func (s Query) insertGraph(ctx context.Context, name string) (model.Graph, error) {
	var graphID int32

	if result, err := s.query(ctx, sqlInsertGraph, map[string]any{
		"name": name,
	}); err != nil {
		return model.Graph{}, err
	} else {
		defer result.Close()

		if !result.Next() {
			if err := result.Err(); err != nil {
				return model.Graph{}, err
			}

			return model.Graph{}, pgx.ErrNoRows
		}

		if err := result.Scan(&graphID); err != nil {
			return model.Graph{}, fmt.Errorf("failed mapping ID from graph entry creation: %w", err)
		}

		return model.Graph{
			ID:   graphID,
			Name: name,
		}, nil
	}
}

func (s Query) CreatePartitionTable(ctx context.Context, name, parent string, graphID int32) (model.GraphPartition, error) {
	if err := s.exec(ctx, formatCreatePartitionTable(name, parent, graphID), nil); err != nil {
		return model.GraphPartition{}, err
	}

	return model.GraphPartition{
		Name: name,
	}, nil
}

func (s Query) SelectTableIndexDefinitions(ctx context.Context, tableName string) ([]string, error) {
	var (
		nextDefinition string
		definitions    []string
	)

	if result, err := s.query(ctx, sqlSelectTableIndexes, map[string]any{
		"tablename": tableName,
	}); err != nil {
		return nil, err
	} else {

		defer result.Close()

		for result.Next() {
			if err := result.Scan(&nextDefinition); err != nil {
				return nil, err
			}

			definitions = append(definitions, strings.ToLower(nextDefinition))
		}

		return definitions, result.Err()
	}
}

func (s Query) SelectKindID(ctx context.Context, kind graph.Kind) (int16, error) {
	var kindID int16

	if result, err := s.query(ctx, sqlSelectKindID, map[string]any{
		"name": kind.String(),
	}); err != nil {
		return -1, err
	} else {
		defer result.Close()

		if !result.Next() {
			if err := result.Err(); err != nil {
				return -1, err
			}

			return -1, pgx.ErrNoRows
		}

		if err := result.Scan(&kindID); err != nil {
			return -1, err
		}

		return kindID, result.Err()
	}
}

func (s Query) assertGraphPartitionIndexes(ctx context.Context, partitions model.GraphPartitions, indexChanges model.IndexChangeSet) error {
	for _, indexToRemove := range append(indexChanges.NodeIndexesToRemove, indexChanges.EdgeIndexesToRemove...) {
		if err := s.DropIndex(ctx, indexToRemove); err != nil {
			return err
		}
	}

	for _, constraintToRemove := range append(indexChanges.NodeConstraintsToRemove, indexChanges.EdgeConstraintsToRemove...) {
		if err := s.DropConstraint(ctx, constraintToRemove); err != nil {
			return err
		}
	}

	for indexName, index := range indexChanges.NodeIndexesToAdd {
		if err := s.CreatePropertyIndex(ctx, indexName, partitions.Node.Name, index.Field, index.Type); err != nil {
			return err
		}
	}

	for constraintName, constraint := range indexChanges.NodeConstraintsToAdd {
		if err := s.CreatePropertyConstraint(ctx, constraintName, partitions.Node.Name, constraint.Field, constraint.Type); err != nil {
			return err
		}
	}

	for indexName, index := range indexChanges.EdgeIndexesToAdd {
		if err := s.CreatePropertyIndex(ctx, indexName, partitions.Edge.Name, index.Field, index.Type); err != nil {
			return err
		}
	}

	for constraintName, constraint := range indexChanges.EdgeConstraintsToAdd {
		if err := s.CreatePropertyConstraint(ctx, constraintName, partitions.Edge.Name, constraint.Field, constraint.Type); err != nil {
			return err
		}
	}

	return nil
}

func (s Query) AssertGraph(ctx context.Context, schema v2.Graph, definition model.Graph) (model.Graph, error) {
	var (
		requiredNodePartition = model.NewGraphPartitionFromSchema(definition.Partitions.Node.Name, schema.NodeIndexes, schema.NodeConstraints)
		requiredEdgePartition = model.NewGraphPartitionFromSchema(definition.Partitions.Edge.Name, schema.EdgeIndexes, schema.EdgeConstraints)
		indexChangeSet        = model.NewIndexChangeSet()
	)

	if presentNodePartition, err := s.describeGraphPartition(ctx, definition.Partitions.Node.Name); err != nil {
		return model.Graph{}, err
	} else {
		for presentNodeIndexName := range presentNodePartition.Indexes {
			if _, hasMatchingDefinition := requiredNodePartition.Indexes[presentNodeIndexName]; !hasMatchingDefinition {
				indexChangeSet.NodeIndexesToRemove = append(indexChangeSet.NodeIndexesToRemove, presentNodeIndexName)
			}
		}

		for presentNodeConstraintName := range presentNodePartition.Constraints {
			if _, hasMatchingDefinition := requiredNodePartition.Constraints[presentNodeConstraintName]; !hasMatchingDefinition {
				indexChangeSet.NodeConstraintsToRemove = append(indexChangeSet.NodeConstraintsToRemove, presentNodeConstraintName)
			}
		}

		for requiredNodeIndexName, requiredNodeIndex := range requiredNodePartition.Indexes {
			if presentNodeIndex, hasMatchingDefinition := presentNodePartition.Indexes[requiredNodeIndexName]; !hasMatchingDefinition {
				indexChangeSet.NodeIndexesToAdd[requiredNodeIndexName] = requiredNodeIndex
			} else if requiredNodeIndex.Type != presentNodeIndex.Type {
				indexChangeSet.NodeIndexesToRemove = append(indexChangeSet.NodeIndexesToRemove, requiredNodeIndexName)
				indexChangeSet.NodeIndexesToAdd[requiredNodeIndexName] = requiredNodeIndex
			}
		}

		for requiredNodeConstraintName, requiredNodeConstraint := range requiredNodePartition.Constraints {
			if presentNodeConstraint, hasMatchingDefinition := presentNodePartition.Constraints[requiredNodeConstraintName]; !hasMatchingDefinition {
				indexChangeSet.NodeConstraintsToAdd[requiredNodeConstraintName] = requiredNodeConstraint
			} else if requiredNodeConstraint.Type != presentNodeConstraint.Type {
				indexChangeSet.NodeConstraintsToRemove = append(indexChangeSet.NodeConstraintsToRemove, requiredNodeConstraintName)
				indexChangeSet.NodeConstraintsToAdd[requiredNodeConstraintName] = requiredNodeConstraint
			}
		}
	}

	if presentEdgePartition, err := s.describeGraphPartition(ctx, definition.Partitions.Edge.Name); err != nil {
		return model.Graph{}, err
	} else {
		for presentEdgeIndexName := range presentEdgePartition.Indexes {
			if _, hasMatchingDefinition := requiredEdgePartition.Indexes[presentEdgeIndexName]; !hasMatchingDefinition {
				indexChangeSet.EdgeIndexesToRemove = append(indexChangeSet.EdgeIndexesToRemove, presentEdgeIndexName)
			}
		}

		for presentEdgeConstraintName := range presentEdgePartition.Constraints {
			if _, hasMatchingDefinition := requiredEdgePartition.Constraints[presentEdgeConstraintName]; !hasMatchingDefinition {
				indexChangeSet.EdgeConstraintsToRemove = append(indexChangeSet.EdgeConstraintsToRemove, presentEdgeConstraintName)
			}
		}

		for requiredEdgeIndexName, requiredEdgeIndex := range requiredEdgePartition.Indexes {
			if presentEdgeIndex, hasMatchingDefinition := presentEdgePartition.Indexes[requiredEdgeIndexName]; !hasMatchingDefinition {
				indexChangeSet.EdgeIndexesToAdd[requiredEdgeIndexName] = requiredEdgeIndex
			} else if requiredEdgeIndex.Type != presentEdgeIndex.Type {
				indexChangeSet.EdgeIndexesToRemove = append(indexChangeSet.EdgeIndexesToRemove, requiredEdgeIndexName)
				indexChangeSet.EdgeIndexesToAdd[requiredEdgeIndexName] = requiredEdgeIndex
			}
		}

		for requiredEdgeConstraintName, requiredEdgeConstraint := range requiredEdgePartition.Constraints {
			if presentEdgeConstraint, hasMatchingDefinition := presentEdgePartition.Constraints[requiredEdgeConstraintName]; !hasMatchingDefinition {
				indexChangeSet.EdgeConstraintsToAdd[requiredEdgeConstraintName] = requiredEdgeConstraint
			} else if requiredEdgeConstraint.Type != presentEdgeConstraint.Type {
				indexChangeSet.EdgeConstraintsToRemove = append(indexChangeSet.EdgeConstraintsToRemove, requiredEdgeConstraintName)
				indexChangeSet.EdgeConstraintsToAdd[requiredEdgeConstraintName] = requiredEdgeConstraint
			}
		}
	}

	return model.Graph{
		ID:   definition.ID,
		Name: definition.Name,
		Partitions: model.GraphPartitions{
			Node: requiredNodePartition,
			Edge: requiredEdgePartition,
		},
	}, s.assertGraphPartitionIndexes(ctx, definition.Partitions, indexChangeSet)
}

func (s Query) createGraphPartitions(ctx context.Context, definition model.Graph) (model.Graph, error) {
	var (
		nodePartitionName = model.NodePartitionTableName(definition.ID)
		edgePartitionName = model.EdgePartitionTableName(definition.ID)
	)

	if nodePartition, err := s.CreatePartitionTable(ctx, nodePartitionName, model.NodeTable, definition.ID); err != nil {
		return model.Graph{}, err
	} else {
		definition.Partitions.Node = nodePartition
	}

	if edgePartition, err := s.CreatePartitionTable(ctx, edgePartitionName, model.EdgeTable, definition.ID); err != nil {
		return model.Graph{}, err
	} else {
		definition.Partitions.Edge = edgePartition
	}

	return definition, nil
}

func (s Query) CreateGraph(ctx context.Context, schema v2.Graph) (model.Graph, error) {
	if definition, err := s.insertGraph(ctx, schema.Name); err != nil {
		return model.Graph{}, err
	} else if graphPartitions, err := s.createGraphPartitions(ctx, definition); err != nil {
		return model.Graph{}, err
	} else {
		return s.AssertGraph(ctx, schema, graphPartitions)
	}
}

func (s Query) InsertOrGetKind(ctx context.Context, kind graph.Kind) (int16, error) {
	var kindID int16

	if result, err := s.query(ctx, sqlInsertKind, map[string]any{
		"name": kind.String(),
	}); err != nil {
		return -1, err
	} else {
		defer result.Close()

		if !result.Next() {
			if err := result.Err(); err != nil {
				return -1, err
			}

			return -1, pgx.ErrNoRows
		}

		if err := result.Scan(&kindID); err != nil {
			return -1, err
		}

		return kindID, result.Err()
	}
}
