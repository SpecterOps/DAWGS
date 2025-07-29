package v1compat

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
)

type BackwardCompatibleInstance interface {
	database.Instance

	RefreshKinds(ctx context.Context) error
	Raw(ctx context.Context, query string, parameters map[string]any) error
}

type BackwardCompatibleDriver interface {
	database.Driver

	UpdateNodeBy(update graph.NodeUpdate) error
	UpdateRelationshipBy(update graph.RelationshipUpdate) error
}

type v1Wrapper struct {
	schema *database.Schema
	v2DB   BackwardCompatibleInstance
}

func (s v1Wrapper) ReadTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return txDelegate(wrapDriverToTransaction(ctx, driver))
	}, database.OptionReadOnly)
}

func (s v1Wrapper) WriteTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return txDelegate(wrapDriverToTransaction(ctx, driver))
	})
}

func (s v1Wrapper) BatchOperation(ctx context.Context, batchDelegate BatchDelegate) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return batchDelegate(wrapDriverToBatch(ctx, driver))
	})
}

func (s v1Wrapper) AssertSchema(ctx context.Context, dbSchema database.Schema) error {
	s.schema = &dbSchema
	return s.v2DB.AssertSchema(ctx, dbSchema)
}

func (s v1Wrapper) SetDefaultGraph(ctx context.Context, graphSchema database.Graph) error {
	if s.schema != nil {
		s.schema.GraphSchemas[graphSchema.Name] = graphSchema
		s.schema.DefaultGraphName = graphSchema.Name
	} else {
		s.schema = &database.Schema{
			GraphSchemas: map[string]database.Graph{
				graphSchema.Name: graphSchema,
			},
			DefaultGraphName: graphSchema.Name,
		}
	}

	return s.v2DB.AssertSchema(ctx, *s.schema)
}

func (s v1Wrapper) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.v2DB.Raw(ctx, query, parameters)
}

func (s v1Wrapper) Close(ctx context.Context) error {
	return s.v2DB.Close(ctx)
}

func (s v1Wrapper) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	return s.v2DB.FetchKinds(ctx)
}

func (s v1Wrapper) RefreshKinds(ctx context.Context) error {
	return s.v2DB.RefreshKinds(ctx)
}

func V1Wrapper(v2DB database.Instance) Database {
	if v1CompatibleInstanceRef, typeOK := v2DB.(BackwardCompatibleInstance); !typeOK {
		panic(fmt.Sprintf("type %T is not a v1CompatibleInstance", v2DB))
	} else {
		return &v1Wrapper{
			v2DB: v1CompatibleInstanceRef,
		}
	}
}

func (s v1Wrapper) SetWriteFlushSize(interval int) {
	// NOOP
}

func (s v1Wrapper) SetBatchWriteSize(interval int) {
	// NOOP
}

type driverBatchWrapper struct {
	ctx    context.Context
	driver BackwardCompatibleDriver
}

func wrapDriverToBatch(ctx context.Context, driver database.Driver) Batch {
	if v1CompatibleDriverRef, typeOK := driver.(BackwardCompatibleDriver); !typeOK {
		panic(fmt.Sprintf("type %T is not a v1CompatibleDriver", driver))
	} else {
		return &driverBatchWrapper{
			ctx:    ctx,
			driver: v1CompatibleDriverRef,
		}
	}
}

func (s driverBatchWrapper) WithGraph(graphSchema database.Graph) Batch {
	s.driver.WithGraph(graphSchema)
	return s
}

func (s driverBatchWrapper) CreateNode(node *graph.Node) error {
	_, err := s.driver.CreateNode(s.ctx, node)
	return err
}

func (s driverBatchWrapper) DeleteNode(id graph.ID) error {
	if builtQuery, err := query.New().Where(
		query.Node().ID().Equals(id),
	).Delete(query.Node()).Build(); err != nil {
		return err
	} else {
		result := s.driver.Exec(s.ctx, builtQuery.Query, builtQuery.Parameters)
		defer result.Close(s.ctx)

		return result.Error()
	}
}

func (s driverBatchWrapper) Nodes() NodeQuery {
	return newNodeQuery(s.ctx, s.driver)
}

func (s driverBatchWrapper) Relationships() RelationshipQuery {
	return newRelationshipQuery(s.ctx, s.driver)
}

func (s driverBatchWrapper) UpdateNodeBy(update graph.NodeUpdate) error {
	return s.driver.UpdateNodeBy(update)
}

func (s driverBatchWrapper) CreateRelationship(relationship *graph.Relationship) error {
	_, err := s.driver.CreateRelationship(s.ctx, relationship)
	return err
}

func (s driverBatchWrapper) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	return s.CreateRelationship(&graph.Relationship{
		StartID:    startNodeID,
		EndID:      endNodeID,
		Kind:       kind,
		Properties: properties,
	})
}

func (s driverBatchWrapper) DeleteRelationship(id graph.ID) error {
	if deleteQuery, err := query.New().Where(
		query.Relationship().ID().Equals(id),
	).Build(); err != nil {
		return err
	} else {
		return s.driver.Exec(s.ctx, deleteQuery.Query, deleteQuery.Parameters).Close(s.ctx)
	}
}

func (s driverBatchWrapper) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	return s.driver.UpdateRelationshipBy(update)
}

func (s driverBatchWrapper) Commit() error {
	return nil
}

type driverTransactionWrapper struct {
	ctx    context.Context
	driver BackwardCompatibleDriver
}

func (s driverTransactionWrapper) GraphQueryMemoryLimit() size.Size {
	return size.Gibibyte
}

func wrapDriverToTransaction(ctx context.Context, driver database.Driver) Transaction {
	if v1CompatibleDriverRef, typeOK := driver.(BackwardCompatibleDriver); !typeOK {
		panic(fmt.Sprintf("type %T is not a v1CompatibleDriver", driver))
	} else {
		return &driverTransactionWrapper{
			ctx:    ctx,
			driver: v1CompatibleDriverRef,
		}
	}
}

func (s driverTransactionWrapper) WithGraph(graphSchema database.Graph) Transaction {
	s.driver.WithGraph(graphSchema)
	return s
}

func (s driverTransactionWrapper) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	newNode := graph.PrepareNode(properties, kinds...)

	if nodeID, err := s.driver.CreateNode(s.ctx, newNode); err != nil {
		return nil, err
	} else {
		newNode.ID = nodeID
	}

	return newNode, nil
}

func (s driverTransactionWrapper) UpdateNode(node *graph.Node) error {
	updateQuery := query.New()

	if len(node.AddedKinds) > 0 {
		updateQuery.Update(query.Node().Kinds().Add(node.AddedKinds))
	}

	if len(node.DeletedKinds) > 0 {
		updateQuery.Update(query.Node().Kinds().Remove(node.DeletedKinds))
	}

	if modifiedProperties := node.Properties.ModifiedProperties(); len(modifiedProperties) > 0 {
		updateQuery.Update(query.Node().SetProperties(modifiedProperties))
	}

	if deletedProperties := node.Properties.DeletedProperties(); len(deletedProperties) > 0 {
		updateQuery.Update(query.Node().RemoveProperties(deletedProperties))
	}

	if buildQuery, err := updateQuery.Build(); err != nil {
		return err
	} else {
		result := s.driver.Exec(s.ctx, buildQuery.Query, buildQuery.Parameters)
		return result.Close(s.ctx)
	}
}

func (s driverTransactionWrapper) Nodes() NodeQuery {
	return newNodeQuery(s.ctx, s.driver)
}

func (s driverTransactionWrapper) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	newRelationship := &graph.Relationship{
		StartID:    startNodeID,
		EndID:      endNodeID,
		Kind:       kind,
		Properties: properties,
	}

	if newRelationshipID, err := s.driver.CreateRelationship(s.ctx, newRelationship); err != nil {
		return nil, err
	} else {
		newRelationship.ID = newRelationshipID
		return newRelationship, nil
	}
}

func (s driverTransactionWrapper) UpdateRelationship(relationship *graph.Relationship) error {
	updateQuery := query.New()

	if modifiedProperties := relationship.Properties.ModifiedProperties(); len(modifiedProperties) > 0 {
		updateQuery.Update(query.Node().SetProperties(modifiedProperties))
	}

	if deletedProperties := relationship.Properties.DeletedProperties(); len(deletedProperties) > 0 {
		updateQuery.Update(query.Node().RemoveProperties(deletedProperties))
	}

	if buildQuery, err := updateQuery.Build(); err != nil {
		return err
	} else {
		result := s.driver.Exec(s.ctx, buildQuery.Query, buildQuery.Parameters)
		return result.Close(s.ctx)
	}
}

func (s driverTransactionWrapper) Relationships() RelationshipQuery {
	return newRelationshipQuery(s.ctx, s.driver)
}

func (s driverTransactionWrapper) Query(query string, parameters map[string]any) Result {
	if cypherQuery, err := frontend.ParseCypher(frontend.NewContext(), query); err != nil {
		return NewErrorResult(err)
	} else {
		return wrapResult(s.driver.Exec(s.ctx, cypherQuery, parameters))
	}
}

func (s driverTransactionWrapper) Commit() error {
	return nil
}
