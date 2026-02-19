package v1compat

import (
	"context"
	"fmt"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"
)

// typedSliceToAnySlice converts a given slice to a slice of type []any
func typedSliceToAnySlice[T any](slice []T) any {
	anyCopy := make([]any, len(slice))

	for idx := 0; idx < len(slice); idx++ {
		anyCopy[idx] = slice[idx]
	}

	return anyCopy
}

// downcastPropertyFields is a dawgs version 1 compatibility tool that emulates the JSON encode/decode pass for an
// entity's properties. This is done via a type cast to avoid the compute cost of actually running the JSON
// encode/decode.
func downcastPropertyFields(props *Properties) {
	for key, value := range props.MapOrEmpty() {
		switch typedValue := value.(type) {
		case []string:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []time.Time:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []bool:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []uint:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []uint8:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []uint16:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []uint32:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []uint64:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []int:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []int8:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []int16:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []int32:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []int64:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []float32:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		case []float64:
			props.Map[key] = typedSliceToAnySlice(typedValue)
		}
	}
}

type BackwardCompatibleInstance interface {
	database.Instance

	RefreshKinds(ctx context.Context) error
	Raw(ctx context.Context, query string, parameters map[string]any) error
}

type BackwardCompatibleDriver interface {
	database.Driver

	UpdateNodes(ctx context.Context, batch []graph.NodeUpdate) error
	UpdateRelationships(ctx context.Context, batch []graph.RelationshipUpdate) error
	CreateNodes(ctx context.Context, batch []*Node) error
	CreateRelationships(ctx context.Context, batch []*Relationship) error
	DeleteNodes(ctx context.Context, batch []graph.ID) error
	DeleteRelationships(ctx context.Context, batch []graph.ID) error
}

type v1Wrapper struct {
	schema         *database.Schema
	v2DB           BackwardCompatibleInstance
	writeFlushSize int
	batchWriteSize int
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

func (s *v1Wrapper) V2() database.Instance {
	return s.v2DB
}

func (s *v1Wrapper) ReadTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return txDelegate(wrapDriverToTransaction(ctx, driver))
	}, database.OptionReadOnly)
}

func (s *v1Wrapper) WriteTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		return txDelegate(wrapDriverToTransaction(ctx, driver))
	})
}

func (s *v1Wrapper) BatchOperation(ctx context.Context, batchDelegate BatchDelegate) error {
	return s.v2DB.Session(ctx, func(ctx context.Context, driver database.Driver) error {
		var (
			batchWrapper = wrapDriverToBatch(ctx, driver)
			delegateErr  = batchDelegate(batchWrapper)
		)

		if delegateErr != nil {
			return delegateErr
		}

		return batchWrapper.tryFlush(0)
	})
}

func (s *v1Wrapper) AssertSchema(ctx context.Context, dbSchema database.Schema) error {
	s.schema = &dbSchema
	return s.v2DB.AssertSchema(ctx, dbSchema)
}

func (s *v1Wrapper) SetDefaultGraph(ctx context.Context, graphSchema database.Graph) error {
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

func (s *v1Wrapper) Run(ctx context.Context, query string, parameters map[string]any) error {
	return s.v2DB.Raw(ctx, query, parameters)
}

func (s *v1Wrapper) Close(ctx context.Context) error {
	return s.v2DB.Close(ctx)
}

func (s *v1Wrapper) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	return s.v2DB.FetchKinds(ctx)
}

func (s *v1Wrapper) RefreshKinds(ctx context.Context) error {
	return s.v2DB.RefreshKinds(ctx)
}

func (s *v1Wrapper) SetWriteFlushSize(interval int) {
	s.writeFlushSize = interval
}

func (s *v1Wrapper) SetBatchWriteSize(interval int) {
	s.batchWriteSize = interval
}

type driverBatchWrapper struct {
	ctx    context.Context
	driver BackwardCompatibleDriver

	nodeDeletionBuffer         []graph.ID
	relationshipDeletionBuffer []graph.ID
	nodeCreateBuffer           []*graph.Node
	nodeUpdateByBuffer         []graph.NodeUpdate
	relationshipCreateBuffer   []*graph.Relationship
	relationshipUpdateByBuffer []graph.RelationshipUpdate
	batchWriteSize             int
}

func wrapDriverToBatch(ctx context.Context, driver database.Driver) *driverBatchWrapper {
	if v1CompatibleDriverRef, typeOK := driver.(BackwardCompatibleDriver); !typeOK {
		panic(fmt.Sprintf("type %T is not a v1CompatibleDriver", driver))
	} else {
		return &driverBatchWrapper{
			ctx:            ctx,
			driver:         v1CompatibleDriverRef,
			batchWriteSize: 2000,
		}
	}
}

func (s *driverBatchWrapper) tryFlush(batchWriteSize int) error {
	if len(s.nodeUpdateByBuffer) >= batchWriteSize {
		if err := s.driver.UpdateNodes(s.ctx, s.nodeUpdateByBuffer); err != nil {
			return err
		}

		s.nodeUpdateByBuffer = s.nodeUpdateByBuffer[:0]
	}

	if len(s.relationshipUpdateByBuffer) >= batchWriteSize {
		if err := s.driver.UpdateRelationships(s.ctx, s.relationshipUpdateByBuffer); err != nil {
			return err
		}

		s.relationshipUpdateByBuffer = s.relationshipUpdateByBuffer[:0]
	}

	if len(s.relationshipCreateBuffer) >= batchWriteSize {
		if err := s.driver.CreateRelationships(s.ctx, s.relationshipCreateBuffer); err != nil {
			return err
		}

		s.relationshipCreateBuffer = s.relationshipCreateBuffer[:0]
	}

	if len(s.nodeCreateBuffer) >= batchWriteSize {
		if err := s.driver.CreateNodes(s.ctx, s.nodeCreateBuffer); err != nil {
			return err
		}

		s.nodeCreateBuffer = s.nodeCreateBuffer[:0]
	}

	if len(s.nodeDeletionBuffer) >= batchWriteSize {
		if err := s.driver.DeleteNodes(s.ctx, s.nodeDeletionBuffer); err != nil {
			return err
		}

		s.nodeDeletionBuffer = s.nodeDeletionBuffer[:0]
	}

	if len(s.relationshipDeletionBuffer) >= batchWriteSize {
		if err := s.driver.DeleteRelationships(s.ctx, s.relationshipDeletionBuffer); err != nil {
			return err
		}

		s.relationshipDeletionBuffer = s.relationshipDeletionBuffer[:0]
	}

	return nil
}

func (s *driverBatchWrapper) WithGraph(graphSchema database.Graph) Batch {
	s.driver.WithGraph(graphSchema)
	return s
}

func (s *driverBatchWrapper) CreateNode(node *graph.Node) error {
	_, err := s.driver.CreateNode(s.ctx, node)
	return err
}

func (s *driverBatchWrapper) DeleteNode(id graph.ID) error {
	s.nodeDeletionBuffer = append(s.nodeDeletionBuffer, id)
	return s.tryFlush(s.batchWriteSize)
}

func (s *driverBatchWrapper) Nodes() NodeQuery {
	return newNodeQuery(s.ctx, s.driver)
}

func (s *driverBatchWrapper) Relationships() RelationshipQuery {
	return newRelationshipQuery(s.ctx, s.driver)
}

func (s *driverBatchWrapper) UpdateNodeBy(update graph.NodeUpdate) error {
	s.nodeUpdateByBuffer = append(s.nodeUpdateByBuffer, update)
	return s.tryFlush(s.batchWriteSize)
}

func (s *driverBatchWrapper) CreateRelationship(relationship *graph.Relationship) error {
	s.relationshipCreateBuffer = append(s.relationshipCreateBuffer, relationship)
	return s.tryFlush(s.batchWriteSize)
}

func (s *driverBatchWrapper) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	return s.CreateRelationship(&graph.Relationship{
		StartID:    startNodeID,
		EndID:      endNodeID,
		Kind:       kind,
		Properties: properties,
	})
}

func (s *driverBatchWrapper) DeleteRelationship(id graph.ID) error {
	s.relationshipDeletionBuffer = append(s.relationshipDeletionBuffer, id)
	return s.tryFlush(s.batchWriteSize)
}

func (s *driverBatchWrapper) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	s.relationshipUpdateByBuffer = append(s.relationshipUpdateByBuffer, update)
	return s.tryFlush(s.batchWriteSize)
}

func (s *driverBatchWrapper) Commit() error {
	return s.tryFlush(0)
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
		downcastPropertyFields(newNode.Properties)

		return newNode, nil
	}
}

func (s driverTransactionWrapper) UpdateNode(node *graph.Node) error {
	updateQuery := query.New().Where(query.Node().ID().Equals(node.ID))

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
		defer result.Close(s.ctx)

		return result.Error()
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
		downcastPropertyFields(newRelationship.Properties)

		return newRelationship, nil
	}
}

func (s driverTransactionWrapper) UpdateRelationship(relationship *graph.Relationship) error {
	updateQuery := query.New().Where(query.Relationship().ID().Equals(relationship.ID))

	if modifiedProperties := relationship.Properties.ModifiedProperties(); len(modifiedProperties) > 0 {
		updateQuery.Update(query.Relationship().SetProperties(modifiedProperties))
	}

	if deletedProperties := relationship.Properties.DeletedProperties(); len(deletedProperties) > 0 {
		updateQuery.Update(query.Relationship().RemoveProperties(deletedProperties))
	}

	if buildQuery, err := updateQuery.Build(); err != nil {
		return err
	} else {
		result := s.driver.Exec(s.ctx, buildQuery.Query, buildQuery.Parameters)
		defer result.Close(s.ctx)

		return result.Error()
	}
}

func (s driverTransactionWrapper) Relationships() RelationshipQuery {
	return newRelationshipQuery(s.ctx, s.driver)
}

func (s driverTransactionWrapper) Query(query string, parameters map[string]any) Result {
	if cypherQuery, err := frontend.ParseCypher(frontend.NewContext(), query); err != nil {
		return NewErrorResult(err)
	} else {
		return wrapResult(s.ctx, s.driver.Exec(s.ctx, cypherQuery, parameters), s.driver.Mapper())
	}
}

func (s driverTransactionWrapper) Commit() error {
	return nil
}
