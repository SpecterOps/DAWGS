package v1compat

import (
	"context"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
)

type v1Wrapper struct {
	v2DB database.Instance
}

func (s v1Wrapper) ReadTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) WriteTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) BatchOperation(ctx context.Context, batchDelegate BatchDelegate) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) AssertSchema(ctx context.Context, dbSchema database.Schema) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) SetDefaultGraph(ctx context.Context, graphSchema database.Graph) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) Run(ctx context.Context, query string, parameters map[string]any) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	//TODO implement me
	panic("implement me")
}

func (s v1Wrapper) RefreshKinds(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func V1Wrapper(v2DB database.Instance) Database {
	return &v1Wrapper{
		v2DB: v2DB,
	}
}

func (s v1Wrapper) SetWriteFlushSize(interval int) {
	// NOOP
}

func (s v1Wrapper) SetBatchWriteSize(interval int) {
	// NOOP
}

type driverTransactionWrapper struct {
	ctx    context.Context
	driver database.Driver
}

func wrapDriverToTransaction(ctx context.Context, driver database.Driver) Transaction {
	return &driverTransactionWrapper{
		ctx:    ctx,
		driver: driver,
	}
}

func (s driverTransactionWrapper) WithGraph(graphSchema database.Graph) Transaction {
	s.driver = s.driver.WithGraph(graphSchema)
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
	//TODO implement me
	panic("implement me")
}

func (s driverTransactionWrapper) Nodes() NodeQuery {
	return newNodeQuery(s.driver)
}

func (s driverTransactionWrapper) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	//TODO implement me
	panic("implement me")
}

func (s driverTransactionWrapper) UpdateRelationship(relationship *graph.Relationship) error {
	//TODO implement me
	panic("implement me")
}

func (s driverTransactionWrapper) Relationships() RelationshipQuery {
	return newRelationshipQuery(s.driver)
}

func (s driverTransactionWrapper) Raw(query string, parameters map[string]any) Result {
	//TODO implement me
	panic("implement me")
}

func (s driverTransactionWrapper) Query(query string, parameters map[string]any) Result {
	//TODO implement me
	panic("implement me")
}

func (s driverTransactionWrapper) Commit() error {
	return nil
}