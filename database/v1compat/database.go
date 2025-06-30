package v1compat

import (
	"context"
	"time"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

type Batch interface {
	// WithGraph scopes the transaction to a specific graph. If the driver for the transaction does not support
	// multiple  graphs the resulting transaction will target the default graph instead and this call becomes a no-op.
	WithGraph(graphSchema database.Graph) Batch

	// CreateNode creates a new Node in the database and returns the creation as a NodeResult.
	CreateNode(node *graph.Node) error

	// DeleteNode deletes a node by the given ID.
	DeleteNode(id graph.ID) error

	// Nodes begins a batch query that can be used to update or delete nodes.
	Nodes() NodeQuery

	// Relationships begins a batch query that can be used to update or delete relationships.
	Relationships() RelationshipQuery

	// UpdateNodeBy is a stop-gap until the query interface can better support targeted batch create-update operations.
	// Nodes identified by the NodeUpdate criteria will either be updated or in the case where the node does not yet
	// exist, created.
	UpdateNodeBy(update graph.NodeUpdate) error

	// TODO: Existing batch logic expects this to perform an upsert on conficts with (start_id, end_id, kind). This is incorrect and should be refactored
	CreateRelationship(relationship *graph.Relationship) error

	// Deprecated: Use CreateRelationship Instead
	//
	// CreateRelationshipByIDs creates a new Relationship from the start Node to the end Node with the given Kind and
	// Properties and returns the creation as a RelationshipResult.
	CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error

	// DeleteRelationship deletes a relationship by the given ID.
	DeleteRelationship(id graph.ID) error

	// UpdateRelationshipBy is a stop-gap until the query interface can better support targeted batch create-update
	// operations. Relationships identified by the RelationshipUpdate criteria will either be updated or in the case
	// where the relationship does not yet exist, created.
	UpdateRelationshipBy(update graph.RelationshipUpdate) error

	// Commit calls to commit this batch transaction right away.
	Commit() error
}

// Transaction is an interface that contains all operations that may be executed against a DAWGS driver. DAWGS drivers are
// expected to support all Transaction operations in-transaction.
type Transaction interface {
	// WithGraph scopes the transaction to a specific graph. If the driver for the transaction does not support
	// multiple  graphs the resulting transaction will target the default graph instead and this call becomes a no-op.
	WithGraph(graphSchema database.Graph) Transaction

	// CreateNode creates a new Node in the database and returns the creation as a NodeResult.
	CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error)

	// UpdateNode updates a Node in the database with the given Node by ID. UpdateNode will not create missing Node
	// entries in the database. Use CreateNode first to create a new Node.
	UpdateNode(node *graph.Node) error

	// Nodes creates a new NodeQuery and returns it.
	Nodes() NodeQuery

	// CreateRelationshipByIDs creates a new Relationship from the start Node to the end Node with the given Kind and
	// Properties and returns the creation as a RelationshipResult.
	CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error)

	// UpdateRelationship updates a Relationship in the database with the given Relationship by ID. UpdateRelationship
	// will not create missing Relationship entries in the database. Use CreateRelationship first to create a new
	// Relationship.
	UpdateRelationship(relationship *graph.Relationship) error

	// Relationships creates a new RelationshipQuery and returns it.
	Relationships() RelationshipQuery

	// Query allows a user to execute a given cypher query that will be translated to the target database.
	Query(query string, parameters map[string]any) Result

	// Commit calls to commit this transaction right away.
	Commit() error

	GraphQueryMemoryLimit() size.Size
}

// TransactionDelegate represents a transactional database context actor. Errors returned from a TransactionDelegate
// result in the rollback of write enabled transactions. Successful execution of a TransactionDelegate (nil error
// return value) results in a transactional commit of work done within the TransactionDelegate.
type TransactionDelegate func(tx Transaction) error

// BatchDelegate represents a transactional database context actor.
type BatchDelegate func(batch Batch) error

// TransactionConfig is a generic configuration that may apply to all supported databases.
type TransactionConfig struct {
	Timeout      time.Duration
	DriverConfig any
}

// TransactionOption is a function that represents a configuration setting for the underlying database transaction.
type TransactionOption func(config *TransactionConfig)

// Database is a high-level interface representing transactional entry-points into DAWGS driver implementations.
type Database interface {
	// SetWriteFlushSize sets a new write flush interval on the current driver
	SetWriteFlushSize(interval int)

	// SetBatchWriteSize sets a new batch write interval on the current driver
	SetBatchWriteSize(interval int)

	// ReadTransaction opens up a new read transactional context in the database and then defers the context to the
	// given logic function.
	ReadTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error

	// WriteTransaction opens up a new write transactional context in the database and then defers the context to the
	// given logic function.
	WriteTransaction(ctx context.Context, txDelegate TransactionDelegate, options ...TransactionOption) error

	// BatchOperation opens up a new write transactional context in the database and then defers the context to the
	// given logic function. Batch operations are fundamentally different between databases supported by DAWGS,
	// necessitating a different interface that lacks many of the convenience features of a regular read or write
	// transaction.
	BatchOperation(ctx context.Context, batchDelegate BatchDelegate) error

	// AssertSchema will apply the given schema to the underlying database.
	AssertSchema(ctx context.Context, dbSchema database.Schema) error

	// SetDefaultGraph sets the default graph namespace for the connection.
	SetDefaultGraph(ctx context.Context, graphSchema database.Graph) error

	// Run allows a user to pass statements directly to the database. Since results may rely on a transactional context
	// only an error is returned from this function
	Run(ctx context.Context, query string, parameters map[string]any) error

	// Close closes the database context and releases any pooled resources held by the instance.
	Close(ctx context.Context) error

	// FetchKinds retrieves the complete list of kinds available to the database.
	FetchKinds(ctx context.Context) (graph.Kinds, error)

	// RefreshKinds refreshes the in memory kinds maps
	RefreshKinds(ctx context.Context) error

	// V2 returns the V2 interface for this V1 database instance
	V2() database.Instance
}
