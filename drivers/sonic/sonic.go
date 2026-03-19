package sonic

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

const DriverName = "sonic"

func init() {
	dawgs.Register(DriverName, func(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
		return NewDatabase(), nil
	})
}

// Database is an in-memory graph database.
type Database struct {
	mu     sync.RWMutex
	nextID atomic.Uint64
	nodes  map[graph.ID]*graph.Node
	edges  map[graph.ID]*graph.Relationship

	// Adjacency indexes
	outEdges map[graph.ID][]graph.ID // nodeID -> edgeIDs where node is start
	inEdges  map[graph.ID][]graph.ID // nodeID -> edgeIDs where node is end

	schema       graph.Schema
	defaultGraph graph.Graph
	kinds        graph.Kinds

	queryMemoryLimit size.Size
	batchWriteSize   int
	writeFlushSize   int
}

func NewDatabase() *Database {
	return &Database{
		nodes:    make(map[graph.ID]*graph.Node),
		edges:    make(map[graph.ID]*graph.Relationship),
		outEdges: make(map[graph.ID][]graph.ID),
		inEdges:  make(map[graph.ID][]graph.ID),
	}
}

func (db *Database) newID() graph.ID {
	return graph.ID(db.nextID.Add(1))
}

func (db *Database) SetWriteFlushSize(interval int) {
	db.writeFlushSize = interval
}

func (db *Database) SetBatchWriteSize(interval int) {
	db.batchWriteSize = interval
}

func (db *Database) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return txDelegate(&transaction{db: db, ctx: ctx})
}

func (db *Database) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return txDelegate(&transaction{db: db, ctx: ctx})
}

func (db *Database) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate) error {
	return batchDelegate(&batch{db: db, ctx: ctx})
}

func (db *Database) AssertSchema(ctx context.Context, dbSchema graph.Schema) error {
	db.schema = dbSchema
	db.defaultGraph = dbSchema.DefaultGraph
	return nil
}

func (db *Database) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	db.defaultGraph = graphSchema
	return nil
}

func (db *Database) Run(ctx context.Context, query string, parameters map[string]any) error {
	// No-op for in-memory driver — raw queries are not supported.
	return nil
}

func (db *Database) Close(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.nodes = make(map[graph.ID]*graph.Node)
	db.edges = make(map[graph.ID]*graph.Relationship)
	db.outEdges = make(map[graph.ID][]graph.ID)
	db.inEdges = make(map[graph.ID][]graph.ID)
	return nil
}

func (db *Database) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	return db.kinds, nil
}

func (db *Database) RefreshKinds(ctx context.Context) error {
	return nil
}
