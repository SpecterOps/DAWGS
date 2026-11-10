package traversal

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type mockGraphDB struct {
	readTransactionCalled bool
	errorMsg              error
	kinds                 graph.Kinds
	tx                    graph.Transaction
	config                *graph.TransactionConfig
}

func (s *mockGraphDB) hasError(msg string) bool {
	return s.errorMsg != nil && s.errorMsg.Error() == msg
}

func (s *mockGraphDB) SetBatchWriteSize(interval int) {}

func (s *mockGraphDB) SetWriteFlushSize(size int) {}

func (s *mockGraphDB) SetDefaultGraph(ctx context.Context, graph graph.Graph) error {
	if s.hasError("Failed setting default graph namespace") {
		return fmt.Errorf("Failed setting default graph namespace")
	}
	return nil
}

func (s *mockGraphDB) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {

	s.readTransactionCalled = true
	if s.hasError("Read Transaction Failed") {
		return fmt.Errorf("Read Transaction Failed")
	}

	return txDelegate(s.tx)
}

func (s *mockGraphDB) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if s.hasError("Write Transaction Failed") {
		return fmt.Errorf("Write Transaction Failed")
	}
	return nil
}

func (s *mockGraphDB) AssertSchema(ctx context.Context, dbSchema graph.Schema) error {
	if s.hasError("Schema Assertion Failed") {
		return fmt.Errorf("Schema Assertion Failed")
	}
	return nil
}

func (s *mockGraphDB) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	if s.hasError("Batch Transaction Failed") {
		return fmt.Errorf("Batch Transaction Failed")
	}
	return nil
}

func (s *mockGraphDB) Close(ctx context.Context) error {
	if s.hasError("Closing DB Failed") {
		return fmt.Errorf("Closing DB Failed")
	}
	return nil
}

func (s *mockGraphDB) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	if s.hasError("Failed to Fetch Kinds") {
		return nil, fmt.Errorf("Failed to Fetch Kinds")
	}
	return s.kinds, nil
}

func (s *mockGraphDB) RefreshKinds(ctx context.Context) error {
	if s.hasError("Failed to refresh kinds") {
		return fmt.Errorf("Failed to refresh kinds")
	}
	return nil
}

func (s *mockGraphDB) Run(ctx context.Context, query string, parameters map[string]any) error {
	if s.hasError("Failed to run SQL statement") {
		return fmt.Errorf("Failed to run SQL statement")
	}
	return nil
}

func (s *mockGraphDB) OptimizeStorage(ctx context.Context) error { return nil }

func collectionPath(startID, endID, relationshipID graph.ID) graph.Path {
	return graph.Path{
		Nodes: []*graph.Node{
			graph.NewNode(startID, nil, kindA),
			graph.NewNode(endID, nil, kindB),
		},
		Edges: []*graph.Relationship{
			graph.NewRelationship(relationshipID, startID, endID, nil, kindR),
		},
	}
}
