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

// hasError matches the failure message configured on the graph database test double.
func (s *mockGraphDB) hasError(msg string) bool {
	return s.errorMsg != nil && s.errorMsg.Error() == msg
}

// SetBatchWriteSize accepts batch sizing without changing the test double's behavior.
func (s *mockGraphDB) SetBatchWriteSize(interval int) {}

// SetWriteFlushSize accepts flush sizing without changing the test double's behavior.
func (s *mockGraphDB) SetWriteFlushSize(size int) {}

// SetDefaultGraph returns the configured namespace-selection failure when requested.
func (s *mockGraphDB) SetDefaultGraph(ctx context.Context, graph graph.Graph) error {
	if s.hasError("Failed setting default graph namespace") {
		return fmt.Errorf("Failed setting default graph namespace")
	}
	return nil
}

// ReadTransaction records the read and executes its delegate unless the configured failure is active.
func (s *mockGraphDB) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {

	s.readTransactionCalled = true
	if s.hasError("Read Transaction Failed") {
		return fmt.Errorf("Read Transaction Failed")
	}

	return txDelegate(s.tx)
}

// WriteTransaction returns the configured write failure without executing a delegate.
func (s *mockGraphDB) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if s.hasError("Write Transaction Failed") {
		return fmt.Errorf("Write Transaction Failed")
	}
	return nil
}

// AssertSchema returns the configured schema-assertion failure when requested.
func (s *mockGraphDB) AssertSchema(ctx context.Context, dbSchema graph.Schema) error {
	if s.hasError("Schema Assertion Failed") {
		return fmt.Errorf("Schema Assertion Failed")
	}
	return nil
}

// BatchOperation returns the configured batch-operation failure when requested.
func (s *mockGraphDB) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	if s.hasError("Batch Transaction Failed") {
		return fmt.Errorf("Batch Transaction Failed")
	}
	return nil
}

// Close returns the configured database-close failure when requested.
func (s *mockGraphDB) Close(ctx context.Context) error {
	if s.hasError("Closing DB Failed") {
		return fmt.Errorf("Closing DB Failed")
	}
	return nil
}

// FetchKinds returns the injected kinds or the configured lookup failure.
func (s *mockGraphDB) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	if s.hasError("Failed to Fetch Kinds") {
		return nil, fmt.Errorf("Failed to Fetch Kinds")
	}
	return s.kinds, nil
}

// RefreshKinds returns the configured kind-refresh failure when requested.
func (s *mockGraphDB) RefreshKinds(ctx context.Context) error {
	if s.hasError("Failed to refresh kinds") {
		return fmt.Errorf("Failed to refresh kinds")
	}
	return nil
}

// Run returns the configured raw-statement failure when requested.
func (s *mockGraphDB) Run(ctx context.Context, query string, parameters map[string]any) error {
	if s.hasError("Failed to run SQL statement") {
		return fmt.Errorf("Failed to run SQL statement")
	}
	return nil
}

// OptimizeStorage accepts maintenance calls without performing storage work in the test double.
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
