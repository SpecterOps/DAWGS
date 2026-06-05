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

func (m *mockGraphDB) SetBatchWriteSize(interval int) {}

func (m *mockGraphDB) SetWriteFlushSize(size int) {}

func (m *mockGraphDB) SetDefaultGraph(ctx context.Context, graph graph.Graph) error {
	if m.errorMsg.Error() == "Failed setting default graph namespace" {
		return fmt.Errorf("Failed setting default graph namespace")
	}
	return nil
}

func (m *mockGraphDB) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {

	m.readTransactionCalled = true
	if m.errorMsg.Error() == "Read Transaction Failed" {
		return fmt.Errorf("Read Transaction Failed")
	}

	return txDelegate(m.tx)
}

func (m *mockGraphDB) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if m.errorMsg.Error() == "Write Transaction Failed" {
		return fmt.Errorf("Write Transaction Failed")
	}
	return nil
}

func (m *mockGraphDB) AssertSchema(ctx context.Context, dbSchema graph.Schema) error {
	if m.errorMsg.Error() == "Schema Assertion Failed" {
		return fmt.Errorf("Schema Assertion Failed")
	}
	return nil
}

func (m *mockGraphDB) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	if m.errorMsg.Error() == "Batch Transaction Failed" {
		return fmt.Errorf("Batch Transaction Failed")
	}
	return nil
}

func (m *mockGraphDB) Close(ctx context.Context) error {
	if m.errorMsg.Error() == "Closing DB Failed" {
		return fmt.Errorf("Closing DB Failed")
	}
	return nil
}

func (m *mockGraphDB) FetchKinds(ctx context.Context) (graph.Kinds, error) {
	if m.errorMsg.Error() == "Failed to Fetch Kinds" {
		return nil, fmt.Errorf("Failed to Fetch Kinds")
	}
	return m.kinds, nil
}

func (m *mockGraphDB) RefreshKinds(ctx context.Context) error {
	if m.errorMsg.Error() == "Failed to refresh kinds" {
		return fmt.Errorf("Failed to refresh kinds")
	}
	return nil
}

func (m *mockGraphDB) Run(ctx context.Context, query string, parameters map[string]any) error {
	if m.errorMsg.Error() == "Failed to run SQL statement" {
		return fmt.Errorf("Failed to run SQL statement")
	}
	return nil
}

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
