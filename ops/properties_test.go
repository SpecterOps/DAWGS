package ops

import (
	"errors"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
)

var errResultIterationFailed = errors.New("controlled result iteration failure")

// fakeResult is a graph.Result double that yields the configured rows and then
// reports the configured error through Error once Next returns false, mirroring
// driver results that surface iteration failures only after iteration stops.
type fakeResult struct {
	rows      [][]any
	err       error
	next      int
	exhausted bool
}

func (s *fakeResult) Keys() []string            { return nil }
func (s *fakeResult) Mapper() graph.ValueMapper { return graph.NewValueMapper() }
func (s *fakeResult) Scan(targets ...any) error { return graph.ScanNextResult(s, targets...) }
func (s *fakeResult) Close()                    {}

func (s *fakeResult) Next() bool {
	if s.next < len(s.rows) {
		s.next++
		return true
	}

	s.exhausted = true
	return false
}

func (s *fakeResult) Values() []any {
	if s.next == 0 || s.next > len(s.rows) {
		return nil
	}

	return s.rows[s.next-1]
}

func (s *fakeResult) Error() error {
	if s.exhausted {
		return s.err
	}

	return nil
}

// resultServingTransaction returns a graph.Transaction double whose node query
// hands the given result to its Query delegate.
func resultServingTransaction(result graph.Result) graph.Transaction {
	return &fakeTransaction{nodes: &fakeNodeQuery{
		query: func(delegate func(graph.Result) error, finalCriteria ...graph.Criteria) error {
			return delegate(result)
		},
	}}
}

func TestFetchNodePropertiesReturnsIterationError(t *testing.T) {
	nodes := graph.NewNodeSet(graph.NewNode(1, nil), graph.NewNode(2, nil))
	tx := resultServingTransaction(&fakeResult{
		rows: [][]any{{int64(1), "first"}},
		err:  errResultIterationFailed,
	})

	if err := FetchNodeProperties(tx, nodes, []string{"name"}); !errors.Is(err, errResultIterationFailed) {
		t.Fatalf("expected the iteration failure to be returned, got: %v", err)
	}

	// The row delivered before the failure is still applied while the second
	// node keeps its unset properties; the caller sees the error either way.
	if got := nodes[1].Properties.MapOrEmpty()["name"]; got != "first" {
		t.Fatalf("expected the first delivered row to be applied, got: %v", got)
	}

	if nodes[2].Properties != nil {
		t.Fatalf("expected the second node's properties to remain unset, got: %v", nodes[2].Properties)
	}
}

func TestFetchNodePropertiesAppliesAllRows(t *testing.T) {
	nodes := graph.NewNodeSet(graph.NewNode(1, nil), graph.NewNode(2, nil))

	var capturedCriteria []graph.Criteria
	tx := &fakeTransaction{nodes: &fakeNodeQuery{
		query: func(delegate func(graph.Result) error, finalCriteria ...graph.Criteria) error {
			capturedCriteria = finalCriteria
			return delegate(&fakeResult{
				rows: [][]any{
					{int64(1), "first", int64(10)},
					{int64(2), "second", int64(20)},
				},
			})
		},
	}}

	if err := FetchNodeProperties(tx, nodes, []string{"name", "count"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(capturedCriteria) != 1 {
		t.Fatalf("expected exactly one returning criteria, got: %d", len(capturedCriteria))
	}

	expected := map[graph.ID]map[string]any{
		1: {"name": "first", "count": int64(10)},
		2: {"name": "second", "count": int64(20)},
	}

	for nodeID, expectedProperties := range expected {
		properties := nodes[nodeID].Properties.MapOrEmpty()

		for name, expectedValue := range expectedProperties {
			if got := properties[name]; got != expectedValue {
				t.Fatalf("expected node %d property %q to be %v, got: %v", nodeID, name, expectedValue, got)
			}
		}
	}
}

func TestFetchNodePropertiesRejectsUnmappableNodeID(t *testing.T) {
	nodes := graph.NewNodeSet(graph.NewNode(1, nil))
	tx := resultServingTransaction(&fakeResult{
		rows: [][]any{{struct{}{}, "first"}},
	})

	err := FetchNodeProperties(tx, nodes, []string{"name"})
	if err == nil {
		t.Fatal("expected an error for an unmappable node ID value")
	}

	if !strings.Contains(err.Error(), "expected node ID as first value") {
		t.Fatalf("expected the node ID mapping error, got: %v", err)
	}

	if nodes[1].Properties != nil {
		t.Fatalf("expected no properties to be applied, got: %v", nodes[1].Properties)
	}
}
