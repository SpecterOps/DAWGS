package ops

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

var errParallelWorkerFailed = errors.New("controlled parallel worker failure")

const parallelTestTimeout = 10 * time.Second

// fakeCursor is a graph.Cursor double that yields its backing channel before
// reporting the configured error.
type fakeCursor[T any] struct {
	err error
	ch  chan T
}

func (s *fakeCursor[T]) Error() error { return s.err }
func (s *fakeCursor[T]) Close()       {}
func (s *fakeCursor[T]) Chan() chan T { return s.ch }

type fakeFetchFunc func(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error

// fakeNodeQuery is a graph.NodeQuery double that only implements the surface
// used by the parallel fetch helpers. Reaching any other method panics through
// the embedded nil interface.
type fakeNodeQuery struct {
	graph.NodeQuery
	fetch fakeFetchFunc
	first func() (*graph.Node, error)
}

func (s *fakeNodeQuery) Filter(graph.Criteria) graph.NodeQuery     { return s }
func (s *fakeNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery { return s }
func (s *fakeNodeQuery) Offset(int) graph.NodeQuery                { return s }
func (s *fakeNodeQuery) Limit(int) graph.NodeQuery                 { return s }
func (s *fakeNodeQuery) Count() (int64, error)                     { return 0, nil }
func (s *fakeNodeQuery) Delete() error                             { return nil }
func (s *fakeNodeQuery) Update(*graph.Properties) error            { return nil }
func (s *fakeNodeQuery) Query(func(graph.Result) error, ...graph.Criteria) error {
	return nil
}
func (s *fakeNodeQuery) FetchIDs(func(cursor graph.Cursor[graph.ID]) error) error {
	return nil
}
func (s *fakeNodeQuery) FetchKinds(func(cursor graph.Cursor[graph.KindsResult]) error) error {
	return nil
}

func (s *fakeNodeQuery) Fetch(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
	return s.fetch(delegate, finalCriteria...)
}

func (s *fakeNodeQuery) First() (*graph.Node, error) {
	if s.first != nil {
		return s.first()
	}

	return nil, graph.ErrNoResultsFound
}

// fakeTransaction is a graph.Transaction double that only serves node queries.
type fakeTransaction struct {
	graph.Transaction
	nodes graph.NodeQuery
}

func (s *fakeTransaction) Nodes() graph.NodeQuery { return s.nodes }

// fakeDatabase is a graph.Database double that only serves read transactions.
type fakeDatabase struct {
	tx graph.Transaction
}

func newFakeDatabase(nodeQuery graph.NodeQuery) *fakeDatabase {
	return &fakeDatabase{tx: &fakeTransaction{nodes: nodeQuery}}
}

func (s *fakeDatabase) SetWriteFlushSize(int)                              {}
func (s *fakeDatabase) SetBatchWriteSize(int)                              {}
func (s *fakeDatabase) SetDefaultGraph(context.Context, graph.Graph) error { return nil }
func (s *fakeDatabase) WriteTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	return nil
}
func (s *fakeDatabase) BatchOperation(context.Context, graph.BatchDelegate, ...graph.BatchOption) error {
	return nil
}
func (s *fakeDatabase) AssertSchema(context.Context, graph.Schema) error  { return nil }
func (s *fakeDatabase) Run(context.Context, string, map[string]any) error { return nil }
func (s *fakeDatabase) Close(context.Context) error                       { return nil }
func (s *fakeDatabase) FetchKinds(context.Context) (graph.Kinds, error)   { return nil, nil }
func (s *fakeDatabase) RefreshKinds(context.Context) error                { return nil }
func (s *fakeDatabase) OptimizeStorage(context.Context) error             { return nil }

func (s *fakeDatabase) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return txDelegate(s.tx)
}

// failingFetch returns a Fetch implementation that fails every range query
// with the given error.
func failingFetch(err error) fakeFetchFunc {
	return func(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
		return err
	}
}

// streamingFetch returns a Fetch implementation that streams the given number
// of uniquely identified nodes for every range query.
func streamingFetch(nodesPerRange int) fakeFetchFunc {
	var nextID atomic.Int64

	return func(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
		nodeC := make(chan *graph.Node)

		go func() {
			defer close(nodeC)

			for i := 0; i < nodesPerRange; i++ {
				nodeC <- graph.NewNode(graph.ID(nextID.Add(1)), nil)
			}
		}()

		return delegate(&fakeCursor[*graph.Node]{ch: nodeC})
	}
}

// blockingFetch returns a Fetch implementation that blocks until the given
// context expires, mirroring a long-running database read.
func blockingFetch(ctx context.Context) fakeFetchFunc {
	return func(delegate func(cursor graph.Cursor[*graph.Node]) error, finalCriteria ...graph.Criteria) error {
		<-ctx.Done()
		return nil
	}
}

func awaitCompletion(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(parallelTestTimeout):
		t.Fatal("parallel operation did not return without external cancellation")
	}
}

func TestParallelFetchNodesStopsOnFirstWorkerFailure(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{fetch: failingFetch(errParallelWorkerFailed)})

	done := make(chan struct{})

	var (
		nodes graph.NodeSet
		err   error
	)

	go func() {
		nodes, err = parallelFetchNodes(context.Background(), db, 50_000, nil, 1)
		close(done)
	}()

	awaitCompletion(t, done)

	if !errors.Is(err, errParallelWorkerFailed) {
		t.Fatalf("expected the originating worker error, got: %v", err)
	}

	if len(nodes) != 0 {
		t.Fatalf("expected no fetched nodes, got: %d", len(nodes))
	}
}

func TestParallelFetchNodesStopsWhenAllWorkersFail(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{fetch: failingFetch(errParallelWorkerFailed)})

	done := make(chan struct{})

	var err error

	go func() {
		_, err = parallelFetchNodes(context.Background(), db, 50_000, nil, 2)
		close(done)
	}()

	awaitCompletion(t, done)

	if !errors.Is(err, errParallelWorkerFailed) {
		t.Fatalf("expected the originating worker error, got: %v", err)
	}
}

func TestParallelFetchNodesCollectsAllRangeNodes(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{fetch: streamingFetch(2)})

	done := make(chan struct{})

	var (
		nodes graph.NodeSet
		err   error
	)

	go func() {
		nodes, err = parallelFetchNodes(context.Background(), db, 50_000, query.Equals(query.NodeID(), 1), 2)
		close(done)
	}()

	awaitCompletion(t, done)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Three 20,000-ID ranges with two streamed nodes each must all be collected.
	if len(nodes) != 6 {
		t.Fatalf("expected 6 collected nodes, got: %d", len(nodes))
	}
}

func TestParallelFetchNodesCallerCancellationUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db := newFakeDatabase(&fakeNodeQuery{fetch: blockingFetch(ctx)})

	done := make(chan struct{})

	go func() {
		_, _ = parallelFetchNodes(ctx, db, 50_000, nil, 1)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	awaitCompletion(t, done)
}

func TestParallelNodeQueryStopsOnFirstWorkerFailure(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{})

	done := make(chan struct{})

	var (
		visited int64
		err     error
	)

	go func() {
		err = parallelNodeQuery(context.Background(), db, 1, nil, 50_000, func(query graph.NodeQuery) error {
			atomic.AddInt64(&visited, 1)
			return errParallelWorkerFailed
		})
		close(done)
	}()

	awaitCompletion(t, done)

	if !errors.Is(err, errParallelWorkerFailed) {
		t.Fatalf("expected the originating worker error, got: %v", err)
	}

	if visited != 1 {
		t.Fatalf("expected exactly one visited range, got: %d", visited)
	}
}

func TestParallelNodeQueryCoversAllRanges(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{})

	done := make(chan struct{})

	var (
		visited int64
		err     error
	)

	go func() {
		err = parallelNodeQuery(context.Background(), db, 2, query.Equals(query.NodeID(), 1), 50_000, func(query graph.NodeQuery) error {
			if query == nil {
				t.Error("expected a non-nil node query delegate argument")
			}

			atomic.AddInt64(&visited, 1)
			return nil
		})
		close(done)
	}()

	awaitCompletion(t, done)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if visited != 3 {
		t.Fatalf("expected 3 visited ranges, got: %d", visited)
	}
}

func TestParallelNodeQueryCallerCancellationUnblocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db := newFakeDatabase(&fakeNodeQuery{})

	done := make(chan struct{})

	go func() {
		_ = parallelNodeQuery(ctx, db, 1, nil, 50_000, func(query graph.NodeQuery) error {
			<-ctx.Done()
			return nil
		})
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	awaitCompletion(t, done)
}

func TestParallelFetchNodesEmptyDatabase(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{fetch: streamingFetch(2)})

	nodes, err := ParallelFetchNodes(context.Background(), db, nil, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(nodes) != 0 {
		t.Fatalf("expected no nodes for an empty database, got: %d", len(nodes))
	}
}

func TestParallelFetchNodesWrapperCollectsNodes(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{
		fetch: streamingFetch(2),
		first: func() (*graph.Node, error) {
			return graph.NewNode(50_000, nil), nil
		},
	})

	nodes, err := ParallelFetchNodes(context.Background(), db, nil, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(nodes) != 6 {
		t.Fatalf("expected 6 collected nodes, got: %d", len(nodes))
	}
}

func TestParallelNodeQueryBuilderStreamStopsOnWorkerFailure(t *testing.T) {
	db := newFakeDatabase(&fakeNodeQuery{
		fetch: failingFetch(errParallelWorkerFailed),
		first: func() (*graph.Node, error) {
			return graph.NewNode(50_000, nil), nil
		},
	})

	builder := NewParallelNodeQuery[int](db).UsingQuery(func(query graph.NodeQuery, outC chan<- int) error {
		return errParallelWorkerFailed
	})

	streamC := builder.Stream(context.Background(), 1)

	consumed := make(chan struct{})

	go func() {
		for range streamC {
		}
		close(consumed)
	}()

	awaitCompletion(t, consumed)
	builder.Join()

	if !errors.Is(builder.Error(), errParallelWorkerFailed) {
		t.Fatalf("expected the originating worker error, got: %v", builder.Error())
	}
}
