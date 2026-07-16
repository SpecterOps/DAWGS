package retriever

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

const testFragmentFormat = "TEST"

type testFragmentMetadata struct {
	Rows int
}

func (s testFragmentMetadata) rowCount() int {
	return s.Rows
}

type fanoutEventLog struct {
	mu     sync.Mutex
	events []string
}

func (s *fanoutEventLog) add(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *fanoutEventLog) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

type fanoutSinkState[T any] struct {
	mu sync.Mutex

	name      string
	events    *fanoutEventLog
	rowOffset int

	openHook    func(context.Context) error
	writeHook   func(context.Context) error
	prepareHook func(context.Context) error
	commitHook  func(context.Context) error
	abortErr    error

	openedOnce   sync.Once
	preparedOnce sync.Once
	opened       chan struct{}
	prepared     chan struct{}

	ids            []shardID
	batches        [][]T
	batchStarts    []*T
	rows           int
	writerAborts   int
	preparedAborts int
	commitAttempts int
	committed      bool
}

func newFanoutSinkState[T any](name string, events *fanoutEventLog) *fanoutSinkState[T] {
	return &fanoutSinkState[T]{
		name:     name,
		events:   events,
		opened:   make(chan struct{}),
		prepared: make(chan struct{}),
	}
}

func (s *fanoutSinkState[T]) addEvent(event string) {
	if s.events != nil {
		s.events.add(s.name + "." + event)
	}
}

type fanoutSinkSnapshot[T any] struct {
	ids            []shardID
	batches        [][]T
	batchStarts    []*T
	writerAborts   int
	preparedAborts int
	commitAttempts int
	committed      bool
}

func (s *fanoutSinkState[T]) snapshot() fanoutSinkSnapshot[T] {
	s.mu.Lock()
	defer s.mu.Unlock()

	batches := make([][]T, len(s.batches))
	for index, batch := range s.batches {
		batches[index] = append([]T(nil), batch...)
	}
	return fanoutSinkSnapshot[T]{
		ids:            append([]shardID(nil), s.ids...),
		batches:        batches,
		batchStarts:    append([]*T(nil), s.batchStarts...),
		writerAborts:   s.writerAborts,
		preparedAborts: s.preparedAborts,
		commitAttempts: s.commitAttempts,
		committed:      s.committed,
	}
}

type fanoutTestSink[T any, M fragmentMetadata] struct {
	state    *fanoutSinkState[T]
	metadata func(int) M
}

func (s fanoutTestSink[T, M]) Open(ctx context.Context, id shardID) (fragmentWriter[T, M], error) {
	s.state.mu.Lock()
	s.state.ids = append(s.state.ids, id)
	s.state.mu.Unlock()
	if s.state.openHook != nil {
		if err := s.state.openHook(ctx); err != nil {
			return nil, err
		}
	}
	s.state.addEvent("open")
	s.state.openedOnce.Do(func() { close(s.state.opened) })
	return &fanoutTestWriter[T, M]{sink: s}, nil
}

type fanoutTestWriter[T any, M fragmentMetadata] struct {
	sink fanoutTestSink[T, M]
}

func (s *fanoutTestWriter[T, M]) WriteBatch(ctx context.Context, records []T) error {
	s.sink.state.mu.Lock()
	s.sink.state.batches = append(s.sink.state.batches, append([]T(nil), records...))
	if len(records) > 0 {
		s.sink.state.batchStarts = append(s.sink.state.batchStarts, &records[0])
	} else {
		s.sink.state.batchStarts = append(s.sink.state.batchStarts, nil)
	}
	s.sink.state.rows += len(records)
	s.sink.state.mu.Unlock()
	s.sink.state.addEvent("write")
	if s.sink.state.writeHook != nil {
		return s.sink.state.writeHook(ctx)
	}
	return nil
}

func (s *fanoutTestWriter[T, M]) Prepare(ctx context.Context) (preparedFragment[M], error) {
	if s.sink.state.prepareHook != nil {
		if err := s.sink.state.prepareHook(ctx); err != nil {
			return nil, err
		}
	}
	s.sink.state.mu.Lock()
	rows := s.sink.state.rows + s.sink.state.rowOffset
	s.sink.state.mu.Unlock()
	prepared := &fanoutTestPrepared[T, M]{state: s.sink.state, metadata: s.sink.metadata(rows)}
	s.sink.state.addEvent("prepare")
	s.sink.state.preparedOnce.Do(func() { close(s.sink.state.prepared) })
	return prepared, nil
}

func (s *fanoutTestWriter[T, M]) Abort() error {
	s.sink.state.mu.Lock()
	s.sink.state.writerAborts++
	s.sink.state.mu.Unlock()
	s.sink.state.addEvent("writer-abort")
	return s.sink.state.abortErr
}

type fanoutTestPrepared[T any, M fragmentMetadata] struct {
	state    *fanoutSinkState[T]
	metadata M
}

func (s *fanoutTestPrepared[T, M]) Metadata() M {
	return s.metadata
}

func (s *fanoutTestPrepared[T, M]) Commit(ctx context.Context) error {
	s.state.mu.Lock()
	s.state.commitAttempts++
	s.state.mu.Unlock()
	s.state.addEvent("commit-attempt")
	if s.state.commitHook != nil {
		if err := s.state.commitHook(ctx); err != nil {
			return err
		}
	}
	s.state.mu.Lock()
	s.state.committed = true
	s.state.mu.Unlock()
	s.state.addEvent("commit")
	return nil
}

func (s *fanoutTestPrepared[T, M]) Abort() error {
	s.state.mu.Lock()
	s.state.preparedAborts++
	s.state.mu.Unlock()
	s.state.addEvent("prepared-abort")
	return s.state.abortErr
}

func newFanoutJSONLSink[T any](state *fanoutSinkState[T]) fragmentSink[T, jsonlFragmentMetadata] {
	return fanoutTestSink[T, jsonlFragmentMetadata]{
		state: state,
		metadata: func(rows int) jsonlFragmentMetadata {
			return jsonlFragmentMetadata{Path: "fragment.jsonl.gz", Rows: rows}
		},
	}
}

func newFanoutSecondarySink[T any](state *fanoutSinkState[T], collected *testFragmentMetadata) shardSink[T] {
	leaf := fanoutTestSink[T, testFragmentMetadata]{
		state: state,
		metadata: func(rows int) testFragmentMetadata {
			return testFragmentMetadata{Rows: rows}
		},
	}
	return newShardSink(testFragmentFormat, leaf, func(_ *committedShard, metadata testFragmentMetadata) {
		*collected = metadata
	})
}

func newTestSinkSet[T any](jsonl, secondary *fanoutSinkState[T], collected *testFragmentMetadata) shardSinkSet[T] {
	return newShardSinkSet(
		newJSONLShardSink(newFanoutJSONLSink(jsonl)),
		newFanoutSecondarySink(secondary, collected),
	)
}

func TestShardSinkSetFansOutSameSlicesAndSummary(t *testing.T) {
	events := &fanoutEventLog{}
	jsonlState := newFanoutSinkState[int]("jsonl", events)
	secondaryState := newFanoutSinkState[int]("secondary", events)
	var collected testFragmentMetadata
	output := newTestSinkSet(jsonlState, secondaryState, &collected)
	id := shardID{Graph: "example", Phase: PhaseNodes, Number: 4}
	summary := shardSummary{ID: id, Rows: 4, ActionCounts: map[string]int{"drop": 2}}
	var accepted shardSummary
	receiver := newShardOutputReceiver(context.Background(), output, func(got shardSummary, committed committedShard) error {
		accepted = got
		if committed.JSONL.Rows != summary.Rows {
			t.Fatalf("committed JSONL rows = %d", committed.JSONL.Rows)
		}
		return nil
	})
	first := []int{1, 2}
	second := []int{3, 4}
	if err := receiver.BeginShard(id); err != nil {
		t.Fatalf("begin shard: %v", err)
	}
	if err := receiver.WriteBatch(first); err != nil {
		t.Fatalf("write first batch: %v", err)
	}
	if err := receiver.WriteBatch(second); err != nil {
		t.Fatalf("write second batch: %v", err)
	}
	if err := receiver.FinishShard(summary); err != nil {
		t.Fatalf("finish shard: %v", err)
	}

	jsonl := jsonlState.snapshot()
	secondary := secondaryState.snapshot()
	if !reflect.DeepEqual(jsonl.ids, []shardID{id}) || !reflect.DeepEqual(secondary.ids, jsonl.ids) {
		t.Fatalf("JSONL ids = %+v, secondary ids = %+v", jsonl.ids, secondary.ids)
	}
	if !reflect.DeepEqual(jsonl.batches, [][]int{{1, 2}, {3, 4}}) || !reflect.DeepEqual(secondary.batches, jsonl.batches) {
		t.Fatalf("JSONL batches = %+v, secondary batches = %+v", jsonl.batches, secondary.batches)
	}
	if !reflect.DeepEqual(jsonl.batchStarts, []*int{&first[0], &second[0]}) || !reflect.DeepEqual(secondary.batchStarts, jsonl.batchStarts) {
		t.Fatalf("sink set did not offer the same slices to both sinks")
	}
	if !reflect.DeepEqual(accepted, summary) || collected.Rows != summary.Rows || !jsonl.committed || !secondary.committed {
		t.Fatalf("summary/result mismatch: accepted=%+v collected=%+v JSONL=%+v secondary=%+v", accepted, collected, jsonl, secondary)
	}

	lifecycle := events.snapshot()
	jsonlPrepare := eventPosition(lifecycle, "jsonl.prepare")
	secondaryPrepare := eventPosition(lifecycle, "secondary.prepare")
	jsonlCommit := eventPosition(lifecycle, "jsonl.commit-attempt")
	secondaryCommit := eventPosition(lifecycle, "secondary.commit-attempt")
	if jsonlPrepare < 0 || secondaryPrepare < 0 || jsonlCommit < jsonlPrepare || jsonlCommit < secondaryPrepare || secondaryCommit < jsonlCommit {
		t.Fatalf("prepare and deterministic commit order = %v", lifecycle)
	}
}

func TestShardSinkSetBackpressuresCurrentBatch(t *testing.T) {
	jsonlState := newFanoutSinkState[int]("jsonl", nil)
	secondaryState := newFanoutSinkState[int]("secondary", nil)
	jsonlStarted := make(chan struct{})
	secondaryStarted := make(chan struct{})
	release := make(chan struct{})
	jsonlState.writeHook = func(context.Context) error {
		close(jsonlStarted)
		return nil
	}
	secondaryState.writeHook = func(context.Context) error {
		close(secondaryStarted)
		<-release
		return nil
	}
	var collected testFragmentMetadata
	writer, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), shardID{Graph: "example", Phase: PhaseNodes, Number: 1})
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	batch := []int{1, 2, 3}
	returned := make(chan error, 1)
	go func() { returned <- writer.WriteBatch(context.Background(), batch) }()
	waitForFanoutSignal(t, jsonlStarted, "JSONL write")
	waitForFanoutSignal(t, secondaryStarted, "secondary write")
	select {
	case err := <-returned:
		t.Fatalf("write returned before the slow sink accepted the batch: %v", err)
	default:
	}
	close(release)
	if err := <-returned; err != nil {
		t.Fatalf("write batch: %v", err)
	}
	jsonl := jsonlState.snapshot()
	secondary := secondaryState.snapshot()
	if len(jsonl.batches) != 1 || len(secondary.batches) != 1 || jsonl.batchStarts[0] != &batch[0] || secondary.batchStarts[0] != &batch[0] {
		t.Fatalf("active batch was copied or queued: JSONL=%+v secondary=%+v", jsonl, secondary)
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
}

func TestShardSinkSetOpenFailureAbortsOpenedSibling(t *testing.T) {
	for _, failingFormat := range []string{jsonlFragmentFormat, testFragmentFormat} {
		t.Run(failingFormat, func(t *testing.T) {
			cause := errors.New("open failed")
			jsonlState := newFanoutSinkState[int]("jsonl", nil)
			secondaryState := newFanoutSinkState[int]("secondary", nil)
			if failingFormat == jsonlFragmentFormat {
				jsonlState.openHook = func(context.Context) error {
					<-secondaryState.opened
					return cause
				}
			} else {
				secondaryState.openHook = func(context.Context) error {
					<-jsonlState.opened
					return cause
				}
			}
			var collected testFragmentMetadata
			id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
			_, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), id)
			assertShardSinkOperation(t, err, failingFormat, id, "open", cause)
			if failingFormat == jsonlFragmentFormat && secondaryState.snapshot().writerAborts != 1 {
				t.Fatalf("secondary sibling was not aborted: %+v", secondaryState.snapshot())
			}
			if failingFormat == testFragmentFormat && jsonlState.snapshot().writerAborts != 1 {
				t.Fatalf("JSONL sibling was not aborted: %+v", jsonlState.snapshot())
			}
		})
	}
}

func TestShardSinkSetWriteFailureCancelsAndAbortsSibling(t *testing.T) {
	for _, failingFormat := range []string{jsonlFragmentFormat, testFragmentFormat} {
		t.Run(failingFormat, func(t *testing.T) {
			cause := errors.New("write failed")
			cleanupCause := errors.New("sibling abort failed")
			jsonlState := newFanoutSinkState[int]("jsonl", nil)
			secondaryState := newFanoutSinkState[int]("secondary", nil)
			siblingStarted := make(chan struct{})
			siblingCanceled := make(chan struct{})
			failure := func(context.Context) error {
				<-siblingStarted
				return cause
			}
			blocking := func(ctx context.Context) error {
				close(siblingStarted)
				<-ctx.Done()
				close(siblingCanceled)
				return ctx.Err()
			}
			if failingFormat == jsonlFragmentFormat {
				jsonlState.writeHook = failure
				secondaryState.writeHook = blocking
				secondaryState.abortErr = cleanupCause
			} else {
				jsonlState.writeHook = blocking
				jsonlState.abortErr = cleanupCause
				secondaryState.writeHook = failure
			}
			var collected testFragmentMetadata
			id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
			writer, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), id)
			if err != nil {
				t.Fatalf("open shard: %v", err)
			}
			err = writer.WriteBatch(context.Background(), []int{1})
			assertShardSinkOperation(t, err, failingFormat, id, "write", cause)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("write error does not retain sibling cancellation: %v", err)
			}
			if !errors.Is(err, cleanupCause) {
				t.Fatalf("write error does not retain sibling cleanup failure: %v", err)
			}
			waitForFanoutSignal(t, siblingCanceled, "sibling cancellation")
			if jsonlState.snapshot().writerAborts != 1 || secondaryState.snapshot().writerAborts != 1 {
				t.Fatalf("write cleanup: JSONL=%+v secondary=%+v", jsonlState.snapshot(), secondaryState.snapshot())
			}
		})
	}
}

func TestShardSinkSetPrepareFailureAbortsPreparedSibling(t *testing.T) {
	for _, failingFormat := range []string{jsonlFragmentFormat, testFragmentFormat} {
		t.Run(failingFormat, func(t *testing.T) {
			cause := errors.New("prepare failed")
			jsonlState := newFanoutSinkState[int]("jsonl", nil)
			secondaryState := newFanoutSinkState[int]("secondary", nil)
			if failingFormat == jsonlFragmentFormat {
				jsonlState.prepareHook = func(context.Context) error {
					<-secondaryState.prepared
					return cause
				}
			} else {
				secondaryState.prepareHook = func(context.Context) error {
					<-jsonlState.prepared
					return cause
				}
			}
			var collected testFragmentMetadata
			id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
			writer, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), id)
			if err != nil {
				t.Fatalf("open shard: %v", err)
			}
			if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
				t.Fatalf("write batch: %v", err)
			}
			_, err = writer.Finish(context.Background(), shardSummary{ID: id, Rows: 1})
			assertShardSinkOperation(t, err, failingFormat, id, "prepare", cause)
			jsonl := jsonlState.snapshot()
			secondary := secondaryState.snapshot()
			if jsonl.commitAttempts != 0 || secondary.commitAttempts != 0 {
				t.Fatalf("commit attempted after prepare failure: JSONL=%+v secondary=%+v", jsonl, secondary)
			}
			if failingFormat == jsonlFragmentFormat && (jsonl.writerAborts != 1 || secondary.preparedAborts != 1) {
				t.Fatalf("JSONL prepare cleanup: JSONL=%+v secondary=%+v", jsonl, secondary)
			}
			if failingFormat == testFragmentFormat && (secondary.writerAborts != 1 || jsonl.preparedAborts != 1) {
				t.Fatalf("secondary prepare cleanup: JSONL=%+v secondary=%+v", jsonl, secondary)
			}
		})
	}
}

func TestShardSinkSetValidatesAllCountsBeforeCommit(t *testing.T) {
	jsonlState := newFanoutSinkState[int]("jsonl", nil)
	secondaryState := newFanoutSinkState[int]("secondary", nil)
	secondaryState.rowOffset = 1
	var collected testFragmentMetadata
	id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
	writer, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), id)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	_, err = writer.Finish(context.Background(), shardSummary{ID: id, Rows: 1})
	assertShardSinkOperation(t, err, testFragmentFormat, id, "validate", nil)
	jsonl := jsonlState.snapshot()
	secondary := secondaryState.snapshot()
	if jsonl.commitAttempts != 0 || secondary.commitAttempts != 0 || jsonl.preparedAborts != 1 || secondary.preparedAborts != 1 {
		t.Fatalf("validation lifecycle: JSONL=%+v secondary=%+v", jsonl, secondary)
	}
}

func TestShardSinkSetCommitFailureAbortsOnlyUncommittedSinks(t *testing.T) {
	for _, failingFormat := range []string{jsonlFragmentFormat, testFragmentFormat} {
		t.Run(failingFormat, func(t *testing.T) {
			cause := errors.New("commit failed")
			jsonlState := newFanoutSinkState[int]("jsonl", nil)
			secondaryState := newFanoutSinkState[int]("secondary", nil)
			if failingFormat == jsonlFragmentFormat {
				jsonlState.commitHook = func(context.Context) error { return cause }
			} else {
				secondaryState.commitHook = func(context.Context) error { return cause }
			}
			var collected testFragmentMetadata
			id := shardID{Graph: "example", Phase: PhaseNodes, Number: 1}
			writer, err := newTestSinkSet(jsonlState, secondaryState, &collected).OpenShard(context.Background(), id)
			if err != nil {
				t.Fatalf("open shard: %v", err)
			}
			if err := writer.WriteBatch(context.Background(), []int{1}); err != nil {
				t.Fatalf("write batch: %v", err)
			}
			_, err = writer.Finish(context.Background(), shardSummary{ID: id, Rows: 1})
			assertShardSinkOperation(t, err, failingFormat, id, "commit", cause)
			jsonl := jsonlState.snapshot()
			secondary := secondaryState.snapshot()
			if failingFormat == jsonlFragmentFormat {
				if jsonl.committed || secondary.commitAttempts != 0 || jsonl.preparedAborts != 1 || secondary.preparedAborts != 1 {
					t.Fatalf("JSONL commit failure: JSONL=%+v secondary=%+v", jsonl, secondary)
				}
			} else if !jsonl.committed || secondary.committed || jsonl.preparedAborts != 0 || secondary.preparedAborts != 1 {
				t.Fatalf("secondary commit failure: JSONL=%+v secondary=%+v", jsonl, secondary)
			}
		})
	}
}

func TestShardSinkSetSingleSinkUsesDirectPath(t *testing.T) {
	state := newFanoutSinkState[int]("jsonl", nil)
	started := make(chan struct{})
	release := make(chan struct{})
	type contextKey struct{}
	callerCtx := context.WithValue(context.Background(), contextKey{}, "caller")
	state.writeHook = func(ctx context.Context) error {
		if ctx != callerCtx {
			return errors.New("single-sink path replaced the caller context")
		}
		close(started)
		<-release
		return nil
	}
	output := newShardSinkSet(newJSONLShardSink(newFanoutJSONLSink(state)))
	writer, err := output.OpenShard(callerCtx, shardID{Graph: "example", Phase: PhaseNodes, Number: 1})
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	returned := make(chan error, 1)
	go func() { returned <- writer.WriteBatch(callerCtx, []int{1}) }()
	waitForFanoutSignal(t, started, "direct write")
	select {
	case err := <-returned:
		t.Fatalf("single sink did not backpressure its caller: %v", err)
	default:
	}
	close(release)
	if err := <-returned; err != nil {
		t.Fatalf("write batch: %v", err)
	}
	if state.snapshot().writerAborts != 0 {
		t.Fatalf("single-sink write unexpectedly aborted: %+v", state.snapshot())
	}
	if err := writer.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
}

func eventPosition(events []string, target string) int {
	for index, event := range events {
		if event == target {
			return index
		}
	}
	return -1
}

func waitForFanoutSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}
