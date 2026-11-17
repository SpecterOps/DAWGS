package pg

import (
	"context"
	"errors"
	"sync"

	"github.com/specterops/dawgs/graph"
)

// compactPathCursor gives its worker sole ownership of result iteration and
// finalization. Close waits for that worker before a query-owned snapshot or
// its leased connection can be reused by the calling builder delegate.
type compactPathCursor struct {
	// cancel stops channel delivery without changing the caller's database context.
	cancel context.CancelFunc

	// paths delivers decoded paths until the worker finishes or delivery is canceled.
	paths chan graph.Path

	// done closes after the worker releases its result and retained snapshot.
	done chan struct{}

	// errorLock protects the final error while clients inspect cursor state.
	errorLock sync.Mutex

	// err stores the worker's combined iteration and result-cleanup failure.
	err error
}

// newCompactPathCursor starts one worker that exclusively owns result reads, decoding, and cleanup.
func newCompactPathCursor(ctx context.Context, result graph.Result) graph.Cursor[graph.Path] {
	var (
		cursorCtx, cancel = context.WithCancel(ctx)
		cursor            = &compactPathCursor{
			cancel: cancel,
			paths:  make(chan graph.Path),
			done:   make(chan struct{}),
		}
	)

	go func() {
		var iterationError error
		defer func() {
			result.Close()

			resultError := result.Error()
			if !errors.Is(resultError, iterationError) {
				resultError = errors.Join(resultError, iterationError)
			}

			cursor.errorLock.Lock()
			cursor.err = resultError
			cursor.errorLock.Unlock()

			close(cursor.paths)
			close(cursor.done)
		}()

		for {
			if err := cursorCtx.Err(); err != nil {
				iterationError = err
				return
			}

			if !result.Next() {
				return
			}

			var path graph.Path
			if err := result.Scan(&path); err != nil {
				iterationError = err
				return
			}

			select {
			case cursor.paths <- path:

			case <-cursorCtx.Done():
				iterationError = cursorCtx.Err()
				return
			}
		}
	}()

	return cursor
}

// Chan returns the path stream whose closure also publishes the final cursor error.
func (s *compactPathCursor) Chan() chan graph.Path {
	return s.paths
}

// Error returns the worker's terminal failure after safe synchronized publication.
func (s *compactPathCursor) Error() error {
	s.errorLock.Lock()
	defer s.errorLock.Unlock()

	return s.err
}

// Close stops delivery and waits for in-flight reads to finish under the caller's database context.
func (s *compactPathCursor) Close() {
	// The SQL operation retains its caller context. If Next is already waiting
	// for PostgreSQL, that context's cancellation/deadline interrupts the wait;
	// closing the cursor alone waits for the in-flight Next before cleanup.
	s.cancel()
	<-s.done
}
