package changelog

import (
	"context"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/util/channels"
)

// loop coordinates the ingestion of deduplicated graph changes.
// It buffers NodeChange and EdgeChange values in memory, and flushes them
// to the backing flusher either when:
//   - the buffer reaches the configured batch size, or
//   - no new changes arrive within the flush interval.
type loop struct {
	readerC       <-chan Change
	writerC       chan<- Change
	flushInterval time.Duration
	batchSize     int

	nodeBuffer *changeBuffer[NodeChange]
	edgeBuffer *changeBuffer[EdgeChange]
}

type flusher interface {
	flushNodeChanges(ctx context.Context, changes []NodeChange) (int64, error)
	flushEdgeChanges(ctx context.Context, changes []EdgeChange) (int64, error)
}

func newLoop(ctx context.Context, f flusher, batchSize int, flushInterval time.Duration) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		writerC:       writerC,
		readerC:       readerC,
		flushInterval: flushInterval,
		batchSize:     batchSize,
		nodeBuffer:    newChangeBuffer(f.flushNodeChanges),
		edgeBuffer:    newChangeBuffer(f.flushEdgeChanges),
	}
}

// changeBuffer is a small helper that accumulates changes and flushes them
// via a supplied function when size thresholds are hit.
type changeBuffer[T any] struct {
	buf     []T
	flushFn func(ctx context.Context, changes []T) (int64, error)
}

func newChangeBuffer[T any](flushFn func(ctx context.Context, changes []T) (int64, error)) *changeBuffer[T] {
	return &changeBuffer[T]{buf: make([]T, 0), flushFn: flushFn}
}

func (s *changeBuffer[T]) add(change T) {
	s.buf = append(s.buf, change)
}

// tryFlush flushes the buffer if batchSize == 0 (force flush) or
// if the buffer length meets/exceeds batchSize. It clears the buffer
// regardless of flush success.
func (s *changeBuffer[T]) tryFlush(ctx context.Context, batchSize int) error {
	if len(s.buf) == 0 {
		return nil
	}
	if batchSize == 0 || len(s.buf) >= batchSize {
		if _, err := s.flushFn(ctx, s.buf); err != nil {
			return err
		}
		s.buf = s.buf[:0]
	}
	return nil
}

func (s *loop) start(ctx context.Context) error {
	idle := time.NewTimer(s.flushInterval) // fires once, when we've been idle for flushInterval
	idle.Stop()                            // if nothing is buffered, keep the timer stopped

	defer func() {
		close(s.writerC)
		idle.Stop()
		slog.InfoContext(ctx, "shutting down changelog")
	}()

	slog.InfoContext(ctx, "starting changelog")

	for {
		select {
		case <-ctx.Done():
			// flush any leftovers
			_ = s.nodeBuffer.tryFlush(ctx, 0)
			_ = s.edgeBuffer.tryFlush(ctx, 0)
			return nil

		case change, ok := <-s.readerC:
			if !ok {
				// input closed; try flushing
				_ = s.nodeBuffer.tryFlush(ctx, 0)
				_ = s.edgeBuffer.tryFlush(ctx, 0)
				return nil
			}

			switch typed := change.(type) {
			case NodeChange:
				s.nodeBuffer.add(typed)

				// sized-base flush
				if err := s.nodeBuffer.tryFlush(ctx, s.batchSize); err != nil {
					slog.WarnContext(ctx, "flush nodes failed", "err", err)
				}

			case EdgeChange:
				s.edgeBuffer.add(typed)

				// sized-base flush
				if err := s.edgeBuffer.tryFlush(ctx, s.batchSize); err != nil {
					slog.WarnContext(ctx, "flush edges failed", "err", err)
				}
			}

			// everytime we append to the buffer, we want the flush timer to start
			// ticking FROM NOW
			idle.Reset(s.flushInterval)

		case <-idle.C:
			// idle-based flush
			if err := s.nodeBuffer.tryFlush(ctx, 0); err != nil {
				slog.WarnContext(ctx, "idle flush (nodes) failed", "err", err)
			}
			if err := s.edgeBuffer.tryFlush(ctx, 0); err != nil {
				slog.WarnContext(ctx, "idle flush (edges) failed", "err", err)
			}
		}

	}

}
