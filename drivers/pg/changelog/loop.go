package changelog

import (
	"context"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/util/channels"
)

type loop struct {
	ReaderC       <-chan Change
	WriterC       chan<- Change
	FlushInterval time.Duration
	NodeBuffer    []NodeChange
	EdgeBuffer    []EdgeChange
	BatchSize     int
	flusher       flusher // interface for testing seam
}

type flusher interface {
	flushNodeChanges(ctx context.Context, changes []NodeChange) (int64, error)
	flushEdgeChanges(ctx context.Context, changes []EdgeChange) (int64, error)
}

func newLoop(ctx context.Context, flusher flusher, batchSize int) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		ReaderC:       readerC,
		WriterC:       writerC,
		FlushInterval: 5 * time.Second,
		NodeBuffer:    make([]NodeChange, 0),
		EdgeBuffer:    make([]EdgeChange, 0),
		BatchSize:     batchSize,
		flusher:       flusher,
	}
}

func (s *loop) start(ctx context.Context) error {
	idle := time.NewTimer(s.FlushInterval) // fires once, when we've been idle for flushInterval
	idle.Stop()                            // if nothing is buffered, keep the timer stopped

	defer func() {
		close(s.WriterC)
		idle.Stop()
		slog.InfoContext(ctx, "shutting down changelog")
	}()

	slog.InfoContext(ctx, "starting changelog")

	for {
		select {
		case <-ctx.Done():
			// flush any leftovers
			if err := s.tryFlush(ctx, 0); err != nil {
				slog.WarnContext(ctx, "final flush failed", "err", err)
			}
			return nil

		case change, ok := <-s.ReaderC:
			if !ok {
				// input closed; try flushing
				if err := s.tryFlush(ctx, 0); err != nil {
					slog.WarnContext(ctx, "final flush failed", "err", err)
				}
				return nil
			}

			switch typed := change.(type) {
			case NodeChange:
				s.NodeBuffer = append(s.NodeBuffer, typed)

				// sized-base flush
				if err := s.tryFlush(ctx, s.BatchSize); err != nil {
					slog.WarnContext(ctx, "flush failed", "err", err)
				}

				// everytime we append to the buffer, we want the flush timer to start
				// ticking FROM NOW
				idle.Reset(s.FlushInterval)

			case EdgeChange:
				s.EdgeBuffer = append(s.EdgeBuffer, typed)

				// sized-base flush
				if err := s.tryFlush(ctx, s.BatchSize); err != nil {
					slog.WarnContext(ctx, "flush failed", "err", err)
				}

				// everytime we append to the buffer, we want the flush timer to start
				// ticking FROM NOW
				idle.Reset(s.FlushInterval)
			}

		case <-idle.C:
			// idle-based flush
			if err := s.tryFlush(ctx, 0); err != nil {
				slog.WarnContext(ctx, "idle flush failed", "err", err)
			}
		}

	}

}

func (s *loop) tryFlush(ctx context.Context, batchWriteSize int) error {
	if len(s.NodeBuffer) >= batchWriteSize {
		if _, err := s.flusher.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
			return err
		}
		// clear buffer regardless of errors
		s.NodeBuffer = s.NodeBuffer[:0]
	}

	if len(s.EdgeBuffer) >= batchWriteSize {
		if _, err := s.flusher.flushEdgeChanges(ctx, s.EdgeBuffer); err != nil {
			return err
		}
		// clear buffer regardless of errors
		s.EdgeBuffer = s.EdgeBuffer[:0]
	}

	return nil
}
