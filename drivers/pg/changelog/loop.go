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
	NodeBuffer    []*NodeChange
	BatchSize     int
	flusher       flusher // interface for testing seam
}

type flusher interface {
	flushNodeChanges(ctx context.Context, changes []*NodeChange) (int64, error)
}

func newLoop(ctx context.Context, flusher flusher, batchSize int) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		ReaderC:       readerC,
		WriterC:       writerC,
		FlushInterval: 5 * time.Second,
		NodeBuffer:    make([]*NodeChange, 0),
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
	start := time.Now()

	channelTicks := 0

	for {
		select {
		case <-ctx.Done():
			slog.Info("channel ticks", "number", channelTicks)
			// flush any leftovers
			if err := s.tryFlush(ctx); err != nil {
				slog.WarnContext(ctx, "final flush failed", "err", err)
			}
			return nil

		case change, ok := <-s.ReaderC:
			channelTicks++
			if !ok {
				// input closed; try flushing
				if err := s.tryFlush(ctx); err != nil {
					slog.WarnContext(ctx, "final flush failed", "err", err)
				}
				return nil
			}

			switch typed := change.(type) {
			case *NodeChange:
				s.NodeBuffer = append(s.NodeBuffer, typed)

				// sized-base flush
				if len(s.NodeBuffer) >= s.BatchSize {
					if err := s.tryFlush(ctx); err != nil {
						slog.WarnContext(ctx, "flush failed", "err", err)
					}

					// prevents duplicate flushes
					// if we've just done a size-based flush,
					idle.Stop()
					continue
				}

				// everytime we append to the buffer, we want the flush timer to start
				// ticking FROM NOW
				idle.Reset(s.FlushInterval)

			case *EdgeChange:
				slog.Info("not implemented yet")
			}

		case <-idle.C:
			slog.Info("idle timer tick", "duration", time.Since(start))
			// idle-based flush
			if len(s.NodeBuffer) > 0 {
				if err := s.tryFlush(ctx); err != nil {
					slog.WarnContext(ctx, "idle flush failed", "err", err)
				}
			}
		}

	}

}

func (s *loop) tryFlush(ctx context.Context) error {
	if _, err := s.flusher.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
		return err
	} else {
		// clear buffer regardless of errors
		s.NodeBuffer = s.NodeBuffer[:0]
	}
	return nil
}
