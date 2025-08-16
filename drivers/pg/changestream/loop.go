package changestream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/util/channels"
)

type loop struct {
	State         *stateManager // todo: remove? unless needed for paritioning
	ReaderC       <-chan Change
	WriterC       chan<- Change
	notificationC chan<- Notification
	FlushInterval time.Duration
	NodeBuffer    []*NodeChange
	BatchSize     int
	flusher       flusher // interface for testing seam
}

type flusher interface {
	flushNodeChanges(ctx context.Context, changes []*NodeChange) (int64, error)
}

func newLoop(ctx context.Context, flusher flusher, notificationC chan<- Notification, batchSize int) loop {
	writerC, readerC := channels.BufferedPipe[Change](ctx)

	return loop{
		ReaderC:       readerC,
		WriterC:       writerC,
		FlushInterval: 5 * time.Second,
		NodeBuffer:    make([]*NodeChange, 0),
		BatchSize:     batchSize,
		notificationC: notificationC,
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
			if err := s.tryFlush(ctx); err != nil {
				slog.WarnContext(ctx, "final flush failed", "err", err)
			}
			return nil

		case change, ok := <-s.ReaderC:
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
	if lastNodeID, err := s.flusher.flushNodeChanges(ctx, s.NodeBuffer); err != nil {
		return err
	} else {
		// clear buffer regardless of errors
		// todo: is this chill?
		s.NodeBuffer = s.NodeBuffer[:0]

		// notify ingest consumer that there are changes ready
		if !channels.Submit(ctx, s.notificationC, Notification{
			Type:       NotificationNode,
			RevisionID: lastNodeID,
		}) {
			slog.Warn(fmt.Sprintf("submitting latest node notification: %v", err))
		}
	}

	return nil
}
