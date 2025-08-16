package changestream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/util/channels"
	"github.com/stretchr/testify/require"
)

func TestLoop(t *testing.T) {
	t.Run("flushes on batch size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, make(chan<- Notification), 2)

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "1"})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "2"})))

		// Run one iteration
		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		require.Len(t, db.flushed, 2)
	})
	t.Run("no flush happens before batch size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, make(chan<- Notification), 3)

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "1"})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "2"})))

		// Run one iteration
		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		require.Len(t, db.flushed, 0) // nothing was flushed because buffer never reached batch_size
	})
	t.Run("timer triggers flush after inactivity", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, make(chan<- Notification), 3)
		loop.FlushInterval = 20 * time.Millisecond // best effort

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "1"})))

		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond) // wait longer than flush interval

		require.Len(t, db.flushed, 1)
	})
	t.Run("notifications are sent after flush", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		notifyC := make(chan Notification, 1)
		loop := newLoop(ctx, db, notifyC, 1)

		require.True(t, channels.Submit(ctx, loop.WriterC, Change(&NodeChange{NodeID: "1"})))

		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		select {
		case notification := <-notifyC:
			require.Equal(t, NotificationNode, notification.Type)
			require.Equal(t, int64(1), notification.RevisionID)
		default:
			t.Fatal("expected notification")
		}
	})
}

type mockFlusher struct {
	mu       sync.Mutex
	flushed  []*NodeChange
	latestID int64
}

func (m *mockFlusher) flushNodeChanges(_ context.Context, changes []*NodeChange) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushed = append(m.flushed, changes...)
	m.latestID = int64(len(m.flushed))
	return m.latestID, nil
}
