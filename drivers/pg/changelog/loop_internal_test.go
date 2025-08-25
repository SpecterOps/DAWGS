package changelog

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs/util/channels"
	"github.com/stretchr/testify/require"
)

func TestLoop(t *testing.T) {
	t.Run("flushes nodes on batch size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, 2)

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "1"})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "2"})))

		// Run one iteration
		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		require.Len(t, db.flushedNodes, 2)
	})

	t.Run("flushes edges on batch size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, 2)

		// queue up nodes < batchSize, edges >= batchSize
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "1"})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(EdgeChange{})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(EdgeChange{})))

		// Run one iteration
		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		require.Len(t, db.flushedNodes, 0)
		require.Len(t, db.flushedEdges, 2)
	})
	t.Run("no flush happens before batch size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, 3)

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "1"})))
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "2"})))

		// Run one iteration
		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		require.Len(t, db.flushedNodes, 0) // nothing was flushed because buffer never reached batch_size
	})
	t.Run("timer triggers flush after inactivity", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := &mockFlusher{}
		loop := newLoop(ctx, db, 3)
		loop.FlushInterval = 20 * time.Millisecond // best effort

		// Inject two changes. explicitly cast the NodeChange bc generics jank
		require.True(t, channels.Submit(ctx, loop.WriterC, Change(NodeChange{NodeID: "1"})))

		go func() { _ = loop.start(ctx) }()
		time.Sleep(50 * time.Millisecond) // wait longer than flush interval

		require.Len(t, db.flushedNodes, 1)
	})
}

type mockFlusher struct {
	mu           sync.Mutex
	flushedNodes []NodeChange
	flushedEdges []EdgeChange
	latestID     int64
}

func (m *mockFlusher) flushNodeChanges(_ context.Context, changes []NodeChange) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushedNodes = append(m.flushedNodes, changes...)
	m.latestID = int64(len(m.flushedNodes))
	return m.latestID, nil
}
func (m *mockFlusher) flushEdgeChanges(_ context.Context, changes []EdgeChange) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushedEdges = append(m.flushedEdges, changes...)
	m.latestID = int64(len(m.flushedEdges))
	return m.latestID, nil
}
