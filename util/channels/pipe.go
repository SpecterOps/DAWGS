package channels

import (
	"context"

	"github.com/gammazero/deque"
)

// BufferedPipe is a generic, channel and deque buffered pipe implementation.
func BufferedPipe[T any](ctx context.Context) (chan<- T, <-chan T) {
	var (
		// Create two channels for the read and write goroutines
		writerC = make(chan T)
		readerC = make(chan T)

		// Buffer to hold values when there's no downstream reader available
		buffer = deque.Deque[T]{}

		// getNext peeks at the next value in the buffer. If the buffer is empty the zero-value of T is returned
		// instead.
		getNext = func() T {
			var next T

			if buffer.Len() > 0 {
				next = buffer.Front()
			}

			return next
		}

		// getReaderC returns the reader channel only if there are items in the buffer. If the buffer is empty then
		// this function returns nil.
		getReaderC = func() chan T {
			if buffer.Len() > 0 {
				return readerC
			}

			return nil
		}
	)

	go func() {
		defer close(readerC)

		// Run in this for-loop for as long as the writer channel is open
		for doneReading := false; !doneReading; {
			select {
			case <-ctx.Done():
				// If the context was canceled, exit right away
				return

			case next, ok := <-writerC:
				if !ok {
					// If the writer channel has been closed then we're done reading
					doneReading = true
				} else {
					buffer.PushBack(next)
				}

			// If there is no work in the buffer both functions in the below select-case will return zero-value
			// references that will short-circuit this case
			case getReaderC() <- getNext():
				buffer.PopFront()
			}
		}

		// Once we're done reading from the writer channel we need to flush the buffer to the reader channel
		for buffer.Len() > 0 {
			select {
			case <-ctx.Done():
				// If the context was canceled, exit right away
				return

			case readerC <- buffer.Front():
				buffer.PopFront()
			}
		}
	}()

	return writerC, readerC
}
