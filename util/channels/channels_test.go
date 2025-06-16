package channels_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/specterops/dawgs/util/channels"
	"github.com/stretchr/testify/require"
)

const (
	expectedValue = 1
)

func TestSubmit(t *testing.T) {
	var (
		channel                      = make(chan int)
		wg                           = &sync.WaitGroup{}
		cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		value, hasValue := channels.Receive(cancelCtx, channel)

		require.True(t, hasValue)
		require.Equal(t, expectedValue, value)
	}()

	// Should return true when the channel is successfully written to
	require.True(t, channels.Submit(cancelCtx, channel, expectedValue))

	// Should return false when the context has expired
	cancelCtxDoneFunc()
	require.False(t, channels.Submit(cancelCtx, channel, expectedValue))

	close(channel)
	wg.Wait()
}

func TestReceive(t *testing.T) {
	var (
		channel                      = make(chan int)
		wg                           = &sync.WaitGroup{}
		cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	)

	wg.Add(1)

	go func() {
		defer wg.Done()
		require.True(t, channels.Submit(cancelCtx, channel, expectedValue))
	}()

	// Should return true when the channel is successfully read from
	value, hasValue := channels.Receive(cancelCtx, channel)
	require.True(t, hasValue)
	require.Equal(t, expectedValue, value)

	// Should return false when the context has expired
	cancelCtxDoneFunc()

	_, hasValue = channels.Receive(cancelCtx, channel)
	require.False(t, hasValue)

	// Should return false when the readable channel is closed
	cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	defer cancelCtxDoneFunc()
	close(channel)

	_, hasValue = channels.Receive(cancelCtx, channel)
	require.False(t, hasValue)

	wg.Wait()
}

func TestPipe(t *testing.T) {
	var (
		inC                          = make(chan int)
		outC                         = make(chan int)
		wg                           = &sync.WaitGroup{}
		cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		require.True(t, channels.Submit(cancelCtx, inC, expectedValue))
	}()

	go func() {
		defer wg.Done()

		value, hasValue := <-outC

		require.True(t, hasValue)
		require.Equal(t, expectedValue, value)
	}()

	// Should return true when able to read from inC and write to outC
	require.True(t, channels.Pipe(cancelCtx, inC, outC))

	// Should return false when the context has expired
	cancelCtxDoneFunc()
	require.False(t, channels.Pipe(cancelCtx, inC, outC))

	// Should return false when the readable channel is closed
	cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	defer cancelCtxDoneFunc()
	close(inC)

	require.False(t, channels.Pipe(cancelCtx, inC, outC))

	wg.Wait()
}

func TestPipeline(t *testing.T) {
	var (
		inC                          = make(chan int)
		outC                         = make(chan string)
		wg                           = &sync.WaitGroup{}
		expectedErr                  = errors.New("error")
		expectedStrValue             = strconv.Itoa(expectedValue)
		cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
		convertf                     = func(value int) (string, error) {
			return strconv.Itoa(value), nil
		}
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		require.True(t, channels.Submit(cancelCtx, inC, expectedValue))
		require.True(t, channels.Submit(cancelCtx, inC, expectedValue))
	}()

	go func() {
		defer wg.Done()

		value, hasValue := <-outC

		require.True(t, hasValue)
		require.Equal(t, expectedStrValue, value)
	}()

	// Should return error and false when conversion fails
	success, err := channels.Pipeline(cancelCtx, inC, outC, func(value int) (string, error) {
		return "", expectedErr
	})

	require.False(t, success)
	require.Equal(t, err, expectedErr)

	// Should return no error and true when able to read from inC and write to outC
	success, err = channels.Pipeline(cancelCtx, inC, outC, convertf)

	require.True(t, success)
	require.Nil(t, err)

	// Should return false when the context has expired
	cancelCtxDoneFunc()
	success, err = channels.Pipeline(cancelCtx, inC, outC, convertf)

	require.False(t, success)
	require.Nil(t, err)

	// Should return false when the readable channel is closed
	cancelCtx, cancelCtxDoneFunc = context.WithCancel(context.Background())
	defer cancelCtxDoneFunc()
	close(inC)

	success, err = channels.Pipeline(cancelCtx, inC, outC, convertf)

	require.False(t, success)
	require.Nil(t, err)

	wg.Wait()
}
