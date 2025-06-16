package atomics_test

import (
	"sync"
	"testing"

	"github.com/specterops/dawgs/util/atomics"
	"github.com/stretchr/testify/require"
)

func TestNewCounterToMaximum(t *testing.T) {
	t.Run("Count to 0", func(t *testing.T) {
		var (
			counter    = atomics.NewCounter[uint32](0)
			iterations = 0
		)

		for !counter() {
			iterations++
		}

		require.Equal(t, 0, iterations)
	})

	t.Run("Count to 10", func(t *testing.T) {
		var (
			counter    = atomics.NewCounter[uint32](10)
			iterations = 0
		)

		for !counter() {
			iterations++
		}

		require.Equal(t, 10, iterations)
	})

	t.Run("10 goroutines counting to 10240", func(t *testing.T) {
		var (
			counter1  = atomics.NewCounter[uint32](10240)
			counter2  = atomics.NewCounter[uint32](10240)
			waitGroup = &sync.WaitGroup{}
		)

		for workerID := 0; workerID < 10; workerID++ {
			waitGroup.Add(1)

			go func() {
				defer waitGroup.Done()

				for !counter1() {
					if counter2() {
						t.Errorf("Check counter reached limit")
						return
					}
				}
			}()
		}

		waitGroup.Wait()
	})
}
