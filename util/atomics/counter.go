package atomics

import "sync/atomic"

// Counter reports whether it found its bound reached instead of incrementing.
type Counter func() bool

// NewCounter returns a concurrency-safe counter initially at zero.
// Each successful increment, including the increment to maximum, returns false.
// Once maximum increments have succeeded, every later call returns true without
// incrementing. A zero maximum returns true on every call.
func NewCounter[T uint32 | uint64](maximum T) Counter {
	var counterFunc Counter

	switch typedMaximum := any(maximum).(type) {
	case uint32:
		counter := &atomic.Uint32{}
		counterFunc = func() bool {
			for currentValue := counter.Load(); currentValue < typedMaximum; currentValue = counter.Load() {
				if counter.CompareAndSwap(currentValue, currentValue+1) {
					return false
				}
			}

			return true
		}

	case uint64:
		counter := &atomic.Uint64{}
		counterFunc = func() bool {
			for currentValue := counter.Load(); currentValue < typedMaximum; currentValue = counter.Load() {
				if counter.CompareAndSwap(currentValue, currentValue+1) {
					return false
				}
			}

			return true
		}
	}

	return counterFunc
}
