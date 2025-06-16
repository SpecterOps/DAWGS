package atomics

import "sync/atomic"

type Counter func() bool

// NewCounter returns a function that atomically counts to a given maximum upon call.
//
// When the resulting closure is called the function enters a for-loop that atomically loads the value
// at address &counter. This value is then compared against the given maximum. If the value is greater than
// or equal to the given maximum this function returns the current value and true to signify completion.
//
// If the value at address &counter at time of the atomic load is less than the given maximum this function
// attempts to swap the value at address &counter with a value equal to the previous value +1. Upon successfully
// setting a new value this function returns the new value and false to signify that the counter has not
// yet reached the given maximum.
//
// The use of compare-and-swap ensures that if the value at address &counter has not yet changed, the function
// atomically replaces it with the previous value +1. If the compare-and-swap fails and returns false then
// another agent as modified the value at address &counter. In this case the function rewinds to the beginning
// of the for-loop for another counter increment attempt.
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
