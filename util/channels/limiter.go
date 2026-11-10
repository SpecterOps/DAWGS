package channels

import "context"

// ConcurrencyLimiter bounds concurrent work with a shared channel of acquired slots.
type ConcurrencyLimiter struct {
	// slotC holds one token per acquired slot; copies of the limiter share this channel.
	slotC chan struct{}
}

// NewConcurrencyLimiter creates a limiter with numSlots concurrent slots.
func NewConcurrencyLimiter(numSlots int) ConcurrencyLimiter {
	return ConcurrencyLimiter{
		slotC: make(chan struct{}, numSlots),
	}
}

// Acquire waits for a slot or context cancellation and reports whether a slot was acquired.
func (s ConcurrencyLimiter) Acquire(ctx context.Context) bool {
	return Submit(ctx, s.slotC, struct{}{})
}

// Release consumes one acquired slot, blocking if none is available.
func (s ConcurrencyLimiter) Release() {
	<-s.slotC
}
