package channels

import "context"

type ConcurrencyLimiter struct {
	slotC chan struct{}
}

func NewConcurrencyLimiter(numSlots int) ConcurrencyLimiter {
	return ConcurrencyLimiter{
		slotC: make(chan struct{}, numSlots),
	}
}

func (s ConcurrencyLimiter) Acquire(ctx context.Context) bool {
	return Submit(ctx, s.slotC, struct{}{})
}

func (s ConcurrencyLimiter) Release() {
	<-s.slotC
}
