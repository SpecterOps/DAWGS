package observe

import "context"

// Observer receives operation observation events.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function to the Observer interface.
type ObserverFunc func(context.Context, Event)

// Observe delivers an event to s.
func (s ObserverFunc) Observe(ctx context.Context, event Event) {
	s(ctx, event)
}

// Emit delivers an event when an observer is configured.
func Emit(ctx context.Context, observer Observer, event Event) {
	if observer != nil {
		observer.Observe(ctx, event)
	}
}
