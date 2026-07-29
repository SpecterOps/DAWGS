package observe_test

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/ret/observe"
	"github.com/stretchr/testify/require"
)

func TestEmitAllowsNilObserver(t *testing.T) {
	require.NotPanics(t, func() {
		observe.Emit(context.Background(), nil, observe.OperationStarted{Operation: "dump"})
	})
}

func TestObserverFuncReceivesTypedEvent(t *testing.T) {
	var got observe.Event
	observer := observe.ObserverFunc(func(_ context.Context, event observe.Event) { got = event })

	observe.Emit(context.Background(), observer, observe.GraphStarted{Operation: "dump", Graph: "asset"})

	require.Equal(t, observe.GraphStarted{Operation: "dump", Graph: "asset"}, got)
}
