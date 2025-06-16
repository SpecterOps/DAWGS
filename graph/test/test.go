package test

import (
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func RequireProperty[T any](t *testing.T, expected T, actual graph.PropertyValue, msg ...any) {
	var (
		value = actual.Any()
		err   error
	)

	switch any(expected).(type) {
	case time.Time:
		value, err = actual.Time()
	}

	require.Nil(t, err, msg...)
	require.Equal(t, expected, value, msg...)
}
