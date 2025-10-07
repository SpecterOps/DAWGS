package container_test

import (
	"testing"

	"github.com/specterops/dawgs/container"
	"github.com/stretchr/testify/assert"
)

func TestPacmap(t *testing.T) {
	pacmap := container.NewPacMap(container.DefaultParameters())

	for kv := uint64(0); kv < 10; kv++ {
		pacmap.Put(kv, kv)
	}

	pacmap.Compact()

	for kv := uint64(0); kv < 10; kv++ {
		fetched, hasValue := pacmap.Get(kv)

		assert.True(t, hasValue)
		assert.Equal(t, kv, fetched)
	}
}
