package frontend_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/test"
)

func TestUnsupportedOperationFilter(t *testing.T) {
	test.LoadFixture(t, test.FilteringTestCases).Run(t)
}
