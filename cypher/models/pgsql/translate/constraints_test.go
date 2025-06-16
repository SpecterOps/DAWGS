package translate

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/stretchr/testify/require"
)

func TestMeasureSelectivity(t *testing.T) {
	selectivity, err := MeasureSelectivity(NewScope(), false, pgd.Equals(
		pgsql.Identifier("123"),
		pgsql.Identifier("456"),
	))

	require.Nil(t, err)
	require.Equal(t, 30, selectivity)
}
