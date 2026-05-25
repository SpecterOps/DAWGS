package translate

import (
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

func MeasureSelectivity(scope *Scope, expression pgsql.Expression) (int, error) {
	return optimize.MeasureSelectivity(scope, expression)
}
