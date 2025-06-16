package query

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

type SortDirection string

const SortDirectionAscending SortDirection = "asc"
const SortDirectionDescending SortDirection = "desc"

type SortItem struct {
	SortCriteria graph.Criteria
	Direction    SortDirection
}

type SortItems []SortItem

func (s SortItems) FormatCypherOrder() *cypher.Order {
	var orderCriteria []graph.Criteria

	for _, sortItem := range s {
		switch sortItem.Direction {
		case SortDirectionAscending:
			orderCriteria = append(orderCriteria, Order(sortItem.SortCriteria, Ascending()))
		case SortDirectionDescending:
			orderCriteria = append(orderCriteria, Order(sortItem.SortCriteria, Descending()))
		}
	}
	return OrderBy(orderCriteria...)
}
