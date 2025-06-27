package v2

import (
	"context"

	"github.com/specterops/dawgs/graph"
)

func FetchNodes(preparedQuery *PreparedQuery, nextNodeFunc func(node *graph.Node) error) DriverLogic {
	var nextNode graph.Node

	return func(ctx context.Context, driver Driver) error {
		result := driver.CypherQuery(ctx, preparedQuery.Query, preparedQuery.Parameters)

		for result.HasNext(ctx) {
			if err := result.Scan(&nextNode); err != nil {
				return err
			}

			if err := nextNodeFunc(&nextNode); err != nil {
				return err
			}
		}

		return result.Error()
	}
}
