package v2

import (
	"context"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

func FetchNodes(query *cypher.RegularQuery, nextNodeFunc func(node *graph.Node) error) DriverLogic {
	var nextNode graph.Node

	return func(ctx context.Context, driver Driver) error {
		result := driver.Execute(ctx, query, nil)

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
