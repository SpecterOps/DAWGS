package ops

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func FetchNodes(preparedQuery *query.PreparedQuery, nextNodeFunc func(node *graph.Node) error) database.QueryLogic {
	var nextNode graph.Node

	return func(ctx context.Context, driver database.Driver) error {
		result := driver.Exec(ctx, preparedQuery.Query, preparedQuery.Parameters)

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

func FetchDirection(queryBuilder query.QueryBuilder, direction graph.Direction, nextDirectionFunc func(node *graph.Node, relationship *graph.Relationship) error) database.QueryLogic {
	var (
		nextNode         graph.Node
		nextRelationship graph.Relationship
	)

	return func(ctx context.Context, driver database.Driver) error {
		switch direction {
		case graph.DirectionInbound:
			queryBuilder.Where(
				query.End())

			queryBuilder.Return(
				query.Start(),
				query.Relationship(),
			)

		case graph.DirectionOutbound:
			queryBuilder.Return(
				query.End(),
				query.Relationship(),
			)

		default:
			return fmt.Errorf("unsupported direction: %v", direction)
		}

		if preparedQuery, err := queryBuilder.Build(); err != nil {
			return err
		} else {
			result := driver.Exec(ctx, preparedQuery.Query, preparedQuery.Parameters)

			for result.HasNext(ctx) {
				if err := result.Scan(&nextNode); err != nil {
					return err
				}

				if err := nextDirectionFunc(&nextNode, &nextRelationship); err != nil {
					return err
				}
			}

			return result.Error()
		}
	}
}
