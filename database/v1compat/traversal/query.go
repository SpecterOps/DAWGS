package traversal

import (
	graph "github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/database/v1compat/query"
	"github.com/specterops/dawgs/graphcache"
)

func fetchNodesByIDQuery(tx graph.Transaction, ids []graph.ID) graph.NodeQuery {
	return tx.Nodes().Filterf(func() graph.Criteria {
		return query.InIDs(query.NodeID(), ids...)
	})
}

func fetchRelationshipsByIDQuery(tx graph.Transaction, ids []graph.ID) graph.RelationshipQuery {
	return tx.Relationships().Filterf(func() graph.Criteria {
		return query.InIDs(query.RelationshipID(), ids...)
	})
}

func FetchNodesByID(tx graph.Transaction, cache graphcache.Cache, ids []graph.ID) ([]*graph.Node, error) {
	var (
		cachedNodes, missingNodeIDs = cache.GetNodes(ids)
		toBeCachedCount             = 0
	)

	if len(missingNodeIDs) > 0 {
		if err := fetchNodesByIDQuery(tx, missingNodeIDs).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for next := range cursor.Chan() {
				cachedNodes = append(cachedNodes, next)
				toBeCachedCount++
			}

			return cursor.Error()
		}); err != nil {
			return nil, err
		}

		if toBeCachedCount > 0 {
			cache.PutNodes(cachedNodes[len(cachedNodes)-toBeCachedCount:])
		}

		if len(missingNodeIDs) != toBeCachedCount {
			return nil, graph.ErrMissingResultExpectation
		}
	}

	return cachedNodes, nil
}

func FetchRelationshipsByID(tx graph.Transaction, cache graphcache.Cache, ids []graph.ID) (graph.RelationshipSet, error) {
	cachedRelationships, missingRelationshipIDs := cache.GetRelationships(ids)

	if len(missingRelationshipIDs) > 0 {
		if err := fetchRelationshipsByIDQuery(tx, ids).Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for next := range cursor.Chan() {
				cachedRelationships = append(cachedRelationships, next)
			}

			return cursor.Error()
		}); err != nil {
			return nil, err
		}

		cache.PutRelationships(cachedRelationships[len(cachedRelationships)-len(missingRelationshipIDs):])
	}

	return graph.NewRelationshipSet(cachedRelationships...), nil
}

func ShallowFetchRelationships(cache graphcache.Cache, graphQuery graph.RelationshipQuery) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship

	if err := graphQuery.FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
		for next := range cursor.Chan() {
			relationships = append(relationships, graph.NewRelationship(next.ID, next.StartID, next.EndID, nil, next.Kind))
		}

		return cursor.Error()
	}); err != nil {
		return nil, err
	}

	cache.PutRelationships(relationships)
	return relationships, nil
}

func ShallowFetchNodesByID(tx graph.Transaction, cache graphcache.Cache, ids []graph.ID) ([]*graph.Node, error) {
	cachedNodes, missingNodeIDs := cache.GetNodes(ids)

	if len(missingNodeIDs) > 0 {
		newNodes := make([]*graph.Node, 0, len(missingNodeIDs))

		if err := fetchNodesByIDQuery(tx, missingNodeIDs).FetchKinds(func(cursor graph.Cursor[graph.KindsResult]) error {
			for next := range cursor.Chan() {
				newNodes = append(newNodes, graph.NewNode(next.ID, nil, next.Kinds...))
			}

			return cursor.Error()
		}); err != nil {
			return nil, err
		}

		// Put the fetched nodes into cache
		cache.PutNodes(newNodes)

		// Append them to the end of the nodes being returned
		cachedNodes = append(cachedNodes, newNodes...)
	}

	return cachedNodes, nil
}
