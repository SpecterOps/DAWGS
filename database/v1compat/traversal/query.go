package traversal

import (
	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/database/v1compat/query"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/graphcache"
)

func fetchNodesByIDQuery(tx v1compat.Transaction, ids []v1compat.ID) v1compat.NodeQuery {
	return tx.Nodes().Filterf(func() v1compat.Criteria {
		return query.InIDs(query.NodeID(), ids...)
	})
}

func ShallowFetchNodesByID(tx v1compat.Transaction, cache graphcache.Cache, ids []v1compat.ID) ([]*v1compat.Node, error) {
	cachedNodes, missingNodeIDs := cache.GetNodes(ids)

	if len(missingNodeIDs) > 0 {
		newNodes := make([]*v1compat.Node, 0, len(missingNodeIDs))

		if err := fetchNodesByIDQuery(tx, missingNodeIDs).FetchKinds(func(cursor v1compat.Cursor[v1compat.KindsResult]) error {
			for next := range cursor.Chan() {
				newNodes = append(newNodes, v1compat.NewNode(next.ID, nil, next.Kinds...))
			}

			return cursor.Error()
		}); err != nil {
			return nil, err
		}

		// Put the fetched nodes into cache
		cache.PutNodes(newNodes)

		// Verify all requested nodes were fetched
		if len(newNodes) != len(missingNodeIDs) {
			return nil, graph.ErrMissingResultExpectation
		}

		// Append them to the end of the nodes being returned
		cachedNodes = append(cachedNodes, newNodes...)
	}

	return cachedNodes, nil
}
