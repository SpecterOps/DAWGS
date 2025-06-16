package graphcache

import "github.com/specterops/dawgs/graph"

func MaterializeIDPath(idPath graph.IDPath, cache Cache, tx graph.Transaction) (graph.Path, error) {
	var (
		path      = graph.AllocatePath(len(idPath.Edges))
		nodeIndex = make(map[graph.ID]int, len(idPath.Nodes))
		edgeIndex = make(map[graph.ID]int, len(idPath.Edges))
	)

	for idx, node := range idPath.Nodes {
		nodeIndex[node] = idx
	}

	for idx, edge := range idPath.Edges {
		edgeIndex[edge] = idx
	}

	if nodes, err := FetchNodesByID(tx, cache, idPath.Nodes); err != nil {
		return path, err
	} else {
		for _, node := range nodes {
			path.Nodes[nodeIndex[node.ID]] = node
		}
	}

	if edges, err := FetchRelationshipsByID(tx, cache, idPath.Edges); err != nil {
		return path, err
	} else {
		for _, edge := range edges {
			path.Edges[edgeIndex[edge.ID]] = edge
		}
	}

	return path, nil
}
