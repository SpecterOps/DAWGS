package ops

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/specterops/dawgs/util/size"

	"github.com/specterops/dawgs/util/channels"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func FetchAllNodeProperties(tx graph.Transaction, nodes graph.NodeSet) error {
	return tx.Nodes().Filter(
		query.InIDs(query.NodeID(), nodes.IDs()...),
	).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
		for next := range cursor.Chan() {
			nodes[next.ID] = next
		}

		return cursor.Error()
	})
}

func FetchNodeProperties(tx graph.Transaction, nodes graph.NodeSet, propertyNames []string) error {
	returningCriteria := make([]graph.Criteria, len(propertyNames)+1)
	returningCriteria[0] = query.NodeID()

	for idx, propertyName := range propertyNames {
		returningCriteria[idx+1] = query.NodeProperty(propertyName)
	}

	return tx.Nodes().Filter(
		query.InIDs(query.NodeID(), nodes.IDs()...),
	).Query(func(results graph.Result) error {
		var (
			nodeID graph.ID
			mapper = results.Mapper()
		)

		for results.Next() {
			var (
				nodeProperties = map[string]any{}
				values         = results.Values()
			)

			// Map the node ID first
			if !mapper.Map(values[0], &nodeID) {
				return fmt.Errorf("expected node ID as first value but received %T", values[0])
			}

			// Map requested properties next by matching the name index
			for idx := 0; idx < len(propertyNames); idx++ {
				nodeProperties[propertyNames[idx]] = values[1+idx]
			}

			// Update the node in the node set
			nodes[nodeID].Properties = graph.AsProperties(nodeProperties)
		}

		return nil
	}, query.Returning(
		returningCriteria...,
	))
}

func DeleteNodes(tx graph.Transaction, nodeIDs ...graph.ID) error {
	return tx.Nodes().Filterf(func() graph.Criteria {
		return query.InIDs(query.NodeID(), nodeIDs...)
	}).Delete()
}

func DeleteRelationshipsQuery(relationshipIDs ...graph.ID) graph.CriteriaProvider {
	return func() graph.Criteria {
		return query.InIDs(query.RelationshipID(), relationshipIDs...)
	}
}

func DeleteRelationships(tx graph.Transaction, relationshipIDs ...graph.ID) error {
	return tx.Relationships().Filterf(DeleteRelationshipsQuery(relationshipIDs...)).Delete()
}

func FetchNodeSet(query graph.NodeQuery) (graph.NodeSet, error) {
	nodes := graph.NewNodeSet()

	return nodes, query.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
		for node := range cursor.Chan() {
			nodes.Add(node)
		}

		return cursor.Error()
	})
}

func DBFetchNodesByIDBitmap(ctx context.Context, db graph.Database, nodeIDs cardinality.Duplex[uint32]) ([]*graph.Node, error) {
	var nodes []*graph.Node

	return nodes, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		if fetchedNodes, err := TXFetchNodesByIDBitmap(tx, nodeIDs); err != nil {
			return err
		} else {
			nodes = fetchedNodes
		}

		return nil
	})
}

func TXFetchNodesByIDBitmap(tx graph.Transaction, nodeIDs cardinality.Duplex[uint32]) ([]*graph.Node, error) {
	return FetchNodes(tx.Nodes().Filter(query.InIDs(query.NodeID(), graph.Uint32SliceToIDs(nodeIDs.Slice())...)))
}

func FetchNodeIDsOfKindFromBitmap(tx graph.Transaction, nodeIDs cardinality.Duplex[uint32], kinds ...graph.Kind) ([]graph.ID, error) {
	return FetchNodeIDs(tx.Nodes().Filter(
		query.And(
			query.InIDs(query.NodeID(), graph.Uint32SliceToIDs(nodeIDs.Slice())...),
			query.KindIn(query.Node(), kinds...),
		)))
}

func FetchNodes(query graph.NodeQuery) ([]*graph.Node, error) {
	var nodes []*graph.Node

	return nodes, query.Fetch(func(cursor graph.Cursor[*graph.Node]) error {
		for node := range cursor.Chan() {
			nodes = append(nodes, node)
		}

		return cursor.Error()
	})
}

func FetchNodeIDs(query graph.NodeQuery) ([]graph.ID, error) {
	var ids []graph.ID

	return ids, query.FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
		for id := range cursor.Chan() {
			ids = append(ids, id)
		}

		return cursor.Error()
	})
}

func FetchNodesByQuery(tx graph.Transaction, query string, limit int) (graph.NodeSet, error) {
	nodes := graph.NodeSet{}

	if result := tx.Query(query, nil); result.Error() != nil {
		return nodes, result.Error()
	} else {
		defer result.Close()

		// Re: (limit <= 0 || nodes.Len() < limit)
		//
		// If the limit is set to < 0 the first part of this condition returns true and short-circuits. Otherwise,
		// the check goes on to see if there are more nodes in the node set than the specified allowed limit
		for (limit <= 0 || nodes.Len() < limit) && result.Next() {
			var node graph.Node

			if err := result.Scan(&node); err != nil {
				return nil, err
			}

			nodes.Add(&node)
		}

		return nodes, result.Error()
	}
}

func FetchPathSetByQuery(tx graph.Transaction, query string) (graph.PathSet, error) {
	var (
		currentPath graph.Path
		pathSet     graph.PathSet
	)

	if result := tx.Query(query, map[string]any{}); result.Error() != nil {
		return pathSet, result.Error()
	} else {
		defer result.Close()

		for result.Next() {
			var (
				relationship = &graph.Relationship{}
				node         = &graph.Node{}
				path         = &graph.Path{}
				mapper       = result.Mapper()
			)

			for _, nextValue := range result.Values() {
				if mapper.Map(nextValue, relationship) {
					currentPath.Edges = append(currentPath.Edges, relationship)
					relationship = &graph.Relationship{}
				} else if mapper.Map(nextValue, node) {
					currentPath.Nodes = append(currentPath.Nodes, node)
					node = &graph.Node{}
				} else if mapper.Map(nextValue, path) {
					pathSet = append(pathSet, *path)
					path = &graph.Path{}
				}
			}

			if tx.GraphQueryMemoryLimit() > 0 {
				var (
					currentPathSize = size.OfSlice(currentPath.Edges) + size.OfSlice(currentPath.Nodes)
					pathSetSize     = size.Of(pathSet)
				)

				if currentPathSize > tx.GraphQueryMemoryLimit() || pathSetSize > tx.GraphQueryMemoryLimit() {
					return pathSet, fmt.Errorf("%s - Limit: %.2f MB", "query required more memory than allowed", tx.GraphQueryMemoryLimit().Mebibytes())
				}
			}
		}

		// If there were elements added to the current path ensure that it's added to the path set before returning
		if len(currentPath.Nodes) > 0 || len(currentPath.Edges) > 0 {
			pathSet = append(pathSet, currentPath)
		}

		return pathSet, result.Error()
	}
}

func FetchNode(tx graph.Transaction, id graph.ID) (*graph.Node, error) {
	return tx.Nodes().Filterf(func() graph.Criteria {
		return query.Equals(query.NodeID(), id)
	}).First()
}

func FetchRelationship(tx graph.Transaction, id graph.ID) (*graph.Relationship, error) {
	return tx.Relationships().Filterf(func() graph.Criteria {
		return query.Equals(query.RelationshipID(), id)
	}).First()
}

func FetchNodeRelationships(tx graph.Transaction, root *graph.Node, direction graph.Direction) ([]*graph.Relationship, error) {
	var queryCriteria graph.Criteria

	switch direction {
	case graph.DirectionInbound:
		queryCriteria = query.InIDs(query.EndID(), root.ID)

	case graph.DirectionOutbound:
		queryCriteria = query.InIDs(query.StartID(), root.ID)

	default:
		return nil, fmt.Errorf("unexpected direction %d", direction)
	}

	return FetchRelationships(tx.Relationships().Filterf(func() graph.Criteria {
		return queryCriteria
	}))
}

func CollectNodeIDs(relationshipQuery graph.RelationshipQuery) (RelationshipNodes, error) {
	var relNodeIDs RelationshipNodes

	return relNodeIDs, relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
		for relationship := range cursor.Chan() {
			relNodeIDs.Add(relationship)
		}

		return cursor.Error()
	})
}

// Note this does not work with mutations inside the delegate
func ForEachNodeID(tx graph.Transaction, ids []graph.ID, delegate func(node *graph.Node) error) error {
	return tx.Nodes().Filterf(func() graph.Criteria {
		return query.InIDs(query.NodeID(), ids...)
	}).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
		for node := range cursor.Chan() {
			if err := delegate(node); err != nil {
				return err
			}
		}

		return cursor.Error()
	})
}

func ForEachStartNode(relationshipQuery graph.RelationshipQuery, delegate func(relationship *graph.Relationship, node *graph.Node) error) error {
	return relationshipQuery.FetchDirection(graph.DirectionOutbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
		for result := range cursor.Chan() {
			if err := delegate(result.Relationship, result.Node); err != nil {
				return err
			}
		}

		return cursor.Error()
	})
}

func ForEachEndNode(relationshipQuery graph.RelationshipQuery, delegate func(relationship *graph.Relationship, node *graph.Node) error) error {
	return relationshipQuery.FetchDirection(graph.DirectionInbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
		for result := range cursor.Chan() {
			if err := delegate(result.Relationship, result.Node); err != nil {
				return err
			}
		}

		return cursor.Error()
	})
}

type RelationshipNodes struct {
	Start []graph.ID
	End   []graph.ID
}

func (s *RelationshipNodes) Add(relationship *graph.Relationship) {
	s.Start = append(s.Start, relationship.StartID)
	s.End = append(s.End, relationship.EndID)
}

func FetchStartNodeIDs(query graph.RelationshipQuery) ([]graph.ID, error) {
	var ids []graph.ID

	return ids, ForEachStartNode(query, func(_ *graph.Relationship, node *graph.Node) error {
		ids = append(ids, node.ID)
		return nil
	})
}

func FetchStartNodes(query graph.RelationshipQuery) (graph.NodeSet, error) {
	nodes := graph.NewNodeSet()

	return nodes, ForEachStartNode(query, func(_ *graph.Relationship, node *graph.Node) error {
		nodes.Add(node)
		return nil
	})
}

func FetchEndNodes(query graph.RelationshipQuery) (graph.NodeSet, error) {
	nodes := graph.NewNodeSet()

	return nodes, ForEachEndNode(query, func(_ *graph.Relationship, node *graph.Node) error {
		nodes.Add(node)
		return nil
	})
}

func FetchRelationships(query graph.RelationshipQuery) ([]*graph.Relationship, error) {
	var relationships []*graph.Relationship

	return relationships, query.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
		for relationship := range cursor.Chan() {
			relationships = append(relationships, relationship)
		}

		return cursor.Error()
	})
}

func FetchRelationshipIDs(query graph.RelationshipQuery) ([]graph.ID, error) {
	var relationshipIDs []graph.ID

	return relationshipIDs, query.FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
		for relationshipID := range cursor.Chan() {
			relationshipIDs = append(relationshipIDs, relationshipID)
		}

		return cursor.Error()
	})
}

func FetchPathSet(queryInst graph.RelationshipQuery) (graph.PathSet, error) {
	pathSet := graph.NewPathSet()

	return pathSet, queryInst.Query(func(results graph.Result) error {
		defer results.Close()

		for results.Next() {
			var (
				start, end graph.Node
				edge       graph.Relationship
			)

			if err := results.Scan(&start, &edge, &end); err != nil {
				return err
			} else {
				pathSet.AddPath(graph.Path{
					Nodes: []*graph.Node{&start, &end},
					Edges: []*graph.Relationship{&edge},
				})
			}
		}

		return results.Error()
	}, query.Returning(
		query.Start(), query.Relationship(), query.End(),
	))
}

func FetchRelationshipNodes(tx graph.Transaction, relationship *graph.Relationship) (*graph.Node, *graph.Node, error) {
	if nodes, err := FetchNodes(tx.Nodes().Filterf(func() graph.Criteria {
		return query.InIDs(query.NodeID(), relationship.StartID, relationship.EndID)
	})); err != nil {
		return nil, nil, err
	} else if len(nodes) != 2 {
		return nil, nil, graph.ErrNoResultsFound
	} else {
		var (
			startNode = nodes[0]
			endNode   = nodes[1]
		)

		if startNode.ID != relationship.StartID {
			startNode = nodes[1]
			endNode = nodes[0]
		}

		return startNode, endNode, nil
	}
}

// CountNodes will fetch the current number of nodes in the database that match the given criteria
func CountNodes(ctx context.Context, db graph.Database, criteria ...graph.Criteria) (int64, error) {
	var (
		nodeCount int64

		err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			if fetchedNodeCount, err := tx.Nodes().Filter(query.And(criteria...)).Count(); err != nil {
				return err
			} else {
				nodeCount = fetchedNodeCount
			}

			return nil
		})
	)

	return nodeCount, err
}

// FetchLargestNodeID will fetch the current node database identifier ceiling.
func FetchLargestNodeID(ctx context.Context, db graph.Database) (graph.ID, error) {
	var (
		largestNodeID graph.ID

		err = db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			if node, err := tx.Nodes().OrderBy(query.Order(query.NodeID(), query.Descending())).Limit(1).First(); err != nil {
				return err
			} else {
				largestNodeID = node.ID
			}

			return nil
		})
	)

	return largestNodeID, err
}

func parallelFetchNodes(ctx context.Context, db graph.Database, maxID graph.ID, criteria graph.Criteria, numWorkers int) (graph.NodeSet, error) {
	const stride = 20_000

	var (
		rangeC   = make(chan graph.ID)
		nodeC    = make(chan *graph.Node)
		errorC   = make(chan error)
		nodes    = graph.NodeSet{}
		workerWG = &sync.WaitGroup{}
		mergeWG  = &sync.WaitGroup{}
		errs     []error
	)

	// Fetch workers
	for workerID := 0; workerID < numWorkers; workerID++ {
		workerWG.Add(1)

		go func() {
			defer workerWG.Done()

			if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
				// Create a slice of criteria to join the node ID range constraints to any passed user criteria
				var criteriaSlice []graph.Criteria

				if criteria != nil {
					criteriaSlice = append(criteriaSlice, criteria)
				}

				// Select the next node ID range floor while honoring context cancellation and channel closure
				nextRangeFloor, channelOpen := channels.Receive(ctx, rangeC)

				for channelOpen {
					if err := tx.Nodes().Filter(query.And(
						append(criteriaSlice,
							query.GreaterThanOrEquals(query.NodeID(), nextRangeFloor),
							query.LessThan(query.NodeID(), nextRangeFloor+stride),
						)...,
					)).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
						channels.PipeAll(ctx, cursor.Chan(), nodeC)
						return cursor.Error()
					}); err != nil {
						return err
					}

					nextRangeFloor, channelOpen = channels.Receive(ctx, rangeC)
				}

				return nil
			}); err != nil {
				channels.Submit(ctx, errorC, err)
			}
		}()
	}

	// Merge goroutine for fetched nodes and collected errors
	mergeWG.Add(1)

	go func() {
		defer mergeWG.Done()

		for {
			select {
			case <-ctx.Done():
				// Bail if the context is canceled
				return

			case nextNode, channelOpen := <-nodeC:
				if !channelOpen {
					// Channel closure indicates completion of work and join of the parallel workers
					return
				}

				nodes.Add(nextNode)

			case nextErr, channelOpen := <-errorC:
				if !channelOpen {
					// Channel closure indicates completion of work and join of the parallel workers
					return
				}

				errs = append(errs, nextErr)
			}
		}
	}()

	// Iterate through node ID ranges up to the maximum ID by the stride constant
	for nextRangeFloor := graph.ID(0); nextRangeFloor <= maxID; nextRangeFloor += stride {
		channels.Submit(ctx, rangeC, nextRangeFloor)
	}

	// Stop the fetch workers
	close(rangeC)
	workerWG.Wait()

	// Wait for the merge routine to join to ensure that both the nodes instance and the errs instance contain
	// everything to be collected from the parallel workers
	close(nodeC)
	close(errorC)
	mergeWG.Wait()

	// Return the fetched nodes and joined errors lastly
	return nodes, errors.Join(errs...)
}

// ParallelFetchNodes will first look up the largest node database identifier. The function will then spin up to
// numWorkers parallel read transactions. Each transaction will apply the user passed criteria to this function to a
// range of node database identifiers to avoid parallel worker collisions.
func ParallelFetchNodes(ctx context.Context, db graph.Database, criteria graph.Criteria, numWorkers int) (graph.NodeSet, error) {
	if largestNodeID, err := FetchLargestNodeID(ctx, db); err != nil {
		if graph.IsErrNotFound(err) {
			return graph.NodeSet{}, nil
		}

		return nil, err
	} else {
		return parallelFetchNodes(ctx, db, largestNodeID, criteria, numWorkers)
	}
}
