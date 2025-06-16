package traversal

import (
	"context"
	"sync"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
)

type NodeCollector struct {
	Nodes graph.NodeSet
	lock  *sync.Mutex
}

func NewNodeCollector() *NodeCollector {
	return &NodeCollector{
		Nodes: graph.NewNodeSet(),
		lock:  &sync.Mutex{},
	}
}

func (s *NodeCollector) Collect(next *graph.PathSegment) {
	s.Add(next.Node)
}

func (s *NodeCollector) PopulateProperties(ctx context.Context, db graph.Database, propertyNames ...string) error {
	return db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return ops.FetchNodeProperties(tx, s.Nodes, propertyNames)
	})
}

func (s *NodeCollector) Add(node *graph.Node) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Nodes.Add(node)
}

type PathCollector struct {
	Paths graph.PathSet
	lock  *sync.Mutex
}

func NewPathCollector() *PathCollector {
	return &PathCollector{
		lock: &sync.Mutex{},
	}
}

func (s *PathCollector) PopulateNodeProperties(ctx context.Context, db graph.Database, propertyNames ...string) error {
	return db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return ops.FetchNodeProperties(tx, s.Paths.AllNodes(), propertyNames)
	})
}

func (s *PathCollector) Add(path graph.Path) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Paths = append(s.Paths, path)
}
