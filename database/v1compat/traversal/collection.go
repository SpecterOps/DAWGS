package traversal

import (
	"context"
	"sync"

	"github.com/specterops/dawgs/database/v1compat"
	"github.com/specterops/dawgs/database/v1compat/ops"
)

type NodeCollector struct {
	Nodes v1compat.NodeSet
	lock  *sync.Mutex
}

func NewNodeCollector() *NodeCollector {
	return &NodeCollector{
		Nodes: v1compat.NewNodeSet(),
		lock:  &sync.Mutex{},
	}
}

func (s *NodeCollector) Collect(next *v1compat.PathSegment) {
	s.Add(next.Node)
}

func (s *NodeCollector) PopulateProperties(ctx context.Context, db v1compat.Database, propertyNames ...string) error {
	return db.ReadTransaction(ctx, func(tx v1compat.Transaction) error {
		return ops.FetchNodeProperties(tx, s.Nodes, propertyNames)
	})
}

func (s *NodeCollector) Add(node *v1compat.Node) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Nodes.Add(node)
}

type PathCollector struct {
	Paths v1compat.PathSet
	lock  *sync.Mutex
}

func NewPathCollector() *PathCollector {
	return &PathCollector{
		lock: &sync.Mutex{},
	}
}

func (s *PathCollector) PopulateNodeProperties(ctx context.Context, db v1compat.Database, propertyNames ...string) error {
	return db.ReadTransaction(ctx, func(tx v1compat.Transaction) error {
		return ops.FetchNodeProperties(tx, s.Paths.AllNodes(), propertyNames)
	})
}

func (s *PathCollector) Add(path v1compat.Path) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Paths = append(s.Paths, path)
}
