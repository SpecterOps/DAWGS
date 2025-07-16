package graphcache

import (
	"sync"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

type Cache struct {
	nodes         *graph.IndexedSlice[graph.ID, *graph.Node]
	relationships *graph.IndexedSlice[graph.ID, *graph.Relationship]
	lock          *sync.RWMutex
}

func New() Cache {
	return Cache{
		nodes:         graph.NewIndexedSlice[graph.ID, *graph.Node](),
		relationships: graph.NewIndexedSlice[graph.ID, *graph.Relationship](),
		lock:          &sync.RWMutex{},
	}
}

func (s Cache) SizeOf() size.Size {
	// We're not including the total size of the lock here on purpose. Copying a locker can be dangerous and to get
	// the size of the backing struct for the pointer we would have to dereference and copy it.
	return size.Of(s) + s.nodes.SizeOf() + s.relationships.SizeOf()
}

func (s Cache) NodesCached() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.nodes.Len()
}

func (s Cache) RelationshipsCached() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.nodes.Len()
}

func (s Cache) GetNode(id graph.ID) *graph.Node {
	s.lock.RLock()
	defer s.lock.RUnlock()

	node, _ := s.nodes.CheckedGet(id)
	return node
}

func (s Cache) CheckedGetNode(id graph.ID) (*graph.Node, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.nodes.CheckedGet(id)
}

func (s Cache) GetNodes(ids []graph.ID) ([]*graph.Node, []graph.ID) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.nodes.GetAll(ids)
}

func (s Cache) PutNodes(nodes []*graph.Node) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, node := range nodes {
		s.nodes.Put(node.ID, node)
	}
}

func (s Cache) PutNodeSet(nodes graph.NodeSet) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, node := range nodes {
		s.nodes.Put(node.ID, node)
	}
}

func (s Cache) GetRelationship(id graph.ID) *graph.Relationship {
	s.lock.RLock()
	defer s.lock.RUnlock()

	relationship, _ := s.relationships.CheckedGet(id)
	return relationship
}

func (s Cache) CheckedGetRelationship(id graph.ID) (*graph.Relationship, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.relationships.CheckedGet(id)
}

func (s Cache) GetRelationships(ids []graph.ID) ([]*graph.Relationship, []graph.ID) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.relationships.GetAll(ids)
}

func (s Cache) PutRelationships(relationships []*graph.Relationship) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, relationship := range relationships {
		s.relationships.Put(relationship.ID, relationship)
	}
}

func (s Cache) PutRelationshipSet(relationships graph.RelationshipSet) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, relationship := range relationships {
		s.relationships.Put(relationship.ID, relationship)
	}
}
