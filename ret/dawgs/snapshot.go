package dawgs

import "github.com/specterops/dawgs/ret/entity"

type Snapshot struct {
	NodeCount         int64
	RelationshipCount int64
}

type NodeBatch struct {
	Entities []entity.Node
	LastID   uint64
}

type RelationshipBatch struct {
	Entities []entity.Relationship
	LastID   uint64
}
