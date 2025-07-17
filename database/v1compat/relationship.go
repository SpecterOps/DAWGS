package v1compat

import (
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
)

type relationshipQuery struct {
	driver database.Driver
}

func newRelationshipQuery(driver database.Driver) RelationshipQuery {
	return &relationshipQuery{
		driver: driver,
	}
}

func (r relationshipQuery) Filter(criteria Criteria) RelationshipQuery {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Filterf(criteriaDelegate CriteriaProvider) RelationshipQuery {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Update(properties *graph.Properties) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) OrderBy(criteria ...Criteria) RelationshipQuery {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Offset(skip int) RelationshipQuery {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Limit(skip int) RelationshipQuery {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Count() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) First() (*graph.Relationship, error) {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Query(delegate func(results Result) error, finalCriteria ...Criteria) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) Fetch(delegate func(cursor Cursor[*graph.Relationship]) error) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) FetchDirection(direction graph.Direction, delegate func(cursor Cursor[DirectionalResult]) error) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) FetchTriples(delegate func(cursor Cursor[RelationshipTripleResult]) error) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) FetchAllShortestPaths(delegate func(cursor Cursor[graph.Path]) error) error {
	//TODO implement me
	panic("implement me")
}

func (r relationshipQuery) FetchKinds(delegate func(cursor Cursor[RelationshipKindsResult]) error) error {
	//TODO implement me
	panic("implement me")
}
