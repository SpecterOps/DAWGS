package v1compat

import (
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
)

type nodeQuery struct {
	driver database.Driver
}

func newNodeQuery(driver database.Driver) NodeQuery {
	return &nodeQuery{
		driver: driver,
	}
}

func (n nodeQuery) Filter(criteria Criteria) NodeQuery {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Filterf(criteriaDelegate CriteriaProvider) NodeQuery {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Query(delegate func(results Result) error, finalCriteria ...Criteria) error {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Delete() error {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Update(properties *graph.Properties) error {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) OrderBy(criteria ...Criteria) NodeQuery {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Offset(skip int) NodeQuery {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Limit(skip int) NodeQuery {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Count() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) First() (*graph.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) Fetch(delegate func(cursor Cursor[*graph.Node]) error, finalCriteria ...Criteria) error {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) FetchIDs(delegate func(cursor Cursor[graph.ID]) error) error {
	//TODO implement me
	panic("implement me")
}

func (n nodeQuery) FetchKinds(f func(cursor Cursor[KindsResult]) error) error {
	//TODO implement me
	panic("implement me")
}
