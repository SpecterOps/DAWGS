package pg

import "github.com/specterops/dawgs/graph"

func IsPostgreSQLGraph(db graph.Database) bool {
	return graph.IsDriver[*Driver](db)
}
