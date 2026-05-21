package commands

import (
	"strings"

	cypherFrontend "github.com/specterops/dawgs/cypher/frontend"
	cypherModels "github.com/specterops/dawgs/cypher/models/cypher"
)

func parseQueryArray(fields []string) (*cypherModels.RegularQuery, error) {
	cypherCtx := cypherFrontend.DefaultCypherContext()
	return cypherFrontend.ParseCypher(cypherCtx, strings.Join(fields, " "))
}
