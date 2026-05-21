package commands

import (
	"strings"

	cypherFrontend "github.com/specterops/dawgs/cypher/frontend"
	cypherModels "github.com/specterops/dawgs/cypher/models/cypher"
)

func parseQueryArray(fields []string) (*cypherModels.RegularQuery, error) {
	return ParseQueryText(strings.Join(fields, " "))
}

// ParseQueryText parses a Cypher query string using dawgrun's default parser settings.
func ParseQueryText(query string) (*cypherModels.RegularQuery, error) {
	cypherCtx := cypherFrontend.DefaultCypherContext()
	return cypherFrontend.ParseCypher(cypherCtx, query)
}
