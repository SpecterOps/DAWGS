package v2

import (
	"strings"

	"github.com/specterops/dawgs/drivers/pg"
)

const translationCacheKeyVersion uint8 = 1

// translationKey identifies an immutable Cypher-to-SQL translation that is
// safe to reuse on one physical PostgreSQL connection.
type translationKey struct {
	version          uint8
	query            string
	graphID          int32
	parameterTypes   string
	policyIdentity   string
	schemaGeneration uint64
}

func newTranslationKey(query string, graphID int32, parameters map[string]any, policyIdentity string, schemaGeneration uint64) translationKey {
	return translationKey{
		version:          translationCacheKeyVersion,
		query:            strings.TrimSpace(query),
		graphID:          graphID,
		parameterTypes:   pg.TranslationParameterTypeKey(parameters),
		policyIdentity:   policyIdentity,
		schemaGeneration: schemaGeneration,
	}
}
