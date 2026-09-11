package pg

import "strings"

// translationCacheKeyVersion partitions retained translations by key schema.
const translationCacheKeyVersion uint8 = 1

// translationKey identifies an immutable Cypher-to-SQL translation that is
// safe to reuse on one physical PostgreSQL connection.
type translationKey struct {
	// version identifies the key schema used to construct this value.
	version uint8

	// query is normalized Cypher source without caller-owned backing storage.
	query string

	// graphID scopes generated SQL to one graph partition.
	graphID int32

	// parameterTypes partitions translations by negotiated parameter types.
	parameterTypes string

	// policyIdentity partitions translations by effective traversal policy.
	policyIdentity string

	// schemaGeneration prevents reuse after schema-sensitive changes.
	schemaGeneration uint64
}

// newTranslationKey derives the complete immutable cache identity for one translation.
func newTranslationKey(query string, graphID int32, parameters map[string]any, policyIdentity string, schemaGeneration uint64) translationKey {
	return translationKey{
		version:          translationCacheKeyVersion,
		query:            strings.TrimSpace(query),
		graphID:          graphID,
		parameterTypes:   TranslationParameterTypeKey(parameters),
		policyIdentity:   policyIdentity,
		schemaGeneration: schemaGeneration,
	}
}
