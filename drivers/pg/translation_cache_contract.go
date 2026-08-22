package pg

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	model "github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
)

// CypherTranslationCache implements reusable Cypher-to-SQL translation.
// Implementations must not retain caller parameter values or mutable maps.
type CypherTranslationCache interface {
	TranslateWithPolicy(
		query string,
		graphID int32,
		parameters map[string]any,
		policyIdentity string,
		build func() (translate.Result, string, error),
	) (string, map[string]any, error)
}

// CypherTranslationCacheProvider selects cache ownership for a physical
// connection. Returning nil deliberately bypasses translation retention.
type CypherTranslationCacheProvider interface {
	CacheForConnection(conn *pgx.Conn) CypherTranslationCache
}

// TranslationParameterTypeKey encodes sorted parameter names and negotiated
// PostgreSQL data types into an unambiguous cache-key component.
func TranslationParameterTypeKey(parameters map[string]any) string {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var key strings.Builder
	for _, name := range keys {
		value := parameters[name]
		var typeName string
		if value == nil {
			typeName = "null"
		} else if dataType, err := model.ValueToDataType(value); err == nil {
			typeName = dataType.String()
		} else {
			typeName = fmt.Sprintf("invalid:%T", value)
		}
		key.WriteString(strconv.Itoa(len(name)))
		key.WriteByte(':')
		key.WriteString(name)
		key.WriteString(strconv.Itoa(len(typeName)))
		key.WriteByte(':')
		key.WriteString(typeName)
	}
	return key.String()
}

func translationParameterTypeKey(parameters map[string]any) string {
	return TranslationParameterTypeKey(parameters)
}

func cacheableTranslation(result translate.Result, parameters map[string]any) bool {
	if len(result.Parameters) != len(result.ParameterSources) {
		return false
	}
	for identifier := range result.Parameters {
		source, found := result.ParameterSources[identifier]
		if !found || source == "" {
			return false
		}
		if _, found := parameters[source]; !found {
			return false
		}
	}
	return true
}
