package query

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	schemaIndexNamePattern = regexp.MustCompile(`(?i)\b(?:create\s+(?:unique\s+)?index|add\s+constraint)\s+(?:concurrently\s+)?(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_$]*)`)

	// Matches the suffix that pg.sqlSelectInvalidReindexArtifacts treats as an interrupted REINDEX CONCURRENTLY.
	reindexArtifactSuffixPattern = regexp.MustCompile(`_cc(new|old)[0-9]*$`)
)

// Storage optimization drops INVALID indexes whose names end in _ccnew[N] or _ccold[N]. Schema-owned index names must
// never use that suffix so they cannot be mistaken for interrupted REINDEX CONCURRENTLY artifacts.
func TestSchemaUpIndexNamesAvoidReindexArtifactSuffix(t *testing.T) {
	var names []string
	for _, match := range schemaIndexNamePattern.FindAllStringSubmatch(sqlSchemaUp, -1) {
		names = append(names, match[1])
	}

	require.Subset(t, names, []string{
		"node_kind_ids_index",
		"edge_start_id_kind_id_end_id_graph_id_key",
		"edge_inbound_traversal_index",
		"edge_kind_index",
	})

	for _, name := range names {
		require.NotRegexp(t, reindexArtifactSuffixPattern, name, "schema_up.sql index %q uses a reindex artifact suffix", name)
	}
}
