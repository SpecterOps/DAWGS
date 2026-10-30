package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/test"
	"github.com/stretchr/testify/require"
)

func TestCreateUniqueRejected(t *testing.T) {
	for _, pattern := range []string{
		"(a)-[:R]->(b)", "(a)<-[:R]-(b)", "(a)-[:R]-(b)",
		"(a)-[:R {value: $value}]->(b)", "(:Created {value: 1})",
	} {
		for _, template := range []string{
			"MATCH (a), (b) CREATE UNIQUE %s",
			"MATCH (a), (b) CREATE UNIQUE %s RETURN a",
			"MATCH (a), (b) SET a.value = 1 CREATE UNIQUE %s",
			"MATCH (a), (b) WITH a, b CREATE UNIQUE %s RETURN b",
			"CREATE UNIQUE %s",
			"/* before */\ncreate unique %s;",
		} {
			source := fmt.Sprintf(template, pattern)
			t.Run(source, func(t *testing.T) {
				assertUnsupportedClause(t, source, "CREATE UNIQUE is not supported")
			})
		}
	}
}

func TestLoadCSVRejected(t *testing.T) {
	for _, clause := range []string{
		"LOAD CSV FROM 'file:///missing.csv' AS row",
		"LOAD CSV WITH HEADERS FROM 'file:///missing.csv' AS row",
		"LOAD CSV FROM $url AS row",
		"LOAD CSV WITH HEADERS FROM $url AS row FIELDTERMINATOR ';'",
		"LOAD CSV FROM reduce(url='',chunk IN ['file:', '///missing.csv']|url + chunk) AS row",
		"load csv from 'file:///missing.csv' as row",
	} {
		for _, template := range []string{
			"%s RETURN row",
			"%s MATCH (n) RETURN count(*) AS c",
			"%s WITH row MATCH (n) RETURN n",
			"%s CREATE (:Created {value: 1})",
			"%s MATCH (n) SET n.value = 1 RETURN n",
			"%s MATCH (n) DETACH DELETE n",
			"MATCH (n) WITH n %s SET n.value = 1",
			"CREATE (:Created) WITH 1 AS x %s RETURN x",
		} {
			source := fmt.Sprintf(template, clause)
			t.Run(source, func(t *testing.T) {
				assertUnsupportedClause(t, source, "LOAD CSV is not supported")
			})
		}
	}
}

func assertUnsupportedClause(t *testing.T, source, message string) {
	t.Helper()
	for _, ctx := range []*frontend.Context{frontend.NewContext(), frontend.DefaultCypherContext()} {
		_, err := frontend.ParseCypher(ctx, source)
		require.ErrorContains(t, err, message)
	}
	formatted, err := test.CypherToCypher(frontend.NewContext(), source)
	require.ErrorContains(t, err, message)
	require.Empty(t, formatted)
}

func TestUnsupportedImportAndCreateErrorLocations(t *testing.T) {
	for _, tc := range []struct{ source, message string }{
		{"MATCH (a), (b)\n  CREATE UNIQUE (a)-[:R]->(b)", "line 2:2 CREATE UNIQUE is not supported"},
		{"// comment\n  LOAD CSV FROM $url AS row RETURN row", "line 2:2 LOAD CSV is not supported"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			_, err := frontend.ParseCypher(frontend.NewContext(), tc.source)
			require.EqualError(t, err, tc.message)
		})
	}
}

func TestUnsupportedImportAndCreatePreserveSupportedClauses(t *testing.T) {
	for _, source := range []string{
		"create (n:Created {value: 1}) return n",
		"match (a), (b) create (a)-[:R]->(b)",
		"merge (n:Created {value: 1}) return n",
		"unwind [1, 2] as row create (:Created {value: row})",
	} {
		t.Run(source, func(t *testing.T) {
			formatted, err := test.CypherToCypher(frontend.NewContext(), source)
			require.NoError(t, err)
			require.Equal(t, source, formatted)
		})
	}
}
