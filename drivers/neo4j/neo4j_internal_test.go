package neo4j

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNeo4jConnectionTargetPreservesAcceptedSchemes(t *testing.T) {
	testCases := []struct {
		name             string
		connStr          string
		expectedTarget   string
		expectedDatabase string
	}{{
		name:             "plain routing",
		connStr:          "neo4j://neo4j:password@localhost:7687",
		expectedTarget:   "neo4j://localhost:7687",
		expectedDatabase: "",
	}, {
		name:             "secure routing",
		connStr:          "neo4j+s://neo4j:password@cluster.example:7687",
		expectedTarget:   "neo4j+s://cluster.example:7687",
		expectedDatabase: "",
	}, {
		name:             "self signed routing with database and query",
		connStr:          "neo4j+ssc://neo4j:password@cluster.example:7687/analytics?policy=fast",
		expectedTarget:   "neo4j+ssc://cluster.example:7687?policy=fast",
		expectedDatabase: "analytics",
	}}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			connectionURL, err := url.Parse(testCase.connStr)
			require.NoError(t, err)
			require.True(t, isNeo4jConnectionScheme(connectionURL.Scheme))

			target, err := neo4jConnectionTarget(connectionURL)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedTarget, target)

			databaseName, err := neo4jConnectionDatabaseName(connectionURL)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedDatabase, databaseName)
		})
	}
}

func TestNeo4jConnectionDatabaseNameRejectsNestedPath(t *testing.T) {
	for _, connStr := range []string{
		"neo4j://neo4j:password@localhost:7687/db/extra",
		"neo4j://neo4j:password@localhost:7687/db%2Fextra",
	} {
		connectionURL, err := url.Parse(connStr)
		require.NoError(t, err)

		_, err = neo4jConnectionDatabaseName(connectionURL)
		require.ErrorContains(t, err, "single database name")
	}
}
