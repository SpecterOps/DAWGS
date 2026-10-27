package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/specterops/dawgs/drivers/neo4j"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

func TestDriverFromConnectionString(t *testing.T) {
	for _, tc := range []struct {
		uri, driver, message string
	}{
		{"postgres://localhost/dawgs", pg.DriverName, ""},
		{"postgresql://localhost/dawgs", pg.DriverName, ""},
		{"neo4j://localhost", neo4j.DriverName, ""},
		{"neo4j+s://localhost", neo4j.DriverName, ""},
		{"neo4j+ssc://localhost", neo4j.DriverName, ""},
		{"unsupported://localhost", "", "unknown connection string scheme"},
		{"://invalid", "", "failed to parse connection string"},
	} {
		t.Run(tc.uri, func(t *testing.T) {
			driver, err := DriverFromConnectionString(tc.uri)
			if tc.message != "" {
				require.ErrorContains(t, err, tc.message)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.driver, driver)
		})
	}
}

// TestOpenSkipsUnselectedBackend ensures optional sessions return before any
// database construction, including when a different backend URI is exported.
func TestOpenSkipsUnselectedBackend(t *testing.T) {
	for _, tc := range []struct {
		name, uri, env string
	}{
		{"missing connection", "", ConnectionStringEnv},
		{"different backend", "postgres://unused@127.0.0.1:1/dawgs", "DAWGS_HARNESS_TEST_URI"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.uri)
			skipped, returned := false, false
			t.Run("session", func(t *testing.T) {
				defer func() { skipped = t.Skipped() }()
				Open(t, Options{
					RequireDriver:          neo4j.DriverName,
					ConnectionStringEnvVar: tc.env,
					SkipIfNoConnection:     true,
					SkipIfDriverMismatch:   true,
				})
				returned = true
			})
			require.True(t, skipped)
			require.False(t, returned)
		})
	}
}

func TestFixtureSchemaCombinesKindsWithoutDatabase(t *testing.T) {
	fixturePath := filepath.Join(t.TempDir(), "fixture.json")
	require.NoError(t, os.WriteFile(fixturePath, []byte(`{"graph": {
		"nodes": [
			{"id": "a", "kinds": ["First", "Shared"]},
			{"id": "b", "kinds": ["Shared"]}
		],
		"edges": [{"start_id": "a", "end_id": "b", "kind": "Link"}]
	}}`), 0o600))
	for _, name := range []string{"", "custom_graph"} {
		opts := Options{
			GraphName:      name,
			Datasets:       []string{"fixture"},
			ExtraNodeKinds: graph.Kinds{graph.StringKind("Shared"), graph.StringKind("Extra")},
			ExtraEdgeKinds: graph.Kinds{graph.StringKind("Link"), graph.StringKind("ExtraLink")},
			DatasetPath: func(dataset string) string {
				require.Equal(t, "fixture", dataset)
				return fixturePath
			},
		}
		schema := buildSchema(t, opts)
		require.NotNil(t, schema)
		if name == "" {
			name = "integration_test"
		}
		require.Equal(t, name, schema.DefaultGraph.Name)
		require.Len(t, schema.Graphs, 1)
		require.Equal(t, name, schema.Graphs[0].Name)
		require.ElementsMatch(t, []string{"First", "Shared", "Extra"}, schema.Graphs[0].Nodes.Strings())
		require.ElementsMatch(t, []string{"Link", "ExtraLink"}, schema.Graphs[0].Edges.Strings())
	}
	require.Nil(t, buildSchema(t, Options{}))
	defaults := Options{}
	require.Equal(t, "testdata/base.json", defaults.datasetPath()("base"))
	require.Equal(t, size.Gibibyte, defaults.graphQueryMemoryLimit())
	require.Equal(t, size.Mebibyte, (Options{GraphQueryMemoryLimit: size.Mebibyte}).graphQueryMemoryLimit())
}

type rollbackHarnessDatabase struct {
	graph.Database
	transaction graph.Transaction
	err         error
	delegateErr error
}

func (s *rollbackHarnessDatabase) WriteTransaction(_ context.Context, delegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	if s.err != nil {
		return s.err
	}
	s.delegateErr = delegate(s.transaction)
	return s.delegateErr
}

func TestSessionRollbackPreservesFailures(t *testing.T) {
	failure := errors.New("fixture failed")
	for _, tc := range []struct {
		name                   string
		delegateErr, driverErr error
	}{
		{name: "successful fixture requests rollback"},
		{name: "delegate failure", delegateErr: failure},
		{name: "driver failure", driverErr: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &struct{ graph.Transaction }{}
			db := &rollbackHarnessDatabase{transaction: tx, err: tc.driverErr}
			session := &Session{DB: db, Ctx: context.Background()}
			called := false
			err := session.WithRollback(t, func(actual graph.Transaction) error {
				called = true
				require.Same(t, tx, actual)
				return tc.delegateErr
			})
			require.Equal(t, tc.driverErr == nil, called)
			if tc.delegateErr != nil || tc.driverErr != nil {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
				require.ErrorIs(t, db.delegateErr, errFixtureRollback)
			}
		})
	}
}
