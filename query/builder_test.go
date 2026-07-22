package query_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	cypherFormat "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func TestBuilderProjectionModifiersAreOrderIndependent(t *testing.T) {
	testCases := map[string][]graph.Criteria{
		"before return": {
			query.OrderBy(query.NodeID()),
			query.Limit(7),
			query.Offset(3),
			query.Returning(query.Node()),
		},
		"after return": {
			query.Returning(query.Node()),
			query.Offset(3),
			query.Limit(7),
			query.OrderBy(query.NodeID()),
		},
		"criteria slice": {
			[]graph.Criteria{
				query.OrderBy(query.NodeID()),
				query.Limit(7),
				query.Offset(3),
			},
			query.Returning(query.Node()),
		},
		"last modifier and replacement return win": {
			query.Returning(query.NodeID()),
			query.OrderBy(query.NodeProperty("name")),
			query.OrderBy(query.NodeID()),
			query.Limit(2),
			query.Limit(7),
			query.Offset(1),
			query.Offset(3),
			query.Returning(query.Node()),
		},
	}

	for name, criteria := range testCases {
		t.Run(name, func(t *testing.T) {
			builder := query.NewBuilder(nil)
			builder.Apply(criteria...)

			regularQuery, err := builder.Build(false)
			if err != nil {
				t.Fatalf("build query: %v", err)
			}

			var cypher bytes.Buffer
			if err := cypherFormat.NewCypherEmitter(false).Write(regularQuery, &cypher); err != nil {
				t.Fatalf("render Cypher: %v", err)
			}

			assertRetrieverProjection(t, cypher.String())

			translated, err := translate.FromCypher(context.Background(), regularQuery, nil, false, 1)
			if err != nil {
				t.Fatalf("translate PostgreSQL query: %v", err)
			}

			sql := strings.ToLower(translated.Statement)
			for _, expected := range []string{"order by", "offset 3", "limit 7"} {
				if !strings.Contains(sql, expected) {
					t.Fatalf("PostgreSQL query missing %q:\n%s", expected, translated.Statement)
				}
			}
		})
	}
}

func assertRetrieverProjection(t *testing.T, rendered string) {
	t.Helper()

	normalized := strings.ToLower(rendered)
	for _, expected := range []string{"return n", "order by id(n) asc", "skip 3", "limit 7"} {
		if !strings.Contains(normalized, expected) {
			t.Fatalf("query missing %q: %s", expected, rendered)
		}
	}
}
