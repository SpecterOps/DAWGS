package test

import (
	"context"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
	"github.com/stretchr/testify/require"
)

const (
	PGConnectionStringEV = "PG_CONNECTION_STRING"
)

func TestTranslationTestCases(t *testing.T) {
	var (
		testCtx, done = context.WithCancel(context.Background())
		// pgConnectionStr = os.Getenv(PGConnectionStringEV)
		pgConnectionStr = "user=bhe password=weneedbetterpasswords dbname=bhe host=localhost port=55432"
	)

	defer done()

	require.NotEmpty(t, pgConnectionStr)

	if pgxPool, err := pgxpool.New(testCtx, pgConnectionStr); err != nil {
		t.Fatalf("Failed opening database connection: %v", err)
	} else if connection, err := dawgs.Open(context.TODO(), pg.DriverName, dawgs.Config{
		GraphQueryMemoryLimit: size.Gibibyte,
		Pool:                  pgxPool,
	}); err != nil {
		t.Fatalf("Failed opening database connection: %v", err)
	} else if pgConnection, typeOK := connection.(*pg.Driver); !typeOK {
		t.Fatalf("Invalid connection type: %T", connection)
	} else {
		defer connection.Close(testCtx)

		graphSchema := graph.Schema{
			Graphs: []graph.Graph{{
				Name: "test",
				Nodes: graph.Kinds{
					graph.StringKind("NodeKind1"),
					graph.StringKind("NodeKind2"),
				},
				Edges: graph.Kinds{
					graph.StringKind("EdgeKind1"),
					graph.StringKind("EdgeKind2"),
				},
			}},
			DefaultGraph: graph.Graph{
				Name: "test",
			},
		}

		if err := connection.AssertSchema(testCtx, graphSchema); err != nil {
			t.Fatalf("Failed asserting graph schema: %v", err)
		}

		casesRun := 0

		if testCases, err := ReadTranslationTestCases(); err != nil {
			t.Fatal(err)
		} else {
			for _, testCase := range testCases {
				t.Run(testCase.Name, func(t *testing.T) {
					defer func() {
						if err := recover(); err != nil {
							debug.PrintStack()
							t.Error(err)
						}
					}()

					testCase.AssertLive(testCtx, t, pgConnection)
				})

				casesRun += 1
			}
		}

		fmt.Printf("Validated %d test cases\n", casesRun)
	}
}
