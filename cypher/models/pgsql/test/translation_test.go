package test

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/pgutil"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

var (
	NodeKind1 = graph.StringKind("NodeKind1")
	NodeKind2 = graph.StringKind("NodeKind2")
	EdgeKind1 = graph.StringKind("EdgeKind1")
	EdgeKind2 = graph.StringKind("EdgeKind2")
	NodeBase  = graph.StringKind("Base")
)

func newKindMapper() pgsql.KindMapper {
	mapper := pgutil.NewInMemoryKindMapper()

	// This is here to make SQL output a little more predictable for test cases
	mapper.Put(NodeKind1)
	mapper.Put(NodeKind2)
	mapper.Put(EdgeKind1)
	mapper.Put(EdgeKind2)
	mapper.Put(NodeBase)

	return mapper
}

func TestTranslate(t *testing.T) {
	var (
		casesRun   = 0
		kindMapper = newKindMapper()
	)

	if updateCases, varSet := os.LookupEnv("CYSQL_UPDATE_CASES"); varSet && strings.ToLower(strings.TrimSpace(updateCases)) == "true" {
		if err := UpdateTranslationTestCases(kindMapper); err != nil {
			fmt.Printf("Error updating cases: %v\n", err)
		}
	}

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

				testCase.Assert(t, testCase.PgSQL, kindMapper)
			})

			casesRun += 1
		}
	}

	fmt.Printf("Ran %d test cases\n", casesRun)
}
