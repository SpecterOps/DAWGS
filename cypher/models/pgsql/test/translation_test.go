package test

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
)

func translationTestKinds() graph.Kinds {
	// Keep this order stable. Translation case SQL fixtures depend on these IDs.
	return graph.Kinds{
		NodeKind1,
		NodeKind2,
		EdgeKind1,
		EdgeKind2,
		graph.StringKind("Computer"),
		graph.StringKind("User"),
		graph.StringKind("HasSession"),
		graph.StringKind("GPO"),
		graph.StringKind("OU"),
		graph.StringKind("Base"),
		graph.StringKind("GPLink"),
		graph.StringKind("Contains"),
		graph.StringKind("Group"),
	}.Add(graph.StringsToKinds([]string{
		"AddAllowedToAct",
		"AddMember",
		"AdminTo",
		"AllExtendedRights",
		"AllowedToDelegate",
		"CanRDP",
		"ForceChangePassword",
		"GenericAll",
		"GenericWrite",
		"GetChangesAll",
		"GetChanges",
		"MemberOf",
		"Owns",
		"ReadLAPSPassword",
		"SQLAdmin",
		"TrustedBy",
		"WriteAccountRestrictions",
		"WriteOwner",
		"AZUser",
		// Synthetic reconciliation kinds are append-only. The first 9 and all 30
		// are used by cardinality-sensitive golden cases without renumbering any
		// established kind IDs above.
		"RegressionKind01",
		"RegressionKind02",
		"RegressionKind03",
		"RegressionKind04",
		"RegressionKind05",
		"RegressionKind06",
		"RegressionKind07",
		"RegressionKind08",
		"RegressionKind09",
		"RegressionKind10",
		"RegressionKind11",
		"RegressionKind12",
		"RegressionKind13",
		"RegressionKind14",
		"RegressionKind15",
		"RegressionKind16",
		"RegressionKind17",
		"RegressionKind18",
		"RegressionKind19",
		"RegressionKind20",
		"RegressionKind21",
		"RegressionKind22",
		"RegressionKind23",
		"RegressionKind24",
		"RegressionKind25",
		"RegressionKind26",
		"RegressionKind27",
		"RegressionKind28",
		"RegressionKind29",
		"RegressionKind30",
		"RegressionKind31",
		"RegressionKind32",
		"RegressionKind33",
		"RegressionKind34",
		"RegressionKind35",
		"RegressionKind36",
		"RegressionKind37",
		"RegressionKind38",
		"RegressionKind39",
	})...)
}

func newKindMapper() pgsql.KindMapper {
	mapper := pgutil.NewInMemoryKindMapper()

	for _, kind := range translationTestKinds() {
		mapper.Put(kind)
	}

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
