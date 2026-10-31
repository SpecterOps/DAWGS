//go:build manual_integration

package integration

import (
	"maps"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// A forced diagnostic must either decline unsupported correlation or satisfy
// the same exact path assertions as the incumbent.
func TestPostgreSQLCorrelatedSuffixGuardAdmission(t *testing.T) {
	var corpus cypherTemplateFile
	for _, file := range loadCypherTemplateFiles(t) {
		if strings.HasSuffix(file.path, "/correlated_suffix_shapes.json") {
			corpus = file
			break
		}
	}
	require.NotEmpty(t, corpus.Families)
	nodeKinds, edgeKinds := cypherTemplateKinds([]cypherTemplateFile{corpus})
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfNoConnection: true, SkipIfDriverMismatch: true, CleanupMode: CleanupGraph, ExtraNodeKinds: nodeKinds, ExtraEdgeKinds: edgeKinds})
	driver := session.DB.(*pg.Driver)
	defaultGraph, ok := driver.DefaultGraph()
	require.True(t, ok)
	for _, family := range corpus.Families {
		for _, variant := range family.Variants {
			t.Run(variant.Name, func(t *testing.T) {
				query, err := frontend.ParseCypher(frontend.NewContext(), renderCypherTemplate(t, family.Template, variant.Vars))
				require.NoError(t, err)
				translation, err := translate.TranslateForTool(session.Ctx, query, driver.KindMapper(), nil, defaultGraph.ID, translate.ToolOptions{EnableExpansionSuffixReverseGuard: true})
				if err != nil {
					require.ErrorContains(t, err, "statically eligible full-path fixed-suffix target")
					return
				}
				formatted, err := translate.Translated(translation)
				require.NoError(t, err)
				maps.Copy(translation.Parameters, formatted.Parameters)
				require.NoError(t, session.WithRollbackFixture(t, family.Fixture, false, func(tx graph.Transaction, ids opengraph.IDMap) error {
					result := tx.Raw(formatted.Statement, translation.Parameters)
					defer result.Close()
					parseAssertion(t, variant.Assert).checkResult(t, result, newAssertionContext(ids))
					return nil
				}))
			})
		}
	}
}
