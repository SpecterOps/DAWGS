package walk_test

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLMergeWalker(t *testing.T) {
	for _, subquery := range []bool{false, true} {
		merge := pgsql.Merge{Table: pgsql.TableReference{Name: pgsql.Identifier("target").AsCompoundIdentifier()}, Source: pgsql.TableReference{Name: pgsql.Identifier("source").AsCompoundIdentifier()}, JoinTarget: pgsql.NewLiteral(true, pgsql.Boolean), Actions: []pgsql.MergeAction{pgsql.MergeDoNothing{Matched: true, Predicate: pgsql.Identifier("nothing_predicate")}, pgsql.MatchedUpdate{Predicate: pgsql.Identifier("update_predicate"), Assignments: []pgsql.Assignment{pgsql.NewBinaryExpression(pgsql.Identifier("prop"), pgsql.OperatorAssignment, pgsql.Identifier("assigned_value"))}}, pgsql.MatchedDelete{Predicate: pgsql.Identifier("delete_predicate")}, pgsql.UnmatchedAction{Predicate: pgsql.Identifier("insert_predicate"), Columns: []pgsql.Identifier{"prop"}, Values: pgsql.Values{Values: []pgsql.Expression{pgsql.Identifier("inserted_value")}}}}, Returning: pgsql.Projection{pgsql.Identifier("returned_value")}}
		if subquery {
			merge.SourceQuery = &pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{pgsql.Identifier("source_value")}}}}
		}
		seen := map[pgsql.Identifier]bool{}
		require.NoError(t, walk.PgSQL(pgsql.Query{Body: merge}, walk.NewSimpleVisitor(func(node pgsql.SyntaxNode, _ walk.VisitorHandler) {
			if id, ok := node.(pgsql.Identifier); ok {
				seen[id] = true
			}
			if id, ok := node.(pgsql.CompoundIdentifier); ok && len(id) > 0 {
				seen[id[0]] = true
			}
		})))
		for _, id := range []pgsql.Identifier{"nothing_predicate", "update_predicate", "assigned_value", "delete_predicate", "insert_predicate", "inserted_value", "returned_value"} {
			require.True(t, seen[id], "missing %s", id)
		}
		if subquery {
			require.True(t, seen["source_value"])
		} else {
			require.True(t, seen["source"])
		}
	}
}
