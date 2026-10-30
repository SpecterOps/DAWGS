package frontend_test

import (
	"fmt"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"
)

func TestInQueryCallRejected(t *testing.T) {
	for _, invocation := range []string{
		"test.doNothing()", "test.write($value)", "test.write({value: 1})",
		"db.labels() YIELD label", "db.labels() YIELD label AS name WHERE name = 'x'",
		"test.read(reduce(x=0,v IN [1,2]|x+v)) YIELD value",
	} {
		for _, template := range []string{
			"CALL %s RETURN 1 AS x",
			"MATCH (n) CALL %s RETURN n",
			"WITH 1 AS x CALL %s RETURN x",
			"MATCH (n) CALL %s SET n.value = 1 RETURN n",
			"MATCH (n) CALL %s DETACH DELETE n",
			"CREATE (n:Created) WITH n CALL %s RETURN n",
			"MATCH (n) SET n.value = 1 WITH n CALL %s RETURN n",
			"MATCH (n) CALL %s WITH n RETURN n",
			"MATCH (n) call %s RETURN n;",
		} {
			source := fmt.Sprintf(template, invocation)
			t.Run(source, func(t *testing.T) { assertUnsupportedClause(t, source, "in-query CALL is not supported") })
		}
	}
}

func TestInQueryCallErrorLocation(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n)\n  CALL test.doNothing() RETURN n")
	require.EqualError(t, err, "line 2:2 in-query CALL is not supported")
}

func TestInQueryCallPreservesDefaultRestrictions(t *testing.T) {
	_, err := frontend.ParseCypher(frontend.DefaultCypherContext(), "MATCH (n) CALL test.read($value) YIELD value RETURN n")
	require.ErrorIs(t, err, frontend.ErrProcedureInvocationNotSupported)
	require.ErrorIs(t, err, frontend.ErrUserSpecifiedParametersNotSupported)
	require.ErrorContains(t, err, "in-query CALL is not supported")
}
