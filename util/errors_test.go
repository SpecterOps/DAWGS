package util_test

import (
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util"
	"github.com/stretchr/testify/require"
)

func TestIsNeoTimeoutError(t *testing.T) {
	neoTimeOutErr := neo4j.Neo4jError{
		Code: "Neo.ClientError.Transaction.TransactionTimedOut",
		Msg:  "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. The transaction has not completed within the specified timeout (dbms.transaction.timeout). You may want to retry with a longer timeout.",
	}
	require.True(t, util.IsNeoTimeoutError(&neoTimeOutErr))

	notTimeOutErr := neo4j.Neo4jError{
		Code: "This.Is.A.Test",
		Msg:  "Blah",
	}
	require.False(t, util.IsNeoTimeoutError(&notTimeOutErr))

	driverTimeOutQuery := "match (u1:User {domain: \"ESC6.LOCAL\"}), (u2:User {domain: \"ESC3.LOCAL\"}) where u1.samaccountname <> \"krbtgt\" and u1.samaccountname = u2.samaccountname with u2 match p1 = (u2)-[*1..]->(g:Group) with p1 match p2 = (u2)-[*1..]->(g:Group) return p1, p2"
	driverError := errors.New("Neo4jError: Neo.ClientError.Transaction.TransactionTimedOut (The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. The transaction has not completed within the specified timeout (dbms.transaction.timeout). You may want to retry with a longer timeout. )")

	driverTimeOutErr := graph.NewError(driverTimeOutQuery, driverError)
	require.True(t, util.IsNeoTimeoutError(driverTimeOutErr))

	notDriverTimeOutQuery := "match (u1:User {domain: \"ESC6.LOCAL\"}), (u2:User {domain: \"ESC3.LOCAL\"}) where u1.samaccountname <> \"krbtgt\" and u1.samaccountname = u2.samaccountname with u2 match p1 = (u2)-[*1..]->(g:Group) with p1 match p2 = (u2)-[*1..]->(g:Group) return p1, p2"
	notDriverError := errors.New("Some other error")

	notDriverTimeOutErr := graph.NewError(notDriverTimeOutQuery, notDriverError)

	require.False(t, util.IsNeoTimeoutError(notDriverTimeOutErr))
}
