package format_test

import (
	"bytes"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher/format"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/stretchr/testify/require"

	"github.com/specterops/dawgs/cypher/test"
)

func TestCypherEmitter_StripLiterals(t *testing.T) {
	var (
		buffer            = &bytes.Buffer{}
		regularQuery, err = frontend.ParseCypher(frontend.DefaultCypherContext(), "match (n {value: 'PII'}) where n.other = 'more pii' and n.number = 411 return n.name, n")
		emitter           = format.Emitter{
			StripLiterals: true,
		}
	)

	require.Nil(t, err)
	require.Nil(t, emitter.Write(regularQuery, buffer))
	require.Equal(t, "match (n {value: $STRIPPED}) where n.other = $STRIPPED and n.number = $STRIPPED return n.name, n", buffer.String())
}

func TestCypherEmitter_HappyPath(t *testing.T) {
	test.LoadFixture(t, test.MutationTestCases).Run(t)
	test.LoadFixture(t, test.PositiveTestCases).Run(t)
}

func TestCypherEmitter_NegativeCases(t *testing.T) {
	test.LoadFixture(t, test.NegativeTestCases).Run(t)
}
