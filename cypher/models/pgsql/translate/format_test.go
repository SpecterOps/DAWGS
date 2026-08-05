package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

func TestFromCypherProperlyEscapesDebugComment(t *testing.T) {
	t.Parallel()

	kindMapper := pgutil.NewInMemoryKindMapper()

	query, err := frontend.ParseCypher(
		frontend.NewContext(),
		"MATCH (n) WHERE n.`begin\nfail1\rfail2\r\nfail3\n\rfail4` = 1 RETURN n",
	)
	require.NoError(t, err)

	formatted, err := FromCypher(context.Background(), query, kindMapper, false, DefaultGraphID)
	require.NoError(t, err)

	IsPGNewline := func(r rune) bool {
		return r == '\n' || r == '\r'
	}
	for line := range strings.FieldsFuncSeq(formatted.Statement, IsPGNewline) {
		if strings.HasPrefix(line, "with s0") {
			break
		} else if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		require.NotContains(t, line, "fail")
	}
}
