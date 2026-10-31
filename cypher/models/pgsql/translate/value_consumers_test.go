package translate

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/stretchr/testify/require"
)

func TestWithPropertyConsumersTranslation(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		t.Run(string(mode), func(t *testing.T) {
			for _, expression := range []string{"toString(x)", "toLower(x)", "toUpper(x)", "split(x, ',')", "split('a,b', x)", "size(x)", "head(x)", "tail(x)", "head(coalesce(x, [1]))", "toInteger(x)", "coalesce(x, 'fallback')", "coalesce(x, 7)", "coalesce(x, [])", "x = 'Alice'", "x <> 'Alice'", "x IS NULL", "date(x)"} {
				t.Run(expression, func(t *testing.T) {
					parsed, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n) WITH n.x AS x RETURN "+expression+" AS value")
					require.NoError(t, err)
					translated, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
					require.NoError(t, err)
					_, err = Translated(translated)
					require.NoError(t, err)
				})
			}
		})
	}
}

func TestValueConsumerErrors(t *testing.T) {
	translator := &Translator{scope: NewScope()}
	invalid := pgsql.CompoundIdentifier{"invalid", "three", "parts"}
	_, _, err := translator.functionJSONArgument(invalid)
	require.Error(t, err)
	_, err = translator.functionTextArgument(invalid, "toString")
	require.Error(t, err)
	_, err = translator.scalarCastArgument(invalid)
	require.Error(t, err)
	_, err = translator.booleanArgument(invalid)
	require.Error(t, err)
	_, err = translator.functionTextArgument(pgsql.ArrayLiteral{CastType: pgsql.Int8Array}, "toString")
	require.ErrorContains(t, err, "toString does not support a list")
}

func TestWithPropertyConsumerAliasRebinding(t *testing.T) {
	for _, mode := range []OptimizerMode{OptimizerEnabled, OptimizerDisabled} {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), "MATCH (n) WITH n.name AS x WITH toString(x) AS x RETURN toString(x) AS value")
		require.NoError(t, err)
		result, err := TranslateWithOptions(context.Background(), parsed, optimizerSafetyKindMapper(), nil, DefaultGraphID, Options{OptimizerMode: mode})
		require.NoError(t, err)
		formatted, err := Translated(result)
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(formatted.Statement, "cypher_text("), "the converted alias must have a text type")
	}
}
