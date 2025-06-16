package translate

import (
	"bytes"
	"context"

	"github.com/specterops/dawgs/cypher/models/cypher"
	cypherFormat "github.com/specterops/dawgs/cypher/models/cypher/format"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
)

func Translated(translation Result) (string, error) {
	return format.Statement(translation.Statement, format.NewOutputBuilder())
}

func FromCypher(ctx context.Context, regularQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, stripLiterals bool) (format.Formatted, error) {
	var (
		output  = &bytes.Buffer{}
		emitter = cypherFormat.NewCypherEmitter(stripLiterals)
	)

	output.WriteString("-- ")

	if err := emitter.Write(regularQuery, output); err != nil {
		return format.Formatted{}, err
	}

	output.WriteString("\n")

	if translation, err := Translate(ctx, regularQuery, kindMapper, nil); err != nil {
		return format.Formatted{}, err
	} else if sqlQuery, err := format.Statement(translation.Statement, format.NewOutputBuilder()); err != nil {
		return format.Formatted{}, err
	} else {
		output.WriteString(sqlQuery)

		return format.Formatted{
			Statement:  output.String(),
			Parameters: translation.Parameters,
		}, nil
	}
}
