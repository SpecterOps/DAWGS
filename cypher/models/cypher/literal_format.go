package cypher

import (
	"math"
	"strconv"
	"strings"
)

// FormatFloatLiteral renders value in its shortest round-trip decimal form
// while keeping a floating-point spelling. Values whose decimal form has no
// fractional part gain a trailing ".0", so the rendered text still parses as
// a double literal (RegularDecimalReal) instead of degrading to an integer
// literal. Non-finite values have no decimal literal spelling and keep their
// strconv rendering.
func FormatFloatLiteral(value float64) string {
	formatted := strconv.FormatFloat(value, 'f', -1, 64)

	if !strings.Contains(formatted, ".") && !math.IsInf(value, 0) && !math.IsNaN(value) {
		formatted += ".0"
	}

	return formatted
}
