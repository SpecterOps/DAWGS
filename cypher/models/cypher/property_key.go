package cypher

import (
	"errors"
	"strings"
	"unicode"
)

// ErrEmptyPropertyKeyName reports that a property-key token decoded to an empty name.
var ErrEmptyPropertyKeyName = errors.New("property key name must not be empty")

// isCypherIDStart reports whether char may begin an unescaped Cypher identifier.
func isCypherIDStart(char rune) bool {
	return unicode.IsLetter(char) || unicode.In(char, unicode.Nl, unicode.Other_ID_Start)
}

// isCypherIDContinue reports whether char may follow the first rune of an unescaped Cypher identifier.
func isCypherIDContinue(char rune) bool {
	return isCypherIDStart(char) || unicode.In(char, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc, unicode.Other_ID_Continue)
}

// isCypherSymbolStart reports whether char may begin an unescaped symbolic name, including connector punctuation.
func isCypherSymbolStart(char rune) bool {
	return isCypherIDStart(char) || unicode.In(char, unicode.Pc)
}

// isCypherSymbolPart reports whether char may appear after the first rune of an unescaped symbolic name.
func isCypherSymbolPart(char rune) bool {
	return isCypherIDContinue(char) || unicode.In(char, unicode.Sc)
}

// CanEmitBarePropertyKeyName returns true when a raw property key can be emitted without backticks.
//
// This is specific to Cypher property-key position, such as n.name and {name: value}. Property keys use
// oC_PropertyKeyName -> oC_SchemaName, where reserved words are valid bare names, unlike variable or parameter
// symbols. Empty keys and keys containing characters outside the unescaped symbolic-name grammar return false; non-empty
// keys outside the bare grammar are still representable by EscapePropertyKeyName using backticks.
func CanEmitBarePropertyKeyName(name string) bool {
	if name == "" {
		return false
	}

	for idx, char := range name {
		if idx == 0 {
			if !isCypherSymbolStart(char) {
				return false
			}
		} else if !isCypherSymbolPart(char) {
			return false
		}
	}

	return true
}

// ValidatePropertyKeyName rejects empty decoded property-key names.
func ValidatePropertyKeyName(name string) error {
	if name == "" {
		return ErrEmptyPropertyKeyName
	}

	return nil
}

// EscapePropertyKeyName formats a raw property key as a Cypher property-key token.
func EscapePropertyKeyName(name string) string {
	if CanEmitBarePropertyKeyName(name) {
		return name
	}

	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// IsEscapedPropertyKeyName returns true when name is wrapped in Cypher backtick delimiters.
func IsEscapedPropertyKeyName(name string) bool {
	return len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`'
}

// UnescapePropertyKeyName decodes a Cypher property-key token into the raw property key it names.
func UnescapePropertyKeyName(name string) string {
	if !IsEscapedPropertyKeyName(name) {
		return name
	}

	return strings.ReplaceAll(name[1:len(name)-1], "``", "`")
}
