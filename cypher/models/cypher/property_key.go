package cypher

import (
	"errors"
	"strings"
	"unicode"
)

var ErrEmptyPropertyKeyName = errors.New("property key name must not be empty")

func isCypherIDStart(char rune) bool {
	return unicode.IsLetter(char) || unicode.In(char, unicode.Nl, unicode.Other_ID_Start)
}

func isCypherIDContinue(char rune) bool {
	return isCypherIDStart(char) || unicode.In(char, unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc, unicode.Other_ID_Continue)
}

func isCypherSymbolStart(char rune) bool {
	return isCypherIDStart(char) || unicode.In(char, unicode.Pc)
}

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
