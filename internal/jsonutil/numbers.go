// Package jsonutil provides shared JSON import conversions.
package jsonutil

import (
	"encoding/json"
	"fmt"
	"strings"
)

// NormalizeNumbers replaces json.Number values in a decoded JSON value with
// driver-compatible numbers. Integer tokens must fit int64; decimal and exponent
// tokens use float64. Maps and slices are updated in place and may be partially
// normalized on error. Callers must discard the decoded record on error.
func NormalizeNumbers(value any) (any, error) {
	switch typed := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(typed), ".eE") {
			integer, err := typed.Int64()
			if err != nil {
				return nil, fmt.Errorf("JSON integer outside signed 64-bit range: %w", err)
			}
			return integer, nil
		}
		decimal, err := typed.Float64()
		if err != nil {
			return nil, fmt.Errorf("JSON number outside float64 range: %w", err)
		}
		return decimal, nil
	case map[string]any:
		for key, item := range typed {
			normalized, err := NormalizeNumbers(item)
			if err != nil {
				return nil, fmt.Errorf("property %q: %w", key, err)
			}
			typed[key] = normalized
		}
	case []any:
		for index, item := range typed {
			normalized, err := NormalizeNumbers(item)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", index, err)
			}
			typed[index] = normalized
		}
	}
	return value, nil
}
