// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package testutil provides reusable corpus, fixture, and baseline helpers for
// DAWGS tests and diagnostic commands.
package testutil

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	typeKey    = "$type"
	valueKey   = "value"
	prefixKey  = "prefix"
	countKey   = "count"
	includeKey = "include"
)

// Params is a query parameter map that supports tagged generated and temporal
// values. A datetime is represented in JSON as:
//
//	{"$type": "datetime", "value": "2026-01-02T03:04:05Z"}
//
// A deterministic string list is represented without committing a large
// handwritten array as:
//
//	{"$type": "string_list", "prefix": "missing", "count": 1000, "include": ["target-id"]}
//
// Tagged values may also appear in nested maps and lists.
type Params map[string]any

func (s *Params) UnmarshalJSON(raw []byte) error {
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}

	converted, err := convertMap(decoded)
	if err != nil {
		return err
	}

	*s = Params(converted)
	return nil
}

func convertMap(values map[string]any) (map[string]any, error) {
	converted := make(map[string]any, len(values))
	for key, value := range values {
		typedValue, err := convertValue(value)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", key, err)
		}

		converted[key] = typedValue
	}

	return converted, nil
}

func convertValue(value any) (any, error) {
	switch typedValue := value.(type) {
	case map[string]any:
		if typeName, tagged := typedValue[typeKey]; tagged {
			return convertTaggedValue(typeName, typedValue)
		}

		return convertMap(typedValue)

	case []any:
		converted := make([]any, len(typedValue))
		for idx, item := range typedValue {
			next, err := convertValue(item)
			if err != nil {
				return nil, fmt.Errorf("list item %d: %w", idx, err)
			}
			converted[idx] = next
		}

		return converted, nil

	default:
		return value, nil
	}
}

func convertTaggedValue(rawType any, tagged map[string]any) (any, error) {
	typeName, ok := rawType.(string)
	if !ok {
		return nil, fmt.Errorf("%s must be a string", typeKey)
	}

	switch typeName {
	case "datetime":
		rawValue, found := tagged[valueKey]
		if !found {
			return nil, fmt.Errorf("datetime is missing %q", valueKey)
		}

		value, ok := rawValue.(string)
		if !ok {
			return nil, fmt.Errorf("datetime %q must be a string", valueKey)
		}

		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return nil, fmt.Errorf("parse datetime %q: %w", value, err)
		}

		if len(tagged) != 2 {
			return nil, fmt.Errorf("datetime must contain only %q and %q", typeKey, valueKey)
		}

		return parsed, nil

	case "string_list":
		return convertStringList(tagged)

	default:
		return nil, fmt.Errorf("unsupported tagged parameter type %q", typeName)
	}
}

func convertStringList(tagged map[string]any) ([]string, error) {
	rawPrefix, found := tagged[prefixKey]
	if !found {
		return nil, fmt.Errorf("string_list is missing %q", prefixKey)
	}
	prefix, ok := rawPrefix.(string)
	if !ok {
		return nil, fmt.Errorf("string_list %q must be a string", prefixKey)
	}

	rawCount, found := tagged[countKey]
	if !found {
		return nil, fmt.Errorf("string_list is missing %q", countKey)
	}
	countValue, ok := rawCount.(float64)
	if !ok || countValue < 0 || countValue != float64(int(countValue)) {
		return nil, fmt.Errorf("string_list %q must be a non-negative integer", countKey)
	}
	count := int(countValue)

	include := make([]string, 0)
	if rawInclude, found := tagged[includeKey]; found {
		values, ok := rawInclude.([]any)
		if !ok {
			return nil, fmt.Errorf("string_list %q must be a string list", includeKey)
		}
		include = make([]string, len(values))
		for idx, value := range values {
			stringValue, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("string_list %q item %d must be a string", includeKey, idx)
			}
			include[idx] = stringValue
		}
	}

	if len(tagged) != 3 && !(len(tagged) == 4 && tagged[includeKey] != nil) {
		return nil, fmt.Errorf("string_list must contain only %q, %q, %q, and optional %q", typeKey, prefixKey, countKey, includeKey)
	}

	width := len(fmt.Sprintf("%d", max(count-1, 0)))
	if width < 2 {
		width = 2
	}

	values := make([]string, 0, len(include)+count)
	values = append(values, include...)
	for idx := range count {
		values = append(values, fmt.Sprintf("%s-%0*d", prefix, width, idx))
	}
	return values, nil
}
