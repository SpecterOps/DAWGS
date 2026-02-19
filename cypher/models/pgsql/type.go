package pgsql

import (
	"encoding/json"

	"reflect"

	"github.com/specterops/dawgs/graph"
)

func MapStringAnyToJSONB(values map[string]any) (json.RawMessage, error) {
	for key, value := range values {
		reflectValue := reflect.ValueOf(value)

		if reflectValue.Kind() == reflect.Slice {
			if reflectValue.IsNil() {
				// Nil slices are not encoded by the sql driver to an empty array but rather as a JSON `null`. To avoid this, replace any
				// nil slice reference with a new 0 capacity allocation.
				values[key] = reflect.MakeSlice(reflectValue.Type(), 0, 0).Interface()
			}
		}
	}

	return json.Marshal(values)
}

func PropertiesToJSONB(properties *graph.Properties) (json.RawMessage, error) {
	return MapStringAnyToJSONB(properties.MapOrEmpty())
}
