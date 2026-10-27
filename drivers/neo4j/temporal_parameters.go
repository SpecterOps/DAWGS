package neo4j

import (
	"reflect"
	"time"
)

// normalizeTemporalParameters supplies the marker the Neo4j driver requires for
// numeric-offset times. RFC3339 parsing can create an unnamed location, which
// the driver would otherwise send as an invalid empty timezone name. Named
// locations and the caller's parameter containers are preserved.
func normalizeTemporalParameters(parameters map[string]any) map[string]any {
	normalized, changed := normalizeTemporalValue(reflect.ValueOf(parameters))
	if !changed {
		return parameters
	}
	return normalized.Interface().(map[string]any)
}

func normalizeTemporalValue(value reflect.Value) (reflect.Value, bool) {
	if value.IsValid() && value.CanInterface() {
		if timestamp, ok := value.Interface().(time.Time); ok {
			if timestamp.Location().String() == "" {
				_, offset := timestamp.Zone()
				return reflect.ValueOf(timestamp.In(time.FixedZone("Offset", offset))), true
			}
			return value, false
		}
	}

	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			return normalizeTemporalValue(value.Elem())
		}
	case reflect.Map:
		if !temporalParameterType(value.Type().Elem()) {
			return value, false
		}
		var result reflect.Value
		for iterator := value.MapRange(); iterator.Next(); {
			if normalized, changed := normalizeTemporalValue(iterator.Value()); changed {
				if !result.IsValid() {
					result = reflect.MakeMapWithSize(value.Type(), value.Len())
					for entry := value.MapRange(); entry.Next(); {
						result.SetMapIndex(entry.Key(), entry.Value())
					}
				}
				result.SetMapIndex(iterator.Key(), normalized)
			}
		}
		if result.IsValid() {
			return result, true
		}
	case reflect.Slice, reflect.Array:
		if !temporalParameterType(value.Type().Elem()) {
			return value, false
		}
		var result reflect.Value
		for idx := 0; idx < value.Len(); idx++ {
			if normalized, changed := normalizeTemporalValue(value.Index(idx)); changed {
				if !result.IsValid() {
					if value.Kind() == reflect.Slice {
						result = reflect.MakeSlice(value.Type(), value.Len(), value.Len())
						reflect.Copy(result, value)
					} else {
						result = reflect.New(value.Type()).Elem()
						result.Set(value)
					}
				}
				result.Index(idx).Set(normalized)
			}
		}
		if result.IsValid() {
			return result, true
		}
	}
	return value, false
}

// Skip scalar collections, particularly the large ID and byte lists used by batches.
func temporalParameterType(valueType reflect.Type) bool {
	if valueType == reflect.TypeFor[time.Time]() {
		return true
	}
	switch valueType.Kind() {
	case reflect.Interface, reflect.Map, reflect.Slice, reflect.Array:
		return true
	default:
		return false
	}
}
