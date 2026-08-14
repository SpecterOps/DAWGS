package parquet

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go/variant"
)

type variantReference struct {
	kind     reflect.Kind
	pointer  uintptr
	length   int
	capacity int
}

func validateProperties(properties map[string]any) error {
	if err := validateVariantValue(reflect.ValueOf(properties), map[variantReference]string{}, "properties"); err != nil {
		return err
	}
	_, _, err := variant.Marshal(properties)
	return err
}

func validateVariantValue(value reflect.Value, active map[variantReference]string, path string) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateVariantValue(value.Elem(), active, path)
	}

	switch value.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return nil

	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return nil

	case reflect.Uint, reflect.Uint64:
		if value.Uint() > math.MaxInt64 {
			return fmt.Errorf("VARIANT value at %s overflows int64", path)
		}
		return nil

	case reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		release, err := trackVariantReference(value, active, path)
		if err != nil {
			return err
		}
		defer release()
		return validateVariantValue(value.Elem(), active, path)

	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		if value.Type().Key().Kind() != reflect.String {
			return fmt.Errorf("VARIANT map at %s has key type %s, want string", path, value.Type().Key())
		}
		release, err := trackVariantReference(value, active, path)
		if err != nil {
			return err
		}
		defer release()

		iterator := value.MapRange()
		for iterator.Next() {
			fieldPath := path + "." + iterator.Key().String()
			if err := validateVariantValue(iterator.Value(), active, fieldPath); err != nil {
				return err
			}
		}
		return nil

	case reflect.Slice:
		if value.IsNil() || value.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		release, err := trackVariantReference(value, active, path)
		if err != nil {
			return err
		}
		defer release()
		return validateVariantElements(value, active, path)

	case reflect.Array:
		return validateVariantElements(value, active, path)

	case reflect.Struct:
		valueType := value.Type()
		for index := range value.NumField() {
			field := valueType.Field(index)
			if !field.IsExported() || variantFieldSkipped(field) {
				continue
			}
			if err := validateVariantValue(value.Field(index), active, path+"."+field.Name); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("VARIANT value at %s has unsupported type %s", path, value.Type())
	}
}

func validateVariantElements(value reflect.Value, active map[variantReference]string, path string) error {
	for index := range value.Len() {
		if err := validateVariantValue(value.Index(index), active, fmt.Sprintf("%s[%d]", path, index)); err != nil {
			return err
		}
	}
	return nil
}

func trackVariantReference(value reflect.Value, active map[variantReference]string, path string) (func(), error) {
	reference := variantReference{kind: value.Kind(), pointer: value.Pointer()}
	if value.Kind() == reflect.Slice {
		reference.length = value.Len()
		reference.capacity = value.Cap()
	}
	if firstPath, found := active[reference]; found {
		return nil, fmt.Errorf("VARIANT value cycle at %s references active value at %s", path, firstPath)
	}
	active[reference] = path
	return func() {
		delete(active, reference)
	}, nil
}

func variantFieldSkipped(field reflect.StructField) bool {
	if tag, found := field.Tag.Lookup("variant"); found {
		name, _, _ := strings.Cut(tag, ",")
		if name != "" {
			return name == "-"
		}
	}
	if tag, found := field.Tag.Lookup("json"); found {
		name, _, _ := strings.Cut(tag, ",")
		return name == "-"
	}
	return false
}
