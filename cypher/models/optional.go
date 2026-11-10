package models

// Optional is a simple generic optional type.
//
// See: https://en.wikipedia.org/wiki/Option_type
type Optional[T any] struct {
	// Value holds the present value when Set is true, including its type's zero value.
	Value T
	// Set distinguishes an absent value from a present zero value.
	Set bool
}

// GetOr returns the present value or defaultValue without changing the optional.
func (s Optional[T]) GetOr(defaultValue T) T {
	if s.Set {
		return s.Value
	}

	return defaultValue
}

// OptionalValue marks value as present, even when it is the zero value.
func OptionalValue[T any](value T) Optional[T] {
	return Optional[T]{
		Value: value,
		Set:   true,
	}
}

// OptionalPointer copies a non-nil pointed-to value into a present optional; nil produces an absent value.
func OptionalPointer[T any](value *T) Optional[T] {
	if value == nil {
		return EmptyOptional[T]()
	}

	return Optional[T]{
		Value: *value,
		Set:   true,
	}
}

// EmptyOptional returns an absent optional containing the type's zero value.
func EmptyOptional[T any]() Optional[T] {
	var emptyT T

	return Optional[T]{
		Value: emptyT,
		Set:   false,
	}
}
