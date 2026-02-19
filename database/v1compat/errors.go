package v1compat

import (
	"errors"
	"fmt"
)

func IsErrNotFound(err error) bool {
	return errors.Is(err, ErrNoResultsFound)
}

func IsErrPropertyNotFound(err error) bool {
	return errors.Is(err, ErrPropertyNotFound)
}

func IsMissingResultExpectation(err error) bool {
	return errors.Is(err, ErrMissingResultExpectation)
}

// NewError returns an error that contains the given query context elements.
func NewError(query string, driverErr error) error {
	return fmt.Errorf("driver error: %w - query: %s", driverErr, query)
}
