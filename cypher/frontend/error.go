package frontend

import (
	"errors"
	"fmt"
)

type SyntaxError struct {
	Line            int
	Column          int
	OffendingSymbol any
	Message         string
}

func (s SyntaxError) Error() string {
	return fmt.Sprintf("line %d:%d %s", s.Line, s.Column, s.Message)
}

var (
	ErrUpdateClauseNotSupported            = errors.New("updating clauses are not supported")
	ErrUpdateWithExpansionNotSupported     = errors.New("updating clauses with expansions are not supported")
	ErrUserSpecifiedParametersNotSupported = errors.New("user-specified parameters are not supported")
	ErrProcedureInvocationNotSupported     = errors.New("procedure invocation is not supported")

	ErrInvalidInput = errors.New("invalid input")
)
