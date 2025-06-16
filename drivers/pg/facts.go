package pg

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type SQLState string

func (s SQLState) String() string {
	return string(s)
}

func (s SQLState) ErrorMatches(err error) bool {
	var pgConnErr *pgconn.PgError
	return errors.As(err, &pgConnErr) && pgConnErr.Code == s.String()
}

const (
	StateObjectDoesNotExist SQLState = "42704"
)
