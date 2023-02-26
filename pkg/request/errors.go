package request

import (
	"errors"
)

var (
	ErrEmptyDatabase = errors.New("database is required")
	ErrEmptySql      = errors.New("sql is required in querying")
)
