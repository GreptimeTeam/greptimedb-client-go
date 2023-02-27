package request

import (
	"errors"
)

var (
	ErrEmptyDatabase = errors.New("have not set database")
	ErrEmptyTable = errors.New("have not set table")
	ErrEmptySql      = errors.New("sql is required in querying")
)
