package request

import (
	"errors"
)

var (
	ErrEmptyDatabase  = errors.New("have not set database")
	ErrEmptyTable     = errors.New("have not set table")
	ErrEmptyTimestamp = errors.New("have not set timestamp")
	ErrEmptySql       = errors.New("sql is required in querying")
)
