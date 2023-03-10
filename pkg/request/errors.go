package request

import (
	"errors"
)

var (
	ErrEmptyDatabase        = errors.New("name of database should not be empty")
	ErrEmptyTable           = errors.New("name of table should not be be empty")
	ErrEmptyTimestamp       = errors.New("key of timestamp should not be empty")
	ErrEmptyKey             = errors.New("key should not be empty")
	ErrEmptySql             = errors.New("sql is required in querying")
	ErrInvalidTimePrecision = errors.New("precision of timestamp is not valid")
)
