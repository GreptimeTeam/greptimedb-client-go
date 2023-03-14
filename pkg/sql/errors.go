package sql

import (
	"errors"
)

var (
	ErrEmptyDatabase = errors.New("name of database should not be empty")
	ErrEmptyKey      = errors.New("key should not be empty")
)
