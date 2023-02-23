package request

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyDatabase = errors.New("database is required")
	ErrEmptySql      = errors.New("sql is required in querying")
	ErrType          = errors.New("type err")
)

func UnsupportedTypeError(typeName string) error {
	return fmt.Errorf("type '%v' unsupported.", typeName)
}
