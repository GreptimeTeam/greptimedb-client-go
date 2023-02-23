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

func UndefinedTypeError(typeName string) error {
	return fmt.Errorf("undefined: %v", typeName)
}
