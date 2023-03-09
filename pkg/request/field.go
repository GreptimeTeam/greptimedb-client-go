package request

import (
	"database/sql"
	"errors"
	"reflect"
)

// ColumnDef is the representation of model schema's field
type ColumnDef struct {
	Name       string
	ColumnType reflect.Type
	FieldType  reflect.Type
}

func InitColumnDef(columnType *sql.ColumnType) (*ColumnDef, error) {
	if columnType == nil {
		return nil, errors.New("columnType should not be empty")
	}

	return &ColumnDef{
		Name:       columnType.Name(),
		ColumnType: columnType.ScanType(),
		FieldType:  columnType.ScanType(),
	}, nil
}
