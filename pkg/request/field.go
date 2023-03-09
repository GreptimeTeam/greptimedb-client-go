package request

import (
	"database/sql"
	"errors"
	"reflect"
)

// ColumnDef is the representation of model schema's field
type ColumnDef struct {
	Name string
	// BindNames    []string

	// HasDefaultValue       bool
	// DefaultValue          string
	// DefaultValueInterface interface{}
	// NotNull               bool
	// Unique                bool
	// Comment               string
	// Size                  int
	// Precision             int
	ColumnType reflect.Type
	FieldType  reflect.Type
	// IndirectFieldType reflect.Type
	// StructField       reflect.StructField

	// Tag               reflect.StructTag
	// TagSettings       map[string]string
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
