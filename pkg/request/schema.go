package request

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

var ErrUnsupportedDataType = errors.New("unsupported data type")

// Schema
// responsible for parsing row data to user-defined struct
// the bind is name, so far
type Schema struct {
	ColumnIndexByName map[string]int
	ColumnDefs        []*ColumnDef
	ColumnValues      []any
	Len               int
}

// initSchema: init schema with *sql.Rows.
// Map column name to column definitions and column values.
// The `ColumnValues“ are empty.
func initSchema(rows *sql.Rows) (*Schema, error) {
	if rows == nil {
		return nil, errors.New("rows should not be empty")
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	indexMap := map[string]int{}
	for i, column := range columns {
		indexMap[column] = i
	}

	fields := []*ColumnDef{}
	columnsTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	for _, columnType := range columnsTypes {
		field, err := InitColumnDef(columnType)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return &Schema{
		ColumnDefs:        fields,
		ColumnIndexByName: indexMap,
		ColumnValues:      make([]any, len(columns)),
		Len:               len(columns),
	}, nil

}

// withValue: fill in field `ColumnValues` with a row of data source
func (s *Schema) withValue(rows *sql.Rows) error {
	// TODO(vinland-avalon): check if initiated
	if rows == nil {
		return errors.New("rows should not be empty")
	}

	// Iterate over the fields and create pointers to the field values
	// The element are of the same type with user-defined struct fields
	for i := 0; i < s.Len; i++ {
		s.ColumnValues[i] = reflect.New(s.ColumnDefs[i].FieldType).Interface()
	}

	// Fill data
	if err := rows.Scan(s.ColumnValues...); err != nil {
		return err
	}
	return nil
}

func (s *Schema) valueByName(fieldName string) (any, error) {
	if index, ok := s.ColumnIndexByName[fieldName]; ok {
		return s.ColumnValues[index], nil
	}
	return nil, nil
}

// withUDStruct: with fields of user-defined struct, set corresponding column type
func (s *Schema) withUDStruct(typ reflect.Type) error {
	for i := 0; i < typ.NumField(); i++ {
		// if the field in user-defined struct corresponds to one column
		if index, ok := s.ColumnIndexByName[extractFieldName(typ.Field(i))]; ok {
			// then set the column type
			if !s.ColumnDefs[index].ColumnType.AssignableTo(typ.Field(i).Type) {
				return fmt.Errorf("incorrect type for field %s: expected %s, got %s",
					extractFieldName(typ.Field(i)), s.ColumnDefs[index].ColumnType, typ.Field(i).Type)
			}
			s.ColumnDefs[index].FieldType = typ.Field(i).Type
		}
	}
	return nil
}
