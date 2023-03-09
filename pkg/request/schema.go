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
	return rows.Scan(s.ColumnValues...)
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
		fieldName, err := ToColumnName(typ.Field(i).Name)
		if err != nil {
			return err
		}
		if index, ok := s.ColumnIndexByName[fieldName]; ok {
			// then set the column type
			// fmt.Printf("with UDStruct, type for field %s: expected %s, got %s\n",
			// fieldName(typ.Field(i)), s.ColumnDefs[index].ColumnType, typ.Field(i).Type)
			if !s.ColumnDefs[index].ColumnType.AssignableTo(typ.Field(i).Type) {
				return fmt.Errorf("incorrect type for field %s: expected %s, got %s",
					fieldName, s.ColumnDefs[index].ColumnType, typ.Field(i).Type)
			}
			s.ColumnDefs[index].FieldType = typ.Field(i).Type
		}
	}
	return nil
}

// setUDStruct: set the values of the struct fields from the row data
func (s *Schema) setUDStruct(elemType reflect.Type) (reflect.Value, error) {
	// // Create a new struct instance
	structValue := reflect.New(elemType).Elem()

	for i := 0; i < structValue.NumField(); i++ {
		fieldValue := structValue.Field(i)
		fieldName, err := ToColumnName(elemType.Field(i).Name)
		if err != nil {
			return structValue, err
		}
		rawValue, _ := s.valueByName(fieldName)
		if rawValue == nil {
			continue
		}
		value := reflect.ValueOf(rawValue).Elem()
		if !value.IsValid() {
			continue
		}
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				continue
			}
			value = value.Elem()
		}
		if !value.Type().AssignableTo(fieldValue.Type()) {
			return structValue, fmt.Errorf("incorrect type for field %s: expected %s, got %s",
				elemType.Field(i).Name, fieldValue.Type(), value.Type())
		}
		fieldValue.Set(value)
	}

	return structValue, nil
}
