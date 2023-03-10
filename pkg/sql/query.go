package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

func Query(db *sql.DB, sql string, dest any) error {
	if db == nil {
		return ErrEmptyDatabase
	}
	if rows, err := db.Query(sql); err == nil {
		return fillStructSlice(dest, rows)
	} else {
		return err
	}
}

// FIXME(yuanbohan): empty rows means empty result
func fillStructSlice(dest any, rows *sql.Rows) error {
	if rows == nil {
		return errors.New("rows should not be empty")
	}

	// check if the dest can be set
	if err := isStructSliceSettable(dest); err != nil {
		return err
	}

	// reach the real value of dest
	sliceElem := reflect.ValueOf(dest).Elem()

	// Init RowDataSchema
	rowDataSchema, err := initSchema(rows)
	if err != nil {
		return err
	}

	// Get the type of the slice elements
	elemType := sliceElem.Type().Elem()
	if err := rowDataSchema.withUDStruct(elemType); err != nil {
		return err
	}

	// Iterate over the rows and create a new struct for each row
	for rows.Next() {
		if err := rowDataSchema.withValue(rows); err != nil {
			return err
		}

		structValue, err := rowDataSchema.setUDStruct(elemType)
		if err != nil {
			return err
		}

		// Append the new struct to the slice
		sliceElem.Set(reflect.Append(sliceElem, structValue))
	}

	return rows.Err()
}

func isStructSliceSettable(dest any) error {
	// Check that the first input is a pointer to a slice
	sliceValue := reflect.ValueOf(dest)
	if sliceValue.Kind() != reflect.Ptr || sliceValue.Elem().Kind() != reflect.Slice {
		return errors.New("dest must be a pointer to slice")
	}

	// Check that each field can be set
	elemType := sliceValue.Elem().Type().Elem()
	structValue := reflect.New(elemType).Elem()

	for i := 0; i < structValue.NumField(); i++ {
		if !structValue.Field(i).CanSet() {
			return fmt.Errorf("field %s is not settable", elemType.Field(i).Name)
		}
	}

	return nil
}
