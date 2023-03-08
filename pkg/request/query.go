package request

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type QueryRequest struct {
	Header        // required
	Sql    string // required
}

func (r *QueryRequest) WithSql(sql string) *QueryRequest {
	r.Sql = sql
	return r
}

func (r *QueryRequest) IsSqlEmpty() bool {
	return len(strings.TrimSpace(r.Sql)) == 0
}

func (r *QueryRequest) Build() (*greptime.GreptimeRequest, error) {
	if r.IsDatabaseEmpty() {
		return nil, ErrEmptyDatabase
	}

	if r.IsSqlEmpty() {
		return nil, ErrEmptySql
	}

	header := &greptime.RequestHeader{
		Catalog: r.Catalog,
		Schema:  r.Database,
	}

	query := &greptime.GreptimeRequest_Query{
		Query: &greptime.QueryRequest{
			Query: &greptime.QueryRequest_Sql{
				Sql: r.Sql,
			},
		},
	}

	return &greptime.GreptimeRequest{
		Header:  header,
		Request: query,
	}, nil
}

// func Query(db *sql.DB, sql string, dest any) error {
// 	typ := reflect.TypeOf(dest)
// 	value := reflect.ValueOf(dest)
// 	if typ.Kind() != reflect.Pointer {
// 		return errors.New("dest should be a pointer")
// 	}
// 	if typ.Elem().Kind() != reflect.Slice {
// 		return errors.New("dest should point to a slice so far")
// 	}
// 	if !value.IsZero() {
// 		return errors.New("dest should point to an empty slice so far")
// 	}

// 	if db == nil {
// 		return errors.New("db should not be empty")
// 	}
// 	res, err := db.Query(sql)
// 	if err != nil {
// 		fmt.Printf("db.Query err: %v", err)
// 	}

// 	elemType := typ.Elem()
// 	array := dest.([]any)

// 	for res.Next() {
// 		// the pointer to a `monitor` in zero
// 		row := reflect.New(elemType)

// 		data := make([]any, row.NumField(), row.NumField())

// 		err := res.Scan(data)
// 		if err != nil {
// 			fmt.Printf("res.Scan err: %v", err)
// 			continue
// 		}
// 		array := append(array, )
// 	}

// }

func fillStructSlice(dest interface{}, rows *sql.Rows) error {
	// check if the dest can be set
	err := isStructSliceSettable(dest)
	if err != nil {
		return err
	}

	sliceElem := reflect.ValueOf(dest).Elem()

	// Get the type of the slice elements
	elemType := sliceElem.Type().Elem()

	// Iterate over the rows and create a new struct for each row
	for rows.Next() {
		// Create a new struct instance
		structValue := reflect.New(elemType).Elem()

		// Get the row data and make sure it has the right number of columns
		rowData := make([]interface{}, structValue.NumField())
		// Iterate over the fields and create pointers to the field values
		for i := 0; i < structValue.NumField(); i++ {
			rowData[i] = structValue.Field(i).Addr().Interface()
		}
		if err := rows.Scan(rowData...); err != nil {
			return err
		}
		if len(rowData) != structValue.NumField() {
			return fmt.Errorf("incorrect number of columns for row: expected %d, got %d", structValue.NumField(), len(rowData))
		}

		// Set the values of the struct fields from the row data
		for i := 0; i < structValue.NumField(); i++ {
			fieldValue := structValue.Field(i)
			value := reflect.ValueOf(rowData[i]).Elem()
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
				return fmt.Errorf("incorrect type for field %s: expected %s, got %s", elemType.Field(i).Name, fieldValue.Type(), value.Type())
			}
			fieldValue.Set(value)
		}

		// Append the new struct to the slice
		sliceElem.Set(reflect.Append(sliceElem, structValue))
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func isStructSliceSettable(dest interface{}) error {
	// Check that the first input is a pointer to a slice
	sliceValue := reflect.ValueOf(dest)
	if sliceValue.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer to a slice")
	}
	sliceElem := sliceValue.Elem()
	if sliceElem.Kind() != reflect.Slice {
		return errors.New("dest must be a pointer to a slice")
	}

	// Check that each field can be set
	elemType := sliceElem.Type().Elem()
	structValue := reflect.New(elemType).Elem()

	for i := 0; i < structValue.NumField(); i++ {
		fieldValue := structValue.Field(i)
		if !fieldValue.CanSet() {
			return fmt.Errorf("field %s is not settable", elemType.Field(i).Name)
		}
	}

	return nil
}
