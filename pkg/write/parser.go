package write

import (
	"errors"
	"fmt"

	v1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type WriteRows struct {
	request *v1.InsertRequest

	// wrap schema(database name) and table(table name) into "TableName" with "with" function, used for requestHeader
	schema string
	table  string
}

func InitWriteRowsWithTable(database string, table string) *WriteRows {
	var rows WriteRows
	rows.schema = database
	rows.table = table

	rows.request = &v1.InsertRequest{TableName: table, RowCount: 0, RegionNumber: 1}
	return nil
}

func (rows *WriteRows) SetColumnDefs(columnDefs []*ColumnDef) (*WriteRows, error) {
	if len(columnDefs) == 0 {
		return rows, errors.New("No Columns to insert")
	}
	if rows.request == nil {
		return rows, errors.New("rows.request is nil")
	}
	rows.request.Columns = make([]*v1.Column, len(columnDefs))

	for i, columnDef := range columnDefs {
		rows.request.Columns[i] = columnDef.Into()
	}
	return rows, nil
}

func (c *ColumnDef) Into() *v1.Column {
	var column v1.Column

	column.ColumnName = c.name
	column.SemanticType = v1.Column_SemanticType(c.semanticType)
	column.Datatype = v1.ColumnDataType(c.dataType)
	column.Values = &v1.Column_Values{}
	//TODO: think about null
	column.NullMask = make([]byte, 0)

	return &column
}

// TODO(vinland-avalon): the valueType can only be string so far
// TODO(vinland-avalon): do not support null so far, must match all columns
func (rows *WriteRows) Insert(values []any) (*WriteRows, error) {
	if !rows.ensureInitiate() {
		return rows, errors.New("Have not Initiated Columns yet")
	}
	if len(values) != len(rows.request.Columns) {
		return rows, errors.New("Doesn't match all columns")
	}
	for i, value := range values {
		err := rows.fillInColumn(i, value)
		if err != nil {
			return rows, fmt.Errorf("Can not use value: %+v to fill up Column:%v", value, rows.request.Columns[i])
		}
	}
	rows.request.RowCount++
	return rows, nil
}

// TODO: check if columns are defined before
func (rows *WriteRows) ensureInitiate() bool {
	return true
}

func (rows *WriteRows) fillInColumn(index int, value any) error {
	column := rows.request.Columns[index]
	column.Values.StringValues = append(column.Values.StringValues, value.(string))
	return nil
}

type SemanticType v1.Column_SemanticType
type DataType v1.ColumnDataType

type ColumnDef struct {
	semanticType SemanticType
	dataType     DataType
	name         string
}

// TODO(vinland-avalon): the valueType can only be string so far, so should use 12 for dataType field
func InitColumnDef(semanticType SemanticType, dataType DataType, columnName string) *ColumnDef {
	return &ColumnDef{semanticType, dataType, columnName}
}

func initRequestHeader(rows *WriteRows) (*v1.RequestHeader, error) {
	if rows == nil {
		return nil, errors.New("rows is nil")
	}
	return &v1.RequestHeader{Catalog: rows.schema, Schema: rows.table}, nil
}

func InitGreptiemRequest(rows *WriteRows) (*v1.GreptimeRequest, error) {
	if rows == nil || rows.request == nil {
		return nil, errors.New("rows or rows.request is nil")
	}
	header, err := initRequestHeader(rows)
	if err != nil {
		return nil, fmt.Errorf("Cannot init GreptimeRequest with rows:%+v", *rows)
	}
	return &v1.GreptimeRequest{Header: header,
		Request: &v1.GreptimeRequest_Insert{Insert: rows.request}}, nil
}
