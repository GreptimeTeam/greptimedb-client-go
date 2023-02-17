package write

import (
	"errors"
	"fmt"

	v1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type WriteRows struct {
	request v1.InsertRequest
	schema  string
	table   string
}

// TODO: wrap schema(database name) and table(table name) into "TableName" with "with" function, used for requestHeader
func InitWriteRowsWithTable(schema string, table string) *WriteRows {
	var rows WriteRows
	rows.request = v1.InsertRequest{TableName: table, RowCount: 0, RegionNumber: 1}
	rows.schema = schema
	rows.table = table
	return nil
}

func (rows *WriteRows) SetColumnDefs(columnDefs []*ColumnDef) (*WriteRows, error) {
	if len(columnDefs) == 0 {
		return rows, errors.New("No Columns to insert")
	}
	rows.request.Columns = make([]*v1.Column, len(columnDefs))
	for i, columnDef := range columnDefs {
		var column v1.Column

		// TODO: check each field if valid
		column.ColumnName = columnDef.name
		column.SemanticType = v1.Column_SemanticType(columnDef.semanticType)
		column.Datatype = v1.ColumnDataType(columnDef.dataType)
		column.Values = &v1.Column_Values{}
		column.NullMask = make([]byte, 0)

		rows.request.Columns[i] = &column

	}
	return rows, nil
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
