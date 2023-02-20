package insert

import (
	"errors"
	"fmt"

	v1 "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type WriteRows struct {
	request *v1.InsertRequest

	Catalog  string // optional
	Database string // required
}

// the catalog field is optional
func InitWriteRowsWithDatabase(catalog string, database string, table string) *WriteRows {
	var rows WriteRows
	rows.Catalog = catalog
	rows.Database = database

	rows.request = &v1.InsertRequest{TableName: table, RowCount: 0, RegionNumber: 1}

	return &rows
}

func (rows *WriteRows) WithColumnDefs(columnDefs []*ColumnDef) (*WriteRows, error) {
	if len(columnDefs) == 0 {
		return rows, errors.New("No Columns to insert")
	}
	if rows.request == nil {
		return rows, errors.New("rows.request is nil")
	}
	rows.request.Columns = make([]*v1.Column, len(columnDefs))

	for i, columnDef := range columnDefs {
		rows.request.Columns[i] = columnDef.buildColumn()
	}
	return rows, nil
}

func (c *ColumnDef) buildColumn() *v1.Column {
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
func (rows *WriteRows) Insert(values []any) (err error) {
	if !rows.ensureInitiate() {
		err = errors.New("Have not Initiated Columns yet")
		return err
	}
	if len(values) != len(rows.request.Columns) {
		err = errors.New("Doesn't match all columns")
		return err
	}
	for i, value := range values {
		err = rows.fillInColumn(i, value)
		if err != nil {
			err = fmt.Errorf("Can not use value: %+v to fill up Column:%v, err:%+v",
				value, rows.request.Columns[i], err)
			return err
		}
	}
	rows.request.RowCount++
	return nil
}

func (rows *WriteRows) ensureInitiate() bool {
	if rows.request == nil || rows.request.Columns == nil {
		return false
	}
	return true
}

func (rows *WriteRows) fillInColumn(index int, value any) error {
	// TODO(vinland-avalon): replace it with a pachage
	// byteIndex := index / 8
	// bitIndex := index % 8
	// if bitIndex == 0 {
	//	rows.request.Columns[index].NullMask = append(rows.request.Columns[index].NullMask, 0)
	// }

	// if value == nil {
	//	rows.request.Columns[index].NullMask[byteIndex] &= ^(1 << bitIndex)
	// } else {
	//	rows.request.Columns[index].NullMask[byteIndex] |= (1 << bitIndex)
	// }

	column := rows.request.Columns[index]
	column.Values.StringValues = append(column.Values.StringValues, value.(string))

	return nil
}

// Column_TAG       Column_SemanticType = 0
// Column_FIELD     Column_SemanticType = 1
// Column_TIMESTAMP Column_SemanticType = 2
type SemanticType v1.Column_SemanticType
type DataType v1.ColumnDataType

type ColumnDef struct {
	semanticType SemanticType
	dataType     DataType
	name         string
	isNullable   bool
}

// TODO(vinland-avalon): the valueType can only be string so far, so should use 12 for dataType field
func InitColumnDef(semanticType SemanticType, dataType DataType, columnName string, isNullable bool) *ColumnDef {
	return &ColumnDef{semanticType, dataType, columnName, isNullable}
}

func initRequestHeader(rows *WriteRows) (*v1.RequestHeader, error) {
	if rows == nil {
		return nil, errors.New("rows is nil")
	}
	return &v1.RequestHeader{Catalog: rows.Catalog, Schema: rows.Database}, nil
}

func IntoGreptimeRequest(rows *WriteRows) (*v1.GreptimeRequest, error) {
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
