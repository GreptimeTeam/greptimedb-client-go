package request

import (
	"errors"
	"reflect"
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// Semantic const
const (
	Semantic_Tag       string = "TAG"
	Semantic_Field     string = "FIELD"
	Sementic_Timestamp string = "TIMESTAMP"
)

type InsertRequest struct {
	Header
	Table string
	Data  []any
}

func (r *InsertRequest) WithTable(table string) *InsertRequest {
	r.Table = table
	return r
}

func (r *InsertRequest) WithData(data []any) *InsertRequest {
	r.Data = data
	return r
}

func (r *InsertRequest) IsTableEmpty() bool {
	return len(strings.TrimSpace(r.Table)) == 0
}

func (r *InsertRequest) Build() (*greptime.GreptimeRequest, error) {
	if r.IsDatabaseEmpty() {
		return nil, ErrEmptyDatabase
	}

	header := &greptime.RequestHeader{
		Catalog: r.Catalog,
		Schema:  r.Datadase,
	}

	columns, rowCount, err := r.buildColumnsFromData()
	if err != nil {
		return nil, err
	}

	insert := &greptime.GreptimeRequest_Insert{
		Insert: &greptime.InsertRequest{
			TableName:    r.Table,
			Columns:      columns,
			RowCount:     rowCount,
			RegionNumber: 0,
		},
	}

	return &greptime.GreptimeRequest{
		Header:  header,
		Request: insert,
	}, nil
}

// buildColumnsFromData
// return `columns` `rowCount` `error`
func (r *InsertRequest) buildColumnsFromData() ([]*greptime.Column, uint32, error) {
	if len(r.Data) == 0 {
		return nil, 0, nil
	}

	if !allDataInSameType(r.Data) {
		return nil, 0, ErrType
	}

	columns, err := initColumnsFromData(r.Data[0])
	if err != nil {
		return nil, 0, err
	}

	columns, rowCount, err := fillInColumnsWithData(columns, r.Data)
	if err != nil {
		return nil, 0, err
	}

	// TODO(vinland-avalon): so far, nullable is not supported, so nil is not allowed
	// just fill in with 0
	columns = fillInColumnsNullMask(columns)

	return columns, rowCount, nil
}

func allDataInSameType(data []any) bool {
	for i := 1; i < len(data); i++ {
		if reflect.TypeOf(data[i-1]) != reflect.TypeOf(data[i]) {
			return false
		}
	}
	return true
}

func initColumnsFromData(data any) ([]*greptime.Column, error) {
	v := reflect.ValueOf(data)
	columns := make([]*greptime.Column, v.NumField())

	for i := 0; i < v.NumField(); i++ {
		// tag := v.Type().Field(i).Tag
		name, rawSemantic := getMetaData(v.Type().Field(i))
		// get semantic type
		semanticType, err := mapToSemanticType(rawSemantic)
		if err != nil {
			return columns, err
		}

		// init nullMask
		// all the data is nullable, so just fill up with 0 afterwards
		nullMask := make([]byte, 0)

		// get DataType Name
		// typeName := v.Type().Name()
		dataType, err := mapToDataType(v.Type().Field(i).Type)
		if err != nil {
			return columns, err
		}

		column := greptime.Column{
			ColumnName:   name,
			SemanticType: semanticType,
			Values:       &greptime.Column_Values{},
			NullMask:     nullMask,
			Datatype:     dataType,
		}
		columns[i] = &column
	}
	return columns, nil
}

// getMetaData returns `name` `semantic`
// TODO(vinland-avalon): a better extractor to deal with recursive
func getMetaData(field reflect.StructField) (string, string) {
	// set default name: if name alias doesnot exist,
	// use the lowcase-form of field name
	// set default semantic: FIELD
	name := strings.ToLower(field.Name)
	semantic := "FIELD"
	// try to retrieve from tag
	tag := field.Tag
	meta := tag.Get("db")
	if len(meta) > 0 {
		metaSlice := strings.Split(meta, ",")
		name = metaSlice[0]
		if len(metaSlice) > 1 {
			semantic = metaSlice[1]
		}
	}
	return name, semantic
}

func mapToSemanticType(s string) (greptime.Column_SemanticType, error) {
	if val, ok := greptime.Column_SemanticType_value[s]; ok {
		return greptime.Column_SemanticType(val), nil
	}
	return 0, UndefinedTypeError("SemanticType")
}

func mapToDataType(t reflect.Type) (greptime.ColumnDataType, error) {
	if val, ok := greptime.ColumnDataType_value[t.Name()]; ok {
		return greptime.ColumnDataType(val), nil
	}
	return 0, UndefinedTypeError("DataType")
}

// TODO(vinland-avalon): have no idea about how to  transfer with type
// SO, so far just support string
// and int64 for Semantic-TIMESTAMP-second
func fillInColumnsWithData(columns []*greptime.Column, data []any) ([]*greptime.Column, uint32, error) {
	// for i, column := range columns{
	// 	dataType := column.GetDatatype()
	// 	semanticType := column.GetSemanticType()
	// 	switch semanticType {
	// 	case greptime.Column_TIMESTAMP:
	// 			values := make([]int64, 0)
	// 			for j, dataRow := range data {
	// 				value := reflect.ValueOf(data[j]).MapRange().Value()
	// 			}
	// 		}
	// 	}
	return nil, 0, errors.New("Have not implemented fillInColumnsWithData")

}

func fillInColumnsNullMask(columns []*greptime.Column) []*greptime.Column {
	return columns
}
