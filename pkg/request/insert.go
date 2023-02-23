package request

import (
	"errors"
	"fmt"
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

// TODO(yuanbohan): unit test
// mapSemantic only support index, timestamp. Others mean not set
func mapSemantic(s string) greptime.Column_SemanticType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "index":
		return greptime.Column_TAG
	case "timestamp":
		return greptime.Column_TIMESTAMP
	default:
		return greptime.Column_FIELD
	}
}

func mapType(typ reflect.Type) (greptime.ColumnDataType, error) {
	var gt greptime.ColumnDataType = -1
	var err error
	switch typ.Kind() {
	case reflect.Bool:
		gt = greptime.ColumnDataType_BOOLEAN
	case reflect.Int | reflect.Int64:
		gt = greptime.ColumnDataType_INT64
	case reflect.Int8:
		gt = greptime.ColumnDataType_INT8
	case reflect.Int16:
		gt = greptime.ColumnDataType_INT16
	case reflect.Int32:
		gt = greptime.ColumnDataType_INT32
	case reflect.Uint | reflect.Uint64:
		gt = greptime.ColumnDataType_UINT64
	case reflect.Uint8:
		gt = greptime.ColumnDataType_UINT8
	case reflect.Uint16:
		gt = greptime.ColumnDataType_UINT16
	case reflect.Uint32:
		gt = greptime.ColumnDataType_UINT32
	case reflect.Float32:
		gt = greptime.ColumnDataType_FLOAT32
	case reflect.Float64:
		gt = greptime.ColumnDataType_FLOAT64
	case reflect.Slice: // []byte
		if typ.Elem().Kind() == reflect.Uint8 {
			gt = greptime.ColumnDataType_BINARY
		} else {
			err = UnsupportedTypeError(typ.Name())
		}
		gt = greptime.ColumnDataType_STRING
	case reflect.String:
		gt = greptime.ColumnDataType_STRING
	case reflect.Struct: // Time
		if strings.EqualFold(typ.Name(), "time") {
			gt = greptime.ColumnDataType_DATETIME
		} else {
			err = UnsupportedTypeError(typ.Name())
		}
	default:
		err = UnsupportedTypeError(typ.Name())
	}
	return gt, err
}

// TODO(yuanbohan): support struct literal.
// so far only support struct slice
type InsertRequest struct {
	Header
	Table string
	Data  []any
}

func (r *InsertRequest) RowCount() int {
	if reflect.TypeOf(r.Data).Kind() == reflect.Slice {
		reflect.ValueOf(r.Data)
	}

	if len(r.Data) > 0 {
		return reflect.TypeOf(r.Data[0]).NumField()
	} else {
		return 0
	}
}

func (r *InsertRequest) ColumnCount() int {
	if len(r.Data) > 0 {
		return reflect.TypeOf(r.Data[0]).NumField()
	} else {
		return 0
	}
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
		Schema:  r.Database,
	}

	columns, err := r.buildColumns()
	for _, column := range columns {
		fmt.Printf("column: %+v\n\n", column)
	}

	if err != nil {
		return nil, err
	}

	insert := &greptime.GreptimeRequest_Insert{
		Insert: &greptime.InsertRequest{
			TableName:    r.Table,
			Columns:      columns,
			RowCount:     uint32(r.RowCount()),
			RegionNumber: 0,
		},
	}

	return &greptime.GreptimeRequest{
		Header:  header,
		Request: insert,
	}, nil
}

// TODO(yuanbohan): MUST check the index out of boundary
// TODO(yuanbohan): set null mask
func (r *InsertRequest) buildColumns() ([]*greptime.Column, error) {
	if len(r.Data) == 0 {
		return nil, nil
	}

	columns, err := r.extractColumns()
	if err != nil {
		return nil, err
	}

	// row is for null mask
	// for _, data := range r.Data {
	//	val := reflect.ValueOf(data)
	//	for col := 0; col < val.NumField(); col++ {
	//		field := val.Field(col)
	//		setColumnVal(columns[col], field)
	//	}
	// }

	return columns, nil
}

func setColumnVal(column *greptime.Column, val reflect.Value) {
	fmt.Printf("in set column val: %v", val)
	switch column.Datatype {
	case greptime.ColumnDataType_BOOLEAN:
		// column.Values.BoolValues = append(column.Values.BoolValues, val.Bool())
	case greptime.ColumnDataType_INT8:
	case greptime.ColumnDataType_INT16:
	case greptime.ColumnDataType_INT32:
	case greptime.ColumnDataType_INT64:
	case greptime.ColumnDataType_UINT8:
	case greptime.ColumnDataType_UINT16:
	case greptime.ColumnDataType_UINT32:
	case greptime.ColumnDataType_UINT64:
	case greptime.ColumnDataType_FLOAT32:
	case greptime.ColumnDataType_FLOAT64:
	case greptime.ColumnDataType_BINARY:
	case greptime.ColumnDataType_STRING:
	case greptime.ColumnDataType_DATE:
	case greptime.ColumnDataType_DATETIME:
	case greptime.ColumnDataType_TIMESTAMP_SECOND:
	case greptime.ColumnDataType_TIMESTAMP_MILLISECOND:
	case greptime.ColumnDataType_TIMESTAMP_MICROSECOND:
	case greptime.ColumnDataType_TIMESTAMP_NANOSECOND:
	}

}

func (r *InsertRequest) extractColumns() ([]*greptime.Column, error) {
	// FIXME(yuanbohan): Data may not be slice or array, it may be a struct directly (inserting only one row)
	v := reflect.TypeOf(r.Data[0])
	if v.Kind() != reflect.Struct {
		return nil, errors.New("only struct is supproted")
	}

	columns := make([]*greptime.Column, r.ColumnCount())
	for i := 0; i < v.NumField(); i++ {
		column, err := extractColumn(v.Field(i))
		if err != nil {
			return nil, err
		}

		columns[i] = column
	}
	return columns, nil
}

// TODO(yuanbohan): support time.Time, numeric when semantic is timestamp, and support unit (s, ms, ns)
func extractColumn(field reflect.StructField) (*greptime.Column, error) {
	name := strings.ToLower(field.Name)
	semantic := greptime.Column_FIELD

	if db, ok := field.Tag.Lookup("db"); ok {
		meta := strings.SplitN(strings.ToLower(db), ",", 2)
		if len(meta) > 0 {
			metaName := strings.TrimSpace(meta[0])
			if len(metaName) > 0 {
				name = metaName
			}

			if len(meta) == 2 {
				// fmt.Print(meta[1])
				semantic = mapSemantic(meta[1])
				// fmt.Println(semantic)
			}
		}
	}

	typ, err := mapType(field.Type)
	if err != nil {
		return nil, err
	}

	return &greptime.Column{
		ColumnName:   name,
		SemanticType: semantic,
		Datatype:     typ,
	}, nil
}
