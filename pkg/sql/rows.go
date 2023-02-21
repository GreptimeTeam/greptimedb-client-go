package sql

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
)

type rows struct {
	reader *flight.Reader
	fields []arrow.Field
	record array.Record
	idx    int
}

// method of driver.Rows interface
func (r *rows) Columns() []string {
	if r.reader == nil {
		return nil
	}

	columns := make([]string, len(r.fields))
	for i, field := range r.fields {
		columns[i] = field.Name
	}
	return columns
}

// method of driver.Rows interface
func (r *rows) Close() error {
	if r.record != nil {
		r.record = nil
	}
	if r.reader != nil {
		r.reader.Release()
		r.reader = nil
	}
	return nil
}

// method of driver.Rows interface
// Next should return io.EOF when there are no more rows.
func (r *rows) Next(dest []driver.Value) error {
	for r.record == nil || r.idx >= int(r.record.NumRows()) {
		if cur, err := r.reader.Read(); err == io.EOF {
			r.record = nil
			_ = r.Close()
			return io.EOF
		} else if err != nil {
			_ = r.Close()
			return err
		} else {
			r.record = cur
			r.idx = 0
		}
	}

	for i := 0; i < int(r.record.NumCols()); i++ {
		col := r.record.Column(i)
		value, err := fromColumn(col, r.idx)
		if err != nil {
			_ = r.Close()
			return err
		}
		dest[i] = value
	}

	r.idx++

	return nil
}

// retrive arrow value from the column at idx position, and convert it to driver.Value
func fromColumn(column array.Interface, idx int) (driver.Value, error) {
	if column.IsNull(idx) {
		return nil, nil
	}
	switch typedColumn := column.(type) {
	case *array.Timestamp:
		return time.Unix(0, int64(typedColumn.Value(idx))), nil
	case *array.Float64:
		return typedColumn.Value(idx), nil
	case *array.Uint64:
		return typedColumn.Value(idx), nil
	case *array.Int64:
		return typedColumn.Value(idx), nil
	case *array.String:
		return typedColumn.Value(idx), nil
	case *array.Binary:
		return typedColumn.Value(idx), nil
	case *array.Boolean:
		return typedColumn.Value(idx), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
	}
}

// RowsColumnTypeScanType may be implemented by Rows. It should return
// the value type that can be used to scan types into. For example, the database
// column type "bigint" this should return "reflect.TypeOf(int64(0))".
//
// driver.RowsColumnTypeScanType interface
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index >= len(r.fields) {
		return nil
	}
	switch r.fields[index].Type.ID() {
	case arrow.TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case arrow.FLOAT32:
		return reflect.TypeOf(float32(0))
	case arrow.DECIMAL, arrow.FLOAT64:
		return reflect.TypeOf(float64(0))
	case arrow.UINT64:
		return reflect.TypeOf(uint64(0))
	case arrow.INT64:
		return reflect.TypeOf(int64(0))
	case arrow.STRING:
		return reflect.TypeOf("")
	case arrow.BINARY:
		return reflect.TypeOf([]byte(nil))
	case arrow.BOOL:
		return reflect.TypeOf(true)
	default:
		return nil
	}
}

// RowsColumnTypeDatabaseTypeName may be implemented by Rows. It should return the
// database system type name without the length. Type names should be uppercase.
// Examples of returned types: "VARCHAR", "NVARCHAR", "VARCHAR2", "CHAR", "TEXT",
// "DECIMAL", "SMALLINT", "INT", "BIGINT", "BOOL", "[]BIGINT", "JSONB", "XML",
// "TIMESTAMP".
//
//	river.RowsColumnTypeDatabaseTypeName interface
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index >= len(r.fields) {
		return ""
	}
	return r.fields[index].Type.ID().String()
}

// RowsColumnTypeLength may be implemented by Rows. It should return the length
// of the column type if the column is a variable length type. If the column is
// not a variable length type ok should return false.
// If length is not limited other than system limits, it should return math.MaxInt64.
// The following are examples of returned values for various types:
//
//	TEXT          (math.MaxInt64, true)
//	varchar(10)   (10, true)
//	nvarchar(10)  (10, true)
//	decimal       (0, false)
//	int           (0, false)
//	bytea(30)     (30, true)
//
// driver.RowsColumnTypeLength interface
func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index >= len(r.fields) {
		return 0, false
	}
	switch r.fields[index].Type.ID() {
	case arrow.TIMESTAMP, arrow.FLOAT64, arrow.UINT64, arrow.INT64, arrow.BOOL:
		return 0, false
	case arrow.STRING, arrow.BINARY:
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

// RowsColumnTypeNullable may be implemented by Rows. The nullable value should
// be true if it is known the column may be null, or false if the column is known
// to be not nullable.
// If the column nullability is unknown, ok should be false.
//
// driver.RowsColumnTypeNullable interface
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index >= len(r.fields) {
		return false, false
	}
	return r.fields[index].Nullable, true
}

// RowsColumnTypePrecisionScale may be implemented by Rows. It should return
// the precision and scale for decimal types. If not applicable, ok should be false.
// The following are examples of returned values for various types:
//
//	decimal(38, 4)    (38, 4, true)
//	int               (0, 0, false)
//	decimal           (math.MaxInt64, math.MaxInt64, true)
//
// driver.RowsColumnTypePrecisionScale interface
func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return 0, 0, false
}
