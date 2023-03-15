package request

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type column struct {
	typ      greptime.ColumnDataType
	semantic greptime.Column_SemanticType
}

type Series struct {
	// order, columns and vals SHOULD NOT contain timestampAlias
	order          []string
	columns        map[string]column
	vals           map[string]any
	timestampAlias string
	timestamp      time.Time
}

// func (s *Series) WithPrecision(t time.Duration) {
// 	// TODO(vinland-avalon): check if valid
// 	s.timestampPrecision = t
// }

// TODO(vinland-avalon): for timestamp, use another function to return time.Time to keep precision
func (s *Series) Get(key string) (any, bool) {
	val, ok := s.vals[key]
	return val, ok
}

func checkColumnEquality(key string, col1, col2 column) error {
	if col1.typ != col2.typ {
		return fmt.Errorf("the type of '%s' does not match: '%v' and '%v'", key, col1.typ, col2.typ)
	}
	if col1.semantic != col2.semantic {
		return fmt.Errorf("tag and field MUST NOT contain same name")
	}

	return nil
}

func (s *Series) addVal(name string, val any, semantic greptime.Column_SemanticType) error {
	key, err := ToColumnName(name)
	if err != nil {
		return err
	}

	if s.columns == nil {
		s.columns = map[string]column{}
	}

	// although return `type`` along with `value` here, we only set value in `addVal`
	v, err := convert(val, time.Millisecond)
	if err != nil {
		return fmt.Errorf("add tag err: %w", err)
	}

	col, seen := s.columns[key]
	newCol := column{
		typ:      v.typ,
		semantic: semantic,
	}
	if seen {
		if err := checkColumnEquality(key, col, newCol); err != nil {
			return err
		}
	}
	s.columns[key] = newCol
	s.order = append(s.order, key)

	if s.vals == nil {
		s.vals = map[string]any{}
	}
	s.vals[key] = val

	return nil
}

// AddTag prepate tag column, and old value will be replaced if same tag is set.
// the length of key CAN NOT be longer than 100
func (s *Series) AddTag(key string, val any) error {
	return s.addVal(key, val, greptime.Column_TAG)
}

// AddField prepate field column, and old value will be replaced if same field is set.
// the length of key CAN NOT be longer than 100
func (s *Series) AddField(key string, val any) error {
	return s.addVal(key, val, greptime.Column_FIELD)
}

// SetTime set the timestamp column value with default `ts` name and millisecond precision
func (s *Series) SetTime(t time.Time) error {
	return s.SetTimeWithKey("ts", t)
}

// SetTimeWithKey set the timestamp column value with `key` name and millisecond precision
//
// # Pay attention
//
// only one timestamp column is allowed, so the name MUST be consistent, and CAN NOT be changed
func (s *Series) SetTimeWithKey(key string, t time.Time) error {
	if len(s.timestampAlias) != 0 {
		return errors.New("timestamp column name CAN NOT be set twice")
	}

	key, err := ToColumnName(key)
	if err != nil {
		return err
	}

	s.timestampAlias = key
	s.timestamp = t
	return nil
}

type Metric struct {
	timestampAlias     string
	timestampPrecision time.Duration
	// order and columns SHOULD NOT contain timestampAlias key
	order   []string
	columns map[string]column

	series []Series
}

func buildMetricWithReader(r *flight.Reader) (*Metric, error) {
	if r == nil {
		return nil, errors.New("empty pointer")
	}
	// TODO(vinland-avalon): timestamps
	fields := r.Schema().Fields()
	records, err := r.Reader.Read()
	if err != nil {
		return nil, err
	}

	// TODO(vinland-avalon): distinguish tags, fields and timestamp
	metric := Metric{}
	for i := 0; i < int(records.NumRows()); i++ {
		series := Series{}
		for j := 0; j < int(records.NumCols()); j++ {
			// fmt.Printf("schema.field: %+v\n", r.Schema().Field(i))
			// fmt.Printf("meatdata: %+v\n", r.Schema().Field(i).Type.Fingerprint())
			column := records.Column(j)
			colVal, err := FromColumn(column, i)
			if err != nil {
				return nil, err
			}
			series.AddField(fields[j].Name, colVal)
		}
		metric.AddSeries(series)
	}

	return &metric, nil
}

// retrive arrow value from the column at idx position, and convert it to driver.Value
func FromColumn(column array.Interface, idx int) (any, error) {
	if column.IsNull(idx) {
		return nil, nil
	}
	switch typedColumn := column.(type) {
	case *array.Int64:
		return typedColumn.Value(idx), nil
	case *array.Int32:
		return typedColumn.Value(idx), nil
	case *array.Uint64:
		return typedColumn.Value(idx), nil
	case *array.Uint32:
		return typedColumn.Value(idx), nil
	case *array.Float64:
		return typedColumn.Value(idx), nil
	case *array.String:
		return typedColumn.Value(idx), nil
	case *array.Binary:
		return typedColumn.Value(idx), nil
	case *array.Boolean:
		return typedColumn.Value(idx), nil
	// TODO(vinland-avalon): with precision, so far, no matter what precision, will be stored as millisecond.
	// TODO(vinland-avalon): the returned time.Time will be again `convert` to int64, so the user can get the right time
	// TODO(vinland-avalon): with semantic
	case *array.Timestamp:
		value := int64(typedColumn.Value(idx))
		fmt.Printf("got timestamp type: %+v\n", column.DataType())
		dataType, ok := column.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
		}
		switch dataType.Unit {
		case arrow.Microsecond:
			return time.UnixMicro(value), nil
		case arrow.Millisecond:
			return time.UnixMilli(value), nil
		case arrow.Second:
			return time.Unix(value, 0), nil
		case arrow.Nanosecond:
			return time.Unix(0, value), nil
		default:
			return nil, fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
		}
	default:
		return nil, fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
	}
}

// // retrive arrow value from the column at idx position, and convert it to driver.Value
// func FromColumn2(column array.Interface, idx int) (any, error) {
// 	if column.IsNull(idx) {
// 		return nil, nil
// 	}
// 	// column.Data()
// 	switch column.DataType() {
// 	case arrow.FixedWidthTypes.Timestamp_s:
// 		return time.UnixMilli(int64(typedColumn.Value(idx))), nil
// 	default:
// 		return nil, fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
// 	}
// }

func (m *Metric) GetSeries() []Series {
	return m.series
}

// SetTimePrecision set precsion for Metric. Valid durations include time.Nanosecond, time.Microsecond, time.Millisecond, time.Second.
//
// # Pay attention
//
// - once the precision has been set, it can not be changed
// - insert will fail if precision does not match with the existing precision of the table in greptimedb
func (m *Metric) SetTimePrecision(precision time.Duration) error {
	if !IsTimePrecisionValid(precision) {
		return ErrInvalidTimePrecision
	}
	m.timestampPrecision = precision
	return nil
}

func (m *Metric) AddSeries(s Series) error {
	if !IsEmptyString(m.timestampAlias) && !IsEmptyString(s.timestampAlias) &&
		!strings.EqualFold(m.timestampAlias, s.timestampAlias) {
		return fmt.Errorf("different series MUST share same timestamp key, '%s' and '%s' does not match",
			m.timestampAlias, s.timestampAlias)
	} else if IsEmptyString(m.timestampAlias) && !IsEmptyString(s.timestampAlias) {
		m.timestampAlias = s.timestampAlias
	}

	if m.columns == nil {
		m.columns = map[string]column{}
	}
	for _, key := range s.order {
		sCol := s.columns[key]
		mCol, seen := m.columns[key]
		if seen {
			if err := checkColumnEquality(key, mCol, sCol); err != nil {
				return err
			}
		} else {
			m.order = append(m.order, key)
			m.columns[key] = sCol
		}
	}

	m.series = append(m.series, s)
	return nil
}

func (m *Metric) IntoGreptimeColumn() ([]*greptime.Column, error) {
	if len(m.series) == 0 {
		return nil, errors.New("empty series in Metric")
	}

	result, err := m.normalColumns()
	if err != nil {
		return nil, err
	}

	tsColumn, err := m.timestampColumn()
	if err != nil {
		return nil, err
	}

	return append(result, tsColumn), nil
}

func (m *Metric) nullMaskByteSize() int {
	return int(math.Ceil(float64(len(m.series)) / 8.0))
}

// normalColumns does not contain timestamp semantic column
func (m *Metric) normalColumns() ([]*greptime.Column, error) {
	nullMasks := map[string]*Mask{}
	mappedCols := map[string]*greptime.Column{}
	for name, col := range m.columns {
		column := greptime.Column{
			ColumnName:   name,
			SemanticType: col.semantic,
			Datatype:     col.typ,
			Values:       &greptime.Column_Values{},
			NullMask:     nil,
		}
		mappedCols[name] = &column
	}

	for rowIdx, s := range m.series {
		for name, col := range mappedCols {
			if val, exist := s.vals[name]; exist {
				// only use `val` in `v` afterwards
				// other time.Time data (except for timestamp) are stored in millisecond
				v, err := convert(val, time.Millisecond)
				if err != nil {
					return nil, err
				}
				convertedValue := v.val
				if err := setColumn(col, convertedValue); err != nil {
					return nil, err
				}
			} else {
				mask, exist := nullMasks[name]
				if !exist {
					mask = &Mask{}
					nullMasks[name] = mask
				}
				mask.set(uint(rowIdx))
			}
		}
	}

	if len(nullMasks) > 0 {
		if err := setNullMask(mappedCols, nullMasks, m.nullMaskByteSize()); err != nil {
			return nil, err
		}
	}

	result := make([]*greptime.Column, 0, len(mappedCols))
	for _, key := range m.order {
		result = append(result, mappedCols[key])
	}

	return result, nil
}

func (m *Metric) timestampColumn() (*greptime.Column, error) {
	datatype, err := precisionToDataType(m.timestampPrecision)
	if err != nil {
		return nil, err
	}
	tsColumn := &greptime.Column{
		ColumnName:   m.timestampAlias,
		SemanticType: greptime.Column_TIMESTAMP,
		Datatype:     datatype,
		Values:       &greptime.Column_Values{},
		NullMask:     nil,
	}
	nullMask := Mask{}
	for rowIdx, s := range m.series {
		if !IsEmptyString(s.timestampAlias) {
			switch datatype {
			case greptime.ColumnDataType_TIMESTAMP_SECOND:
				setColumn(tsColumn, s.timestamp.Unix())
			case greptime.ColumnDataType_TIMESTAMP_MILLISECOND:
				setColumn(tsColumn, s.timestamp.UnixMilli())
			case greptime.ColumnDataType_TIMESTAMP_MICROSECOND:
				setColumn(tsColumn, s.timestamp.UnixMicro())
			case greptime.ColumnDataType_TIMESTAMP_NANOSECOND:
				setColumn(tsColumn, s.timestamp.UnixNano())
			}
		} else {
			nullMask.set(uint(rowIdx))
		}
	}

	if b, err := nullMask.shrink(m.nullMaskByteSize()); err != nil {
		return nil, err
	} else {
		tsColumn.NullMask = b
	}

	return tsColumn, nil
}

func setColumn(col *greptime.Column, val any) error {
	switch col.Datatype {
	case greptime.ColumnDataType_INT8:
		col.Values.I8Values = append(col.Values.I8Values, val.(int32))
	case greptime.ColumnDataType_INT16:
		col.Values.I16Values = append(col.Values.I16Values, val.(int32))
	case greptime.ColumnDataType_INT32:
		col.Values.I32Values = append(col.Values.I32Values, val.(int32))
	case greptime.ColumnDataType_INT64:
		col.Values.I64Values = append(col.Values.I64Values, val.(int64))
	case greptime.ColumnDataType_UINT8:
		col.Values.U8Values = append(col.Values.U8Values, val.(uint32))
	case greptime.ColumnDataType_UINT16:
		col.Values.U16Values = append(col.Values.U16Values, val.(uint32))
	case greptime.ColumnDataType_UINT32:
		col.Values.U32Values = append(col.Values.U32Values, val.(uint32))
	case greptime.ColumnDataType_UINT64:
		col.Values.U64Values = append(col.Values.U64Values, val.(uint64))
	case greptime.ColumnDataType_FLOAT32:
		col.Values.F32Values = append(col.Values.F32Values, val.(float32))
	case greptime.ColumnDataType_FLOAT64:
		col.Values.F64Values = append(col.Values.F64Values, val.(float64))
	case greptime.ColumnDataType_BOOLEAN:
		col.Values.BoolValues = append(col.Values.BoolValues, val.(bool))
	case greptime.ColumnDataType_STRING:
		col.Values.StringValues = append(col.Values.StringValues, val.(string))
	case greptime.ColumnDataType_BINARY:
		col.Values.BinaryValues = append(col.Values.BinaryValues, val.([]byte))
	case greptime.ColumnDataType_TIMESTAMP_SECOND:
		col.Values.TsSecondValues = append(col.Values.TsSecondValues, val.(int64))
	case greptime.ColumnDataType_TIMESTAMP_MILLISECOND:
		col.Values.TsMillisecondValues = append(col.Values.TsMillisecondValues, val.(int64))
	case greptime.ColumnDataType_TIMESTAMP_MICROSECOND:
		col.Values.TsMicrosecondValues = append(col.Values.TsMicrosecondValues, val.(int64))
	case greptime.ColumnDataType_TIMESTAMP_NANOSECOND:
		col.Values.TsNanosecondValues = append(col.Values.TsNanosecondValues, val.(int64))
	default:
		return fmt.Errorf("unknown column data type: %v", col.Datatype)
	}
	return nil
}

func setNullMask(cols map[string]*greptime.Column, masks map[string]*Mask, size int) error {
	for name, mask := range masks {
		b, err := mask.shrink(size)
		if err != nil {
			return err
		}

		col, exist := cols[name]
		if !exist {
			return fmt.Errorf("%v column not found when set null mask", name)
		}
		col.NullMask = b
	}

	return nil
}
