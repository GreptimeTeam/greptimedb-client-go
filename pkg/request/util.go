package request

import (
	"fmt"
	"strings"
	"time"

	strcase "github.com/stoewer/go-strcase"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type value struct {
	val any
	typ greptime.ColumnDataType
}

func newValue(val any, typ greptime.ColumnDataType) *value {
	return &value{val, typ}
}

func convert(v any) (*value, error) {
	switch t := v.(type) {
	case bool:
		return newValue(t, greptime.ColumnDataType_BOOLEAN), nil
	case string:
		return newValue(t, greptime.ColumnDataType_STRING), nil
	case []byte:
		return newValue(string(t), greptime.ColumnDataType_STRING), nil
	case float64:
		return newValue(t, greptime.ColumnDataType_FLOAT64), nil
	case float32:
		return newValue(float64(t), greptime.ColumnDataType_FLOAT64), nil
	case uint:
		return newValue(uint64(t), greptime.ColumnDataType_UINT64), nil
	case uint64:
		return newValue(t, greptime.ColumnDataType_UINT64), nil
	case uint32:
		return newValue(uint32(t), greptime.ColumnDataType_UINT32), nil
	case uint16:
		return newValue(uint32(t), greptime.ColumnDataType_UINT32), nil
	case uint8:
		return newValue(uint32(t), greptime.ColumnDataType_UINT32), nil
	case int:
		return newValue(int64(t), greptime.ColumnDataType_INT64), nil
	case int64:
		return newValue(t, greptime.ColumnDataType_INT64), nil
	case int32:
		return newValue(int32(t), greptime.ColumnDataType_INT32), nil
	case int16:
		return newValue(int32(t), greptime.ColumnDataType_INT32), nil
	case int8:
		return newValue(int32(t), greptime.ColumnDataType_INT32), nil
	// TODO(vinland-avalon): convert with different precision
	case time.Time:
		return newValue(t.UnixMilli(), greptime.ColumnDataType_TIMESTAMP_MILLISECOND), nil
	case *bool:
		return newValue(*t, greptime.ColumnDataType_BOOLEAN), nil
	case *string:
		return newValue(*t, greptime.ColumnDataType_STRING), nil
	case *[]byte:
		return newValue(string(*t), greptime.ColumnDataType_STRING), nil
	case *float64:
		return newValue(*t, greptime.ColumnDataType_FLOAT64), nil
	case *float32:
		return newValue(float64(*t), greptime.ColumnDataType_FLOAT64), nil
	case *uint:
		return newValue(uint64(*t), greptime.ColumnDataType_UINT64), nil
	case *uint64:
		return newValue(*t, greptime.ColumnDataType_UINT64), nil
	case *uint32:
		return newValue(uint32(*t), greptime.ColumnDataType_UINT32), nil
	case *uint16:
		return newValue(uint32(*t), greptime.ColumnDataType_UINT32), nil
	case *uint8:
		return newValue(uint32(*t), greptime.ColumnDataType_UINT32), nil
	case *int:
		return newValue(int64(*t), greptime.ColumnDataType_INT64), nil
	case *int64:
		return newValue(*t, greptime.ColumnDataType_INT64), nil
	case *int32:
		return newValue(int32(*t), greptime.ColumnDataType_INT32), nil
	case *int16:
		return newValue(int32(*t), greptime.ColumnDataType_INT32), nil
	case *int8:
		return newValue(int32(*t), greptime.ColumnDataType_INT32), nil
	// TODO(vinland-avalon): convert with different precision, as `time.Time` abovementioned
	case *time.Time:
		return newValue(t.UnixMilli(), greptime.ColumnDataType_TIMESTAMP_MILLISECOND), nil
	default:
		return nil, fmt.Errorf("the type '%v' not supported", t)
	}
}

func IsTimePrecisionValid(t time.Duration) bool {
	switch t {
	case time.Second, time.Millisecond, time.Microsecond, time.Nanosecond:
		return true
	default:
		return false
	}
}

func precisionToDataType(d time.Duration) (greptime.ColumnDataType, error) {
	// if the precision has not been set, use defalut precision `time.Millisecond`
	if d == 0 {
		d = time.Millisecond
	}
	switch d {
	case time.Second:
		return greptime.ColumnDataType_TIMESTAMP_SECOND, nil
	case time.Millisecond:
		return greptime.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case time.Microsecond:
		return greptime.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case time.Nanosecond:
		return greptime.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	default:
		return 0, ErrInvalidTimePrecision
	}
}

func IsEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func ToColumnName(s string) (string, error) {
	s = strings.TrimSpace(s)

	if len(s) == 0 {
		return "", ErrEmptyKey
	}

	if len(s) >= 100 {
		return "", fmt.Errorf("the length of name CAN NOT be longer than 100. %s", s)
	}

	return strings.ToLower(strcase.SnakeCase(s)), nil
}
