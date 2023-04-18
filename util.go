package greptime

import (
	"fmt"
	"strings"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stoewer/go-strcase"
)

type value struct {
	val any
	typ greptimepb.ColumnDataType
}

func newValue(val any, typ greptimepb.ColumnDataType) *value {
	return &value{val, typ}
}

func convert(v any) (*value, error) {
	switch t := v.(type) {
	case bool:
		return newValue(t, greptimepb.ColumnDataType_BOOLEAN), nil
	case string:
		return newValue(t, greptimepb.ColumnDataType_STRING), nil
	case []byte:
		return newValue(string(t), greptimepb.ColumnDataType_STRING), nil
	case float64:
		return newValue(t, greptimepb.ColumnDataType_FLOAT64), nil
	case float32:
		return newValue(float64(t), greptimepb.ColumnDataType_FLOAT64), nil
	case uint:
		return newValue(uint64(t), greptimepb.ColumnDataType_UINT64), nil
	case uint64:
		return newValue(t, greptimepb.ColumnDataType_UINT64), nil
	case uint32:
		return newValue(uint32(t), greptimepb.ColumnDataType_UINT32), nil
	case uint16:
		return newValue(uint32(t), greptimepb.ColumnDataType_UINT32), nil
	case uint8:
		return newValue(uint32(t), greptimepb.ColumnDataType_UINT32), nil
	case int:
		return newValue(int64(t), greptimepb.ColumnDataType_INT64), nil
	case int64:
		return newValue(t, greptimepb.ColumnDataType_INT64), nil
	case int32:
		return newValue(int32(t), greptimepb.ColumnDataType_INT32), nil
	case int16:
		return newValue(int32(t), greptimepb.ColumnDataType_INT32), nil
	case int8:
		return newValue(int32(t), greptimepb.ColumnDataType_INT32), nil
	// TODO(vinland-avalon): convert with different precision
	case time.Time:
		return newValue(t.UnixMilli(), greptimepb.ColumnDataType_TIMESTAMP_MILLISECOND), nil

	case *bool:
		return newValue(*t, greptimepb.ColumnDataType_BOOLEAN), nil
	case *string:
		return newValue(*t, greptimepb.ColumnDataType_STRING), nil
	case *[]byte:
		return newValue(string(*t), greptimepb.ColumnDataType_STRING), nil
	case *float64:
		return newValue(*t, greptimepb.ColumnDataType_FLOAT64), nil
	case *float32:
		return newValue(float64(*t), greptimepb.ColumnDataType_FLOAT64), nil
	case *uint:
		return newValue(uint64(*t), greptimepb.ColumnDataType_UINT64), nil
	case *uint64:
		return newValue(*t, greptimepb.ColumnDataType_UINT64), nil
	case *uint32:
		return newValue(uint32(*t), greptimepb.ColumnDataType_UINT32), nil
	case *uint16:
		return newValue(uint32(*t), greptimepb.ColumnDataType_UINT32), nil
	case *uint8:
		return newValue(uint32(*t), greptimepb.ColumnDataType_UINT32), nil
	case *int:
		return newValue(int64(*t), greptimepb.ColumnDataType_INT64), nil
	case *int64:
		return newValue(*t, greptimepb.ColumnDataType_INT64), nil
	case *int32:
		return newValue(int32(*t), greptimepb.ColumnDataType_INT32), nil
	case *int16:
		return newValue(int32(*t), greptimepb.ColumnDataType_INT32), nil
	case *int8:
		return newValue(int32(*t), greptimepb.ColumnDataType_INT32), nil
	// TODO(vinland-avalon): convert with different precision, as `time.Time` abovementioned
	case *time.Time:
		return newValue(t.UnixMilli(), greptimepb.ColumnDataType_TIMESTAMP_MILLISECOND), nil
	default:
		return nil, fmt.Errorf("the type '%T' is not supported", t)
	}
}

func isValidPrecision(t time.Duration) bool {
	return t == time.Second ||
		t == time.Millisecond ||
		t == time.Microsecond ||
		t == time.Nanosecond
}

func precisionToDataType(d time.Duration) (greptimepb.ColumnDataType, error) {
	// if the precision has not been set, use defalut precision `time.Millisecond`
	if d == 0 {
		d = time.Millisecond
	}
	switch d {
	case time.Second:
		return greptimepb.ColumnDataType_TIMESTAMP_SECOND, nil
	case time.Millisecond:
		return greptimepb.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	case time.Microsecond:
		return greptimepb.ColumnDataType_TIMESTAMP_MICROSECOND, nil
	case time.Nanosecond:
		return greptimepb.ColumnDataType_TIMESTAMP_NANOSECOND, nil
	default:
		return 0, ErrInvalidTimePrecision
	}
}

func isEmptyString(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

func toColumnName(s string) (string, error) {
	s = strings.TrimSpace(s)

	if len(s) == 0 {
		return "", ErrEmptyKey
	}

	if len(s) >= 100 {
		return "", fmt.Errorf("the length of column name CAN NOT be longer than 100. %s", s)
	}

	return strings.ToLower(strcase.SnakeCase(s)), nil
}
