package request

import (
	"fmt"
	"time"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// intoGreptimeDataType: convert` rawData` to `data` of greptime.ColumnDataType
func intoGreptimeDataType(v any) (any, error) {
	// TODO(vinland-avalon): check again if they are in proper mapping
	// if not have such a type, return it's string format
	switch v := v.(type) {
	case bool, string, float64, float32,
		uint64, uint32, uint16, uint8,
		int64, int32, int16, int8:
		return v, nil
	case []byte:
		return string(v), nil
	case uint:
		return uint64(v), nil
	case int:
		return int64(v), nil
	// TODO(vinland-avalon): convert with different precision
	case time.Time:
		return time.Time(v).UnixMilli(), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// intoGreptimeDataTypeEnum: extract `rawData`'s greptime DataTYpe
func intoGreptimeDataTypeEnum(v any) (greptime.ColumnDataType, error) {
	// TODO(vinland-avalon): check again if they are in proper mapping
	// if not have such a type, return it's string format
	switch v.(type) {
	case bool:
		return greptime.ColumnDataType_BOOLEAN, nil
	case string:
		return greptime.ColumnDataType_STRING, nil
	case []byte:
		return greptime.ColumnDataType_STRING, nil
	case float64:
		return greptime.ColumnDataType_FLOAT64, nil
	case float32:
		return greptime.ColumnDataType_FLOAT32, nil
	case uint:
		return greptime.ColumnDataType_UINT64, nil
	case uint64:
		return greptime.ColumnDataType_UINT64, nil
	case uint32:
		return greptime.ColumnDataType_UINT32, nil
	case uint16:
		return greptime.ColumnDataType_UINT16, nil
	case uint8:
		return greptime.ColumnDataType_UINT8, nil
	case int:
		return greptime.ColumnDataType_INT64, nil
	case int64:
		return greptime.ColumnDataType_INT64, nil
	case int32:
		return greptime.ColumnDataType_INT32, nil
	case int16:
		return greptime.ColumnDataType_INT16, nil
	case int8:
		return greptime.ColumnDataType_INT8, nil
	// TODO(vinland-avalon): convert with different precision
	case time.Time:
		return greptime.ColumnDataType_TIMESTAMP_MILLISECOND, nil
	default:
		return greptime.ColumnDataType_STRING, nil
	}
}
