package request

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type value struct {
	val any
	typ greptime.ColumnDataType
}

func newValue(val any, typ greptime.ColumnDataType) *value {
	return &value{val, typ}
}

// TODO(yuanbohan): every greptime datatype MUST be covered in test cases
// TODO(yuanbohan): every pointer bisic type MUST be covered in switch
func convert(v any) (*value, error) {
	// TODO(vinland-avalon): check again if they are in proper mapping
	// if not have such a type, return it's string format
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
	default:
		return nil, fmt.Errorf("the type '%v' not supported", t)
	}
}

// convertUintToBytes using BigEndian
func convertUintToBytes(num uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, num)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// convertBytesToUint using BigEndian
func convertBytesToUint(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
