package request

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func TestConvertValue(t *testing.T) {
	// bool
	var expectBool bool = true
	val, err := convert(expectBool)
	assert.Nil(t, err)
	assert.Equal(t, expectBool, val.val)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, val.typ)

	// string
	var expectString string = "string"
	val, err = convert(expectString)
	assert.Nil(t, err)
	assert.Equal(t, expectString, val.val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.typ)

	// bytes
	var expectBytes []byte = []byte("bytes")
	val, err = convert(expectBytes)
	assert.Nil(t, err)
	assert.Equal(t, "bytes", val.val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.typ)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = convert(expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// float32
	var originFloat32 float32 = float32(32.0)
	var expectFloat32 float64 = float64(32.0)
	val, err = convert(originFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = convert(originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint64
	var originUint64 uint64 = uint64(64)
	var expectUint64 uint64 = uint64(64)
	val, err = convert(originUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint32
	var originUint32 uint32 = uint32(32)
	var expectUint32 uint32 = uint32(32)
	val, err = convert(originUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint16
	var originUint16 uint16 = uint16(16)
	var expectUint16 uint32 = uint32(16)
	val, err = convert(originUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint8
	var originUint8 uint8 = uint8(8)
	var expectUint8 uint32 = uint32(8)
	val, err = convert(originUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = convert(originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int64
	var originInt64 int64 = int64(64)
	var expectInt64 int64 = int64(64)
	val, err = convert(originInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int32
	var originInt32 int32 = int32(32)
	var expectInt32 int32 = int32(32)
	val, err = convert(originInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int16
	var originInt16 int16 = int16(16)
	var expectInt16 int32 = int32(16)
	val, err = convert(originInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int8
	var originInt8 int8 = int8(8)
	var expectInt8 int32 = int32(8)
	val, err = convert(originInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	var expectTime int64 = int64(1677571339623)
	val, err = convert(originTime)
	assert.Nil(t, err)
	assert.Equal(t, expectTime, val.val)
	assert.Equal(t, greptime.ColumnDataType_TIMESTAMP_MILLISECOND, val.typ)

	// type not supported
	_, err = convert(time.April)
	assert.NotNil(t, err)
	_, err = convert(map[string]any{})
	assert.NotNil(t, err)
	_, err = convert(func() {})
	assert.NotNil(t, err)

}

func TestConvertValuePtr(t *testing.T) {
	// bool
	var expectBool bool = true
	val, err := convert(&expectBool)
	assert.Nil(t, err)
	assert.Equal(t, expectBool, val.val)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, val.typ)

	// string
	var expectString string = "string"
	val, err = convert(&expectString)
	assert.Nil(t, err)
	assert.Equal(t, expectString, val.val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.typ)

	// bytes
	var expectBytes []byte = []byte("bytes")
	val, err = convert(&expectBytes)
	assert.Nil(t, err)
	assert.Equal(t, "bytes", val.val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.typ)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = convert(&expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// float32
	var originFloat32 float32 = float32(32.0)
	var expectFloat32 float64 = float64(32.0)
	val, err = convert(&originFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = convert(&originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint64
	var originUint64 uint64 = uint64(64)
	var expectUint64 uint64 = uint64(64)
	val, err = convert(&originUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint32
	var originUint32 uint32 = uint32(32)
	var expectUint32 uint32 = uint32(32)
	val, err = convert(&originUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint16
	var originUint16 uint16 = uint16(16)
	var expectUint16 uint32 = uint32(16)
	val, err = convert(&originUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint8
	var originUint8 uint8 = uint8(8)
	var expectUint8 uint32 = uint32(8)
	val, err = convert(&originUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = convert(&originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int64
	var originInt64 int64 = int64(64)
	var expectInt64 int64 = int64(64)
	val, err = convert(&originInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int32
	var originInt32 int32 = int32(32)
	var expectInt32 int32 = int32(32)
	val, err = convert(&originInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int16
	var originInt16 int16 = int16(16)
	var expectInt16 int32 = int32(16)
	val, err = convert(&originInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int8
	var originInt8 int8 = int8(8)
	var expectInt8 int32 = int32(8)
	val, err = convert(&originInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	var expectTime int64 = int64(1677571339623)
	val, err = convert(&originTime)
	assert.Nil(t, err)
	assert.Equal(t, expectTime, val.val)
	assert.Equal(t, greptime.ColumnDataType_TIMESTAMP_MILLISECOND, val.typ)

	// type not supported
	_, err = convert(&map[string]any{})
	assert.NotNil(t, err)
}

func TestEmptyString(t *testing.T) {
	assert.True(t, IsEmptyString(""))
	assert.True(t, IsEmptyString(" "))
	assert.True(t, IsEmptyString("  "))
	assert.True(t, IsEmptyString("\t"))
}

func TestColumnName(t *testing.T) {
	key, err := ToColumnName("ts ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" Ts")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName(" TS ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = ToColumnName("DiskUsage ")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("Disk-Usage")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = ToColumnName("   ")
	assert.NotNil(t, err)
	assert.Equal(t, "", key)

	key, err = ToColumnName(strings.Repeat("timestamp", 20))
	assert.NotNil(t, err)
	assert.Equal(t, "", key)
}

func TestExtractFieldName(t *testing.T) {
	name := fieldName(reflect.TypeOf(Person{}).Field(0))
	assert.Equal(t, "name", name)
}
