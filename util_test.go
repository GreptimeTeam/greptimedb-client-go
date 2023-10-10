// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package greptime

import (
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
	assert.Equal(t, []byte("bytes"), val.val)
	assert.Equal(t, greptime.ColumnDataType_BINARY, val.typ)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = convert(expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// float32
	var expectFloat32 float32 = float32(32.0)
	val, err = convert(expectFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT32, val.typ)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = convert(originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint64
	var expectUint64 uint64 = uint64(64)
	val, err = convert(expectUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint32
	var expectUint32 uint32 = uint32(32)
	val, err = convert(expectUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint16
	var expectUint16 uint16 = uint16(16)
	val, err = convert(expectUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT16, val.typ)

	// uint8
	var expectUint8 uint8 = uint8(8)
	val, err = convert(expectUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT8, val.typ)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = convert(originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int64
	var expectInt64 int64 = int64(64)
	val, err = convert(expectInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int32
	var expectInt32 int32 = int32(32)
	val, err = convert(expectInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int16
	var expectInt16 int16 = int16(16)
	val, err = convert(expectInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT16, val.typ)

	// int8
	var expectInt8 int8 = int8(8)
	val, err = convert(expectInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT8, val.typ)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	// var expectTime int64 = int64(1677571339623)
	val, err = convert(originTime)
	assert.Nil(t, err)
	assert.Equal(t, originTime, val.val)
	assert.Equal(t, greptime.ColumnDataType_DATETIME, val.typ)

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
	assert.Equal(t, []byte("bytes"), val.val)
	assert.Equal(t, greptime.ColumnDataType_BINARY, val.typ)

	// float64
	var expectFloat64 float64 = float64(64.0)
	val, err = convert(&expectFloat64)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat64, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, val.typ)

	// float32
	var expectFloat32 float32 = float32(32.0)
	val, err = convert(&expectFloat32)
	assert.Nil(t, err)
	assert.Equal(t, expectFloat32, val.val)
	assert.Equal(t, greptime.ColumnDataType_FLOAT32, val.typ)

	// uint
	var originUint uint = uint(64)
	var expectUint uint64 = uint64(64)
	val, err = convert(&originUint)
	assert.Nil(t, err)
	assert.Equal(t, expectUint, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint64
	var expectUint64 uint64 = uint64(64)
	val, err = convert(&expectUint64)
	assert.Nil(t, err)
	assert.Equal(t, expectUint64, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT64, val.typ)

	// uint32
	var expectUint32 uint32 = uint32(32)
	val, err = convert(&expectUint32)
	assert.Nil(t, err)
	assert.Equal(t, expectUint32, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT32, val.typ)

	// uint16
	var expectUint16 uint16 = uint16(16)
	val, err = convert(&expectUint16)
	assert.Nil(t, err)
	assert.Equal(t, expectUint16, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT16, val.typ)

	// uint8
	var expectUint8 uint8 = uint8(8)
	val, err = convert(&expectUint8)
	assert.Nil(t, err)
	assert.Equal(t, expectUint8, val.val)
	assert.Equal(t, greptime.ColumnDataType_UINT8, val.typ)

	// int
	var originInt int = int(64)
	var expectInt int64 = int64(64)
	val, err = convert(&originInt)
	assert.Nil(t, err)
	assert.Equal(t, expectInt, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int64
	var expectInt64 int64 = int64(64)
	val, err = convert(&expectInt64)
	assert.Nil(t, err)
	assert.Equal(t, expectInt64, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT64, val.typ)

	// int32
	var expectInt32 int32 = int32(32)
	val, err = convert(&expectInt32)
	assert.Nil(t, err)
	assert.Equal(t, expectInt32, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT32, val.typ)

	// int16
	var expectInt16 int16 = int16(16)
	val, err = convert(&expectInt16)
	assert.Nil(t, err)
	assert.Equal(t, expectInt16, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT16, val.typ)

	// int8
	var expectInt8 int8 = int8(8)
	val, err = convert(&expectInt8)
	assert.Nil(t, err)
	assert.Equal(t, expectInt8, val.val)
	assert.Equal(t, greptime.ColumnDataType_INT8, val.typ)

	// time.Time
	var originTime time.Time = time.UnixMilli(1677571339623)
	// var expectTime int64 = int64(1677571339623)
	val, err = convert(&originTime)
	assert.Nil(t, err)
	assert.Equal(t, originTime, val.val)
	assert.Equal(t, greptime.ColumnDataType_DATETIME, val.typ)

	// type not supported
	_, err = convert(&map[string]any{})
	assert.NotNil(t, err)
}

func TestEmptyString(t *testing.T) {
	assert.True(t, isEmptyString(""))
	assert.True(t, isEmptyString(" "))
	assert.True(t, isEmptyString("  "))
	assert.True(t, isEmptyString("\t"))
}

func TestColumnName(t *testing.T) {
	key, err := toColumnName("ts ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = toColumnName(" Ts")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = toColumnName(" TS ")
	assert.Nil(t, err)
	assert.Equal(t, "ts", key)

	key, err = toColumnName("DiskUsage ")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = toColumnName("Disk-Usage")
	assert.Nil(t, err)
	assert.Equal(t, "disk_usage", key)

	key, err = toColumnName("   ")
	assert.NotNil(t, err)
	assert.Equal(t, "", key)

	key, err = toColumnName(strings.Repeat("timestamp", 20))
	assert.NotNil(t, err)
	assert.Equal(t, "", key)
}

func TestPrecisionToDataType(t *testing.T) {
	_, err := precisionToDataType(123)
	assert.Equal(t, ErrInvalidTimePrecision, err)
}
