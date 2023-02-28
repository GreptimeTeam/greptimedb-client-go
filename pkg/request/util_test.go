package request

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
	assert.Equal(t, "bytes", val.val)
	assert.Equal(t, greptime.ColumnDataType_STRING, val.typ)

	//
	// TODO(yuanbohan): all possible type
	//

	// type not supported
	// TODO(yuanbohan): more unsupported types
	_, err = convert(time.April)
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
