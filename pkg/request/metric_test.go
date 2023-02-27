package request

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func TestSeries(t *testing.T) {
	s := Series{}
	s.AddTag("tag1", "tag val")
	s.AddTag("tag2", true)
	s.AddTag("tag3", int32(32))
	s.AddTag("tag4", float64(32.0))
	timestamp := time.Now()
	s.SetTimeWithKey("timestamp", timestamp)
	s.AddField("field1", []byte("field val"))
	s.AddField("field2", float32(32.0))
	s.AddField("field3", uint8(8))
	s.AddField("field4", uint64(64))

	// check columns
	assert.Equal(t, 8, len(s.columns))
	assert.Equal(t, greptime.ColumnDataType_STRING, s.columns["tag1"].typ)
	assert.Equal(t, greptime.Column_TAG, s.columns["tag1"].semantic)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, s.columns["tag2"].typ)
	assert.Equal(t, greptime.Column_TAG, s.columns["tag2"].semantic)
	assert.Equal(t, greptime.ColumnDataType_INT32, s.columns["tag3"].typ)
	assert.Equal(t, greptime.Column_TAG, s.columns["tag3"].semantic)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, s.columns["tag4"].typ)
	assert.Equal(t, greptime.Column_TAG, s.columns["tag4"].semantic)
	assert.Equal(t, greptime.ColumnDataType_STRING, s.columns["field1"].typ)
	assert.Equal(t, greptime.Column_FIELD, s.columns["field1"].semantic)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, s.columns["field2"].typ)
	assert.Equal(t, greptime.Column_FIELD, s.columns["field2"].semantic)
	assert.Equal(t, greptime.ColumnDataType_UINT32, s.columns["field3"].typ)
	assert.Equal(t, greptime.Column_FIELD, s.columns["field3"].semantic)
	assert.Equal(t, greptime.ColumnDataType_UINT64, s.columns["field4"].typ)
	assert.Equal(t, greptime.Column_FIELD, s.columns["field4"].semantic)

	// check values
	assert.Equal(t, 8, len(s.vals))
	assert.Equal(t, "tag val", s.vals["tag1"])
	assert.Equal(t, true, s.vals["tag2"])
	assert.Equal(t, int32(32), s.vals["tag3"])
	assert.Equal(t, float64(32.0), s.vals["tag4"])
	assert.Equal(t, "field val", s.vals["field1"])
	assert.Equal(t, float64(32.0), s.vals["field2"])
	assert.Equal(t, uint32(8), s.vals["field3"])
	assert.Equal(t, uint64(64), s.vals["field4"])

	// check timestamp
	assert.Equal(t, timestamp, s.timestamp)
	assert.Equal(t, "timestamp", s.timestampAlias)
}

func TestValueReplaced(t *testing.T) {
	s := Series{}
	val := "tag val"
	err := s.AddTag("tag1", val)
	assert.Nil(t, err)
	assert.Equal(t, val, s.vals["tag1"])

	newVal := "tag val again"
	err = s.AddTag("tag1", newVal)
	assert.Nil(t, err)
	assert.Equal(t, newVal, s.vals["tag1"])
}

func TestSeriesError(t *testing.T) {
	s := Series{}

	// type not match
	err := s.AddTag("tag1", "tag val")
	assert.Nil(t, err)
	err = s.AddTag("tag1", true)
	assert.NotNil(t, err)

	// tag and field contain same column
	err = s.AddTag("name", "tag val")
	assert.Nil(t, err)
	err = s.AddField("name", "field val")
	assert.NotNil(t, err)
}

func TestMetric(t *testing.T) {
	s := Series{}
	s.AddTag("tag1", "tag val")
	s.AddTag("tag2", true)
	s.AddTag("tag3", int32(32))
	s.AddTag("tag4", float64(32.0))
	s.AddField("field1", []byte("field val"))
	s.AddField("field2", float32(32.0))
	s.AddField("field3", uint8(8))
	s.AddField("field4", uint64(64))
	s.SetTime(time.Now())

	m := Metric{}
	err := m.AddSeries(s)
	assert.Nil(t, err)
}

func TestMetricTypeNotMatch(t *testing.T) {
	s1 := Series{}
	s1.AddTag("tag", "tag val")
	s1.SetTime(time.Now())

	s2 := Series{}
	s2.AddTag("tag", true)
	s2.SetTime(time.Now())

	m := Metric{}
	err := m.AddSeries(s1)
	assert.Nil(t, err)

	err = m.AddSeries(s2)
	assert.NotNil(t, err)
}

func TestMetricSemanticNotMatch(t *testing.T) {
	s1 := Series{}
	s1.AddTag("name", "tag val")
	s1.SetTime(time.Now())

	s2 := Series{}
	s2.AddField("name", true)
	s2.SetTime(time.Now())

	m := Metric{}
	err := m.AddSeries(s1)
	assert.Nil(t, err)

	err = m.AddSeries(s2)
	assert.NotNil(t, err)
}

// 8 columns
// row1: 4 tags, 2 fields (with 2 null column)
// row2: 2 tags, 4 fields (with 2 null column)
func TestGreptimeColumn(t *testing.T) {
	timestamp := time.Now()

	s1 := Series{}
	s1.AddTag("tag1", "tag1")
	s1.AddTag("tag2", true)
	s1.AddTag("tag3", int32(32))
	s1.AddTag("tag4", float64(32.0))
	s1.AddField("field1", uint8(8))
	s1.AddField("field2", uint64(64))
	s1.SetTime(timestamp)

	s2 := Series{}
	s2.AddTag("tag1", "tag2")
	s2.AddTag("tag2", false)
	s2.AddField("field1", uint8(8))
	s2.AddField("field2", uint64(64))
	s2.AddField("field3", []byte("field3"))
	s2.AddField("field4", float32(32.0))
	s2.SetTime(timestamp)

	m := Metric{}
	assert.Nil(t, m.AddSeries(s1))
	assert.Nil(t, m.AddSeries(s2))

	cols, err := m.IntoGreptimeColumn()
	assert.Nil(t, err)
	assert.Equal(t, 9, len(cols))

	col1 := cols[0]
	assert.Equal(t, "tag1", col1.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_STRING, col1.Datatype)
	assert.Equal(t, greptime.Column_TAG, col1.SemanticType)
	assert.Equal(t, []string{"tag1", "tag2"}, col1.Values.StringValues)
	assert.Empty(t, col1.NullMask)

	col2 := cols[1]
	assert.Equal(t, "tag2", col2.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_BOOLEAN, col2.Datatype)
	assert.Equal(t, greptime.Column_TAG, col2.SemanticType)
	assert.Equal(t, []bool{true, false}, col2.Values.BoolValues)
	assert.Empty(t, col2.NullMask)

	col3 := cols[2]
	assert.Equal(t, "tag3", col3.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_INT32, col3.Datatype)
	assert.Equal(t, greptime.Column_TAG, col3.SemanticType)
	assert.Equal(t, []int32{32}, col3.Values.I32Values)
	assert.Equal(t, []byte{2}, col3.NullMask)

	col4 := cols[3]
	assert.Equal(t, "tag4", col4.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, col4.Datatype)
	assert.Equal(t, greptime.Column_TAG, col4.SemanticType)
	assert.Equal(t, []float64{32}, col4.Values.F64Values)
	assert.Equal(t, []byte{2}, col4.NullMask)

	col5 := cols[4]
	assert.Equal(t, "field1", col5.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_UINT32, col5.Datatype)
	assert.Equal(t, greptime.Column_FIELD, col5.SemanticType)
	assert.Equal(t, []uint32{8, 8}, col5.Values.U32Values)
	assert.Empty(t, col5.NullMask)

	col6 := cols[5]
	assert.Equal(t, "field2", col6.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_UINT64, col6.Datatype)
	assert.Equal(t, greptime.Column_FIELD, col6.SemanticType)
	assert.Equal(t, []uint64{64, 64}, col6.Values.U64Values)
	assert.Empty(t, col6.NullMask)

	col7 := cols[6]
	assert.Equal(t, "field3", col7.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_STRING, col7.Datatype)
	assert.Equal(t, greptime.Column_FIELD, col7.SemanticType)
	assert.Equal(t, []string{"field3"}, col7.Values.StringValues)
	assert.Equal(t, []byte{1}, col7.NullMask)

	col8 := cols[7]
	assert.Equal(t, "field4", col8.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_FLOAT64, col8.Datatype)
	assert.Equal(t, greptime.Column_FIELD, col8.SemanticType)
	assert.Equal(t, []float64{32}, col8.Values.F64Values)
	assert.Equal(t, []byte{1}, col8.NullMask)

	col9 := cols[8]
	assert.Equal(t, "ts", col9.ColumnName)
	assert.Equal(t, greptime.ColumnDataType_TIMESTAMP_MILLISECOND, col9.Datatype)
	assert.Equal(t, greptime.Column_TIMESTAMP, col9.SemanticType)
	assert.Equal(t, []int64{timestamp.UnixMilli(), timestamp.UnixMilli()}, col9.Values.TsMillisecondValues)
	assert.Equal(t, []byte{1}, col8.NullMask)
}
