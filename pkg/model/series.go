package model

import (
	"time"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type Series struct {
	Table     string
	Tags      []Tag
	Fields    []Field
	Timestamp *time.Time
}

type Tag struct {
	dataType greptime.ColumnDataType
	Key      string
	Value    any
}

type Field struct {
	dataType greptime.ColumnDataType
	Key      string
	Value    any
}

func (f *Field) GetDataType() greptime.ColumnDataType {
	return f.dataType
}

func (f *Field) SetDataType(dataType greptime.ColumnDataType) {
	f.dataType = dataType
}

func (t *Tag) GetDataType() greptime.ColumnDataType {
	return t.dataType
}

func (t *Tag) SetDataType(dataType greptime.ColumnDataType) {
	t.dataType = dataType
}
