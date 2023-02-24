package model

import (
	"time"
)

type Series struct {
	Table     string
	Tags      []Tag
	Fields    []Field
	Timestamp *time.Time
}

type Tag struct {
	// dataType greptime.ColumnDataType
	Key   string
	Value any
}

type Field struct {
	// dataType greptime.ColumnDataType
	Key   string
	Value any
}

type KeyValuePair interface {
	GetValue() any
	GetKey() string
}

func (f Field) GetValue() any {
	return f.Value
}

func (f Field) GetKey() string {
	return f.Key
}

func (t Tag) GetValue() any {
	return t.Value
}

func (t Tag) GetKey() string {
	return t.Key
}
