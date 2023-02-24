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
