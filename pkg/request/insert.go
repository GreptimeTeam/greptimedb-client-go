package request

import (
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type InsertRequest struct {
	Header
	Table string
	Data  []any
}

func (r *InsertRequest) WithTable(table string) *InsertRequest {
	r.Table = table
	return r
}

func (r *InsertRequest) WithData(data []any) *InsertRequest {
	r.Data = data
	return r
}

func (r *InsertRequest) IsTableEmpty() bool {
	return len(strings.TrimSpace(r.Table)) == 0
}

func (r *InsertRequest) Build() (*greptime.GreptimeRequest, error) {

	return nil, nil
}
