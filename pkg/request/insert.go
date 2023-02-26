package request

import (
	"errors"
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type InsertRequest struct {
	Header
	Table  string
	Metric Metric
}

func (r *InsertRequest) WithTable(table string) *InsertRequest {
	r.Table = table
	return r
}

func (r *InsertRequest) WithMetric(metric Metric) *InsertRequest {
	r.Metric = metric
	return r
}

func (r *InsertRequest) IsTableEmpty() bool {
	return len(strings.TrimSpace(r.Table)) == 0
}

func (r *InsertRequest) Build() (*greptime.GreptimeRequest, error) {

	return nil, errors.New("not implemented")
}
