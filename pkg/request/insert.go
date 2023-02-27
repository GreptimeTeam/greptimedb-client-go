package request

import (
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
	if len(r.Database) == 0 {
		return nil, ErrEmptyDatabase
	}
	header := greptime.RequestHeader{Catalog: r.Catalog,
		Schema: r.Database}

	columns, err := r.Metric.IntoGreptimeColumn()
	if err != nil {
		return nil, err
	}

	if len(r.Table) == 0 {
		return nil, ErrEmptyTable
	} 
	req := greptime.GreptimeRequest_Insert{
		Insert: &greptime.InsertRequest{
			TableName:    r.Table,
			Columns:      columns,
			RowCount:     uint32(len(r.Metric.series)),
			RegionNumber: 0,
		}}

	return &greptime.GreptimeRequest{Header: &header, Request: &req}, nil
}
