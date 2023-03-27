package request

import (
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

func (r *InsertRequest) RowCount() uint32 {
	return uint32(len(r.Metric.series))
}

func (r *InsertRequest) Build(cfg *Config) (*greptime.GreptimeRequest, error) {
	header, err := r.Header.buildRequestHeader(cfg)
	if err != nil {
		return nil, err
	}

	if IsEmptyString(r.Table) {
		return nil, ErrEmptyTable
	}

	columns, err := r.Metric.IntoGreptimeColumn()
	if err != nil {
		return nil, err
	}

	req := greptime.GreptimeRequest_Insert{
		Insert: &greptime.InsertRequest{
			TableName:    r.Table,
			Columns:      columns,
			RowCount:     r.RowCount(),
			RegionNumber: 0,
		},
	}
	return &greptime.GreptimeRequest{Header: header, Request: &req}, nil
}
