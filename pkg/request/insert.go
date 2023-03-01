package request

import (
	"fmt"
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

func (r *InsertRequest) RowCount() uint32 {
	return uint32(len(r.Metric.series))
}

func (r *InsertRequest) Build() (*greptime.GreptimeRequest, error) {
	if r.IsDatabaseEmpty() {
		return nil, ErrEmptyDatabase
	}
	header := greptime.RequestHeader{
		Catalog: r.Catalog,
		Schema:  r.Database,
	}

	columns, err := r.Metric.IntoGreptimeColumn()
	if err != nil {
		return nil, err
	}

	if r.IsTableEmpty() {
		return nil, ErrEmptyTable
	}
	req := greptime.GreptimeRequest_Insert{
		Insert: &greptime.InsertRequest{
			TableName:    r.Table,
			Columns:      columns,
			RowCount:     r.RowCount(),
			RegionNumber: 0,
		}}
	greptimeRequest := greptime.GreptimeRequest{Header: &header, Request: &req}
	fmt.Printf("greptime.GreptimeRequest: %+v\n", greptimeRequest)
	fmt.Printf("columns in greptime.GreptimeRequest: %+v\n", req)
	return &greptimeRequest, nil
}
