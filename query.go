package greptime

import (
	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type QueryRequest struct {
	header header
	sql    string

	// Promql is not supported yet
	promql      string
	rangePromql RangePromql
}

func (r *QueryRequest) WithDatabase(database string) *QueryRequest {
	r.header = header{
		database: database,
	}
	return r
}

func (r *QueryRequest) WithSql(sql string) *QueryRequest {
	r.sql = sql
	return r
}

func (r *QueryRequest) WithRangePromql(rangePromql RangePromql) *QueryRequest {
	r.rangePromql = rangePromql
	return r
}

func (r *QueryRequest) isSqlEmpty() bool {
	return IsEmptyString(r.sql)
}

func (r *QueryRequest) check() error {
	if r.isSqlEmpty() {
		return r.rangePromql.check()
	}
	return nil
}

func (r *QueryRequest) Build(cfg *Config) (*greptimepb.GreptimeRequest, error) {
	header, err := r.header.Build(cfg)
	if err != nil {
		return nil, err
	}

	if err := r.check(); err != nil {
		return nil, err
	}

	request := &greptimepb.GreptimeRequest_Query{
		Query: &greptimepb.QueryRequest{},
	}

	if !r.isSqlEmpty() {
		request.Query.Query = &greptimepb.QueryRequest_Sql{Sql: r.sql}
	} else {
		request.Query.Query = r.rangePromql.Build()
	}

	return &greptimepb.GreptimeRequest{
		Header:  header,
		Request: request,
	}, nil
}
