package greptime

import (
	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// QueryRequest helps to query data from greptimedb.
// At least one of Sql, Promql, RangePromql MUST be spicified.
// The precedence takes places if multiple fields are specified:
// - Sql
// - Promql
// - RangePromql
type QueryRequest struct {
	header      header
	sql         string
	promql      string // promql is not supported yet
	rangePromql RangePromql
}

// WithDatabase helps to specify different database from the default one.
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

// WithPromql is not supported yet
func (r *QueryRequest) WithPromql(promql string) *QueryRequest {
	r.promql = promql
	return r
}

func (r *QueryRequest) WithRangePromql(rangePromql RangePromql) *QueryRequest {
	r.rangePromql = rangePromql
	return r
}

func (r *QueryRequest) isSqlEmpty() bool {
	return isEmptyString(r.sql)
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
