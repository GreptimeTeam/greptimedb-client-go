// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package greptime

import (
	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// QueryRequest helps to query data from greptimedb, and the response is in Metric.
// But if you expect the response format is the same as Prometheus, you should consider
// [PromqlRequest].
//
// At least one of Sql, InstantPromql, RangePromql MUST be spicified.
// The precedence takes places if multiple fields are specified:
//
//   - Sql
//   - InstantPromql (not implemented)
//   - RangePromql
type QueryRequest struct {
	header header

	sql           string
	instantPromql InstantPromql
	rangePromql   RangePromql
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

// WithInstantPromql is not implemented!
func (r *QueryRequest) WithInstantPromql(instantPromql InstantPromql) *QueryRequest {
	r.instantPromql = instantPromql
	return r
}

func (r *QueryRequest) WithRangePromql(rangePromql RangePromql) *QueryRequest {
	r.rangePromql = rangePromql
	return r
}

func (r *QueryRequest) check() error {
	if !isEmptyString(r.sql) {
		return nil
	}

	if !isEmptyString(r.instantPromql.Query) {
		return nil
	}

	return r.rangePromql.check()
}

func (r *QueryRequest) build(cfg *Config) (*greptimepb.GreptimeRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if err := r.check(); err != nil {
		return nil, err
	}

	request := &greptimepb.GreptimeRequest_Query{
		Query: &greptimepb.QueryRequest{},
	}

	if !isEmptyString(r.sql) {
		request.Query.Query = &greptimepb.QueryRequest_Sql{Sql: r.sql}
	} else if !isEmptyString(r.instantPromql.Query) {
		// TODO(yuanbohan): not implemented!
	} else {
		request.Query.Query = r.rangePromql.buildQueryRequest()
	}

	return &greptimepb.GreptimeRequest{
		Header:  header,
		Request: request,
	}, nil
}

// PromqlRequest helps to query data from greptimedb, and the response
// is the same as Prometheus
// At least one of InstantPromql, RangePromql MUST be spicified.
type PromqlRequest struct {
	header header

	instantPromql InstantPromql
	rangePromql   RangePromql
}

// WithDatabase helps to specify different database from the default one.
func (r *PromqlRequest) WithDatabase(database string) *PromqlRequest {
	r.header = header{
		database: database,
	}
	return r
}

func (r *PromqlRequest) WithInstantPromql(instantPromql InstantPromql) *PromqlRequest {
	r.instantPromql = instantPromql
	return r
}

func (r *PromqlRequest) WithRangePromql(rangePromql RangePromql) *PromqlRequest {
	r.rangePromql = rangePromql
	return r
}

func (r *PromqlRequest) buildInstantPromqlRequest(cfg *Config) (*greptimepb.PromqlRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if err := r.instantPromql.check(); err != nil {
		return nil, err
	}

	return &greptimepb.PromqlRequest{
		Header: header,
		Promql: r.instantPromql.buildPromqlRequest(),
	}, nil
}

func (r *PromqlRequest) buildRangePromqlRequest(cfg *Config) (*greptimepb.PromqlRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if err := r.rangePromql.check(); err != nil {
		return nil, err
	}

	return &greptimepb.PromqlRequest{
		Header: header,
		Promql: r.rangePromql.buildPromqlRequest(),
	}, nil
}
