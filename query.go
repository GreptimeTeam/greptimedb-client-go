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

type query interface {
	// buildGreptimeRequest helps to construct a normal request, and the response
	// is in Metric and Series
	buildGreptimeRequest(header *greptimepb.RequestHeader) (*greptimepb.GreptimeRequest, error)

	// buildPromqlRequest helps to construct a promql request, and the response
	// is absolutely the same as Prometheus
	buildPromqlRequest(header *greptimepb.RequestHeader) (*greptimepb.PromqlRequest, error)
}

// QueryRequest helps to query data from greptimedb, and the response is in Metric.
// But if you expect the response format is the same as Prometheus, you should consider
// [PromqlRequest].
//
// At least one of Sql, InstantPromql, RangePromql MUST be spicified.
// If multiple fields are specified, the field specified later will be used
type QueryRequest struct {
	header header
	query  query
}

// WithDatabase helps to specify different database from the default one.
func (r *QueryRequest) WithDatabase(database string) *QueryRequest {
	r.header = header{
		database: database,
	}
	return r
}

func (r *QueryRequest) WithSql(sql string) *QueryRequest {
	r.query = &Sql{sql: sql}
	return r
}

func (r *QueryRequest) WithInstantPromql(instantPromql *InstantPromql) *QueryRequest {
	r.query = instantPromql
	return r
}

func (r *QueryRequest) WithRangePromql(rangePromql *RangePromql) *QueryRequest {
	r.query = rangePromql
	return r
}

func (r *QueryRequest) buildGreptimeRequest(cfg *Config) (*greptimepb.GreptimeRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if r.query == nil {
		return nil, ErrEmptyQuery
	}

	return r.query.buildGreptimeRequest(header)
}

func (r *QueryRequest) buildPromqlRequest(cfg *Config) (*greptimepb.PromqlRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if r.query == nil {
		return nil, ErrEmptyQuery
	}

	return r.query.buildPromqlRequest(header)
}
