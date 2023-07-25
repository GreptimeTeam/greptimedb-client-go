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

type InsertsRequest struct {
	header  reqHeader
	inserts []InsertRequest
}

// WithDatabase helps to specify different database from the default one.
func (r *InsertsRequest) WithDatabase(database string) *InsertsRequest {
	r.header = reqHeader{
		database: database,
	}
	return r
}

// Append will include one insert into this InsertsRequest
func (r *InsertsRequest) Append(insert InsertRequest) *InsertsRequest {
	if r.inserts == nil {
		r.inserts = make([]InsertRequest, 0)
	}

	r.inserts = append(r.inserts, insert)

	return r
}

func (r InsertsRequest) build(cfg *Config) (*greptimepb.GreptimeRequest, error) {
	header, err := r.header.build(cfg)
	if err != nil {
		return nil, err
	}

	if len(r.inserts) == 0 {
		return nil, ErrEmptyInserts
	}

	reqs := make([]*greptimepb.InsertRequest, 0, len(r.inserts))
	for _, insert := range r.inserts {
		req, err := insert.build()
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
	}

	req := greptimepb.GreptimeRequest_Inserts{
		Inserts: &greptimepb.InsertRequests{Inserts: reqs},
	}

	return &greptimepb.GreptimeRequest{
		Header:  header,
		Request: &req,
	}, nil

}

// InsertRequest insert metric to specified table. You can also specify the database in header.
type InsertRequest struct {
	table  string
	metric Metric
}

func (r *InsertRequest) WithTable(table string) *InsertRequest {
	r.table = table
	return r
}

func (r *InsertRequest) WithMetric(metric Metric) *InsertRequest {
	r.metric = metric
	return r
}

func (r *InsertRequest) RowCount() uint32 {
	return uint32(len(r.metric.series))
}

func (r *InsertRequest) build() (*greptimepb.InsertRequest, error) {
	if isEmptyString(r.table) {
		return nil, ErrEmptyTable
	}

	columns, err := r.metric.intoGreptimeColumn()
	if err != nil {
		return nil, err
	}

	return &greptimepb.InsertRequest{
		TableName:    r.table,
		Columns:      columns,
		RowCount:     r.RowCount(),
		RegionNumber: 0,
	}, nil
}
