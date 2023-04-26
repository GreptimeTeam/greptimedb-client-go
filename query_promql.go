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
	"fmt"
	"strconv"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

var (
	_ query = (*InstantPromql)(nil)
	_ query = (*RangePromql)(nil)
)

// InstantPromql helps to fire a request to greptimedb compatible with Prometheus instant query,
// you can visit [instant query] for detail.
//
// [instant query]: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
type InstantPromql struct {
	Query string
	Ts    time.Time
}

func NewInstantPromql(query string) *InstantPromql {
	return &InstantPromql{query, time.Now()}
}

// WithQuery helps to update the query
func (ip *InstantPromql) WithQuery(query string) *InstantPromql {
	ip.Query = query
	return ip
}

// WithTime to specify the evaluation time. Default is now.
func (ip *InstantPromql) WithTime(ts time.Time) *InstantPromql {
	ip.Ts = ts
	return ip
}

func (ip *InstantPromql) check() error {
	if isEmptyString(ip.Query) {
		return ErrEmptyPromql
	}

	return nil
}

func (ip *InstantPromql) buildGreptimeRequest(header *greptimepb.RequestHeader) (*greptimepb.GreptimeRequest, error) {
	return nil, ErrNotImplemented
}

func (ip *InstantPromql) buildPromqlRequest(header *greptimepb.RequestHeader) (*greptimepb.PromqlRequest, error) {
	if err := ip.check(); err != nil {
		return nil, err
	}

	promql := &greptimepb.PromqlRequest_InstantQuery{
		InstantQuery: &greptimepb.PromInstantQuery{
			Query: ip.Query,
		},
	}

	if !ip.Ts.IsZero() {
		promql.InstantQuery.Time = fmt.Sprintf("%d", ip.Ts.Unix())
	}

	request := &greptimepb.PromqlRequest{
		Header: header,
		Promql: promql,
	}

	return request, nil
}

// RangePromql helps to fire a request to greptimedb compatible with Prometheus range query,
// you can visit [range query] for detail.
//
// [range query]: https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
type RangePromql struct {
	Query string
	Start time.Time
	End   time.Time
	Step  time.Duration
}

func NewRangePromql(query string) *RangePromql {
	return &RangePromql{
		Query: query,
	}
}

// WithQuery helps to update the query
func (rp *RangePromql) WithQuery(query string) *RangePromql {
	rp.Query = query
	return rp
}

// WithStart helps to specify the start of the range
func (rp *RangePromql) WithStart(start time.Time) *RangePromql {
	rp.Start = start
	return rp
}

// WithEnd helps to specify the end of the range
func (rp *RangePromql) WithEnd(end time.Time) *RangePromql {
	rp.End = end
	return rp
}

// WithStep helps to specify the step of the range
func (rp *RangePromql) WithStep(step time.Duration) *RangePromql {
	rp.Step = step
	return rp
}

func (rp *RangePromql) check() error {
	if isEmptyString(rp.Query) {
		return ErrEmptyPromql
	}

	if rp.Start.IsZero() || rp.End.IsZero() {
		return ErrEmptyRange
	}

	if rp.Step <= 0 {
		return ErrEmptyStep
	}

	return nil
}

func (rp *RangePromql) toGreptimedbPromRangeQuery() *greptimepb.PromRangeQuery {
	return &greptimepb.PromRangeQuery{
		Query: rp.Query,
		Start: fmt.Sprintf("%d", rp.Start.Unix()),
		End:   fmt.Sprintf("%d", rp.End.Unix()),
		Step:  strconv.FormatFloat(rp.Step.Seconds(), 'f', -1, 64),
	}
}

func (rp *RangePromql) buildGreptimeRequest(header *greptimepb.RequestHeader) (*greptimepb.GreptimeRequest, error) {
	if err := rp.check(); err != nil {
		return nil, err
	}

	request := &greptimepb.GreptimeRequest_Query{
		Query: &greptimepb.QueryRequest{
			Query: &greptimepb.QueryRequest_PromRangeQuery{
				PromRangeQuery: rp.toGreptimedbPromRangeQuery(),
			},
		},
	}

	return &greptimepb.GreptimeRequest{
		Header:  header,
		Request: request,
	}, nil
}

func (rp *RangePromql) buildPromqlRequest(header *greptimepb.RequestHeader) (*greptimepb.PromqlRequest, error) {
	request := &greptimepb.PromqlRequest{
		Header: header,
		Promql: &greptimepb.PromqlRequest_RangeQuery{
			RangeQuery: rp.toGreptimedbPromRangeQuery(),
		},
	}

	return request, nil
}
