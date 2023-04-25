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
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// InstantPromql helps to fire a request to greptimedb compatible with Prometheus instant query,
// you can visit [instant query] for detail.
//
// [instant query]: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
type InstantPromql struct {
	Query string
	ts    time.Time
}

func NewInstantPromql(query string) *InstantPromql {
	return &InstantPromql{
		Query: query,
		ts:    time.Now(),
	}
}

// WithQuery helps to update the query
func (ip *InstantPromql) WithQuery(query string) *InstantPromql {
	ip.Query = query
	return ip
}

// WithTime to specify the evaluation time. Default is now.
func (ip *InstantPromql) WithTime(ts time.Time) *InstantPromql {
	ip.ts = ts
	return ip
}

func (ip *InstantPromql) check() error {
	if isEmptyString(ip.Query) {
		return ErrEmptyQuery
	}

	return nil
}

// buildPromqlRequest helps to construct a InstantQuery, expecting the response is totally
// the same as Prometheus [instant query]
//
// [instant query]: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries
func (ip *InstantPromql) buildPromqlRequest() *greptimepb.PromqlRequest_InstantQuery {
	query := &greptimepb.PromInstantQuery{
		Query: ip.Query,
	}
	if !ip.ts.IsZero() {
		query.Time = fmt.Sprintf("%d", ip.ts.Unix())
	}
	return &greptimepb.PromqlRequest_InstantQuery{
		InstantQuery: query,
	}
}

// RangePromql helps to fire a request to greptimedb compatible with Prometheus range query,
// you can visit [range query] for detail.
//
// [range query]: https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
type RangePromql struct {
	Query string
	Start time.Time
	End   time.Time
	Step  string
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
func (rp *RangePromql) WithStep(step string) *RangePromql {
	rp.Step = step
	return rp
}

func (rp *RangePromql) check() error {
	if isEmptyString(rp.Query) {
		return ErrEmptyQuery
	}

	if isEmptyString(rp.Step) {
		return ErrEmptyStep
	}

	return nil
}

// buildPromqlRequest helps to construct a RangeQuery, expecting the response is totally
// the same as Prometheus [range query]
//
// [range query]: https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
func (rp *RangePromql) buildPromqlRequest() *greptimepb.PromqlRequest_RangeQuery {
	return &greptimepb.PromqlRequest_RangeQuery{
		RangeQuery: &greptimepb.PromRangeQuery{
			Query: rp.Query,
			Start: fmt.Sprintf("%d", rp.Start.Unix()),
			End:   fmt.Sprintf("%d", rp.End.Unix()),
			Step:  rp.Step,
		},
	}
}

func (rp *RangePromql) buildQueryRequest() *greptimepb.QueryRequest_PromRangeQuery {
	return &greptimepb.QueryRequest_PromRangeQuery{
		PromRangeQuery: &greptimepb.PromRangeQuery{
			Query: rp.Query,
			Start: fmt.Sprintf("%d", rp.Start.Unix()),
			End:   fmt.Sprintf("%d", rp.End.Unix()),
			Step:  rp.Step,
		},
	}
}
