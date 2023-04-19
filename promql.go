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

// RangePromql helps to fire a request to greptimedb compatible with Prometheus, you can visit
// [query range] for detail.
//
// [query range]: https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries
type RangePromql struct {
	Query string
	Start time.Time
	End   time.Time
	Step  string
}

// WithStartSecond helps to specify the start field in unix second.
func (rp *RangePromql) WithStartSecond(second int64) *RangePromql {
	rp.Start = time.Unix(second, 0)
	return rp
}

// WithEndSecond helps to specify the end field in unix second.
func (rp *RangePromql) WithEndSecond(second int64) *RangePromql {
	rp.End = time.Unix(second, 0)
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

func (rp *RangePromql) Build() *greptimepb.QueryRequest_PromRangeQuery {
	return &greptimepb.QueryRequest_PromRangeQuery{
		PromRangeQuery: &greptimepb.PromRangeQuery{
			Query: rp.Query,
			Start: fmt.Sprintf("%d", rp.Start.Unix()),
			End:   fmt.Sprintf("%d", rp.End.Unix()),
			Step:  rp.Step,
		},
	}

}
