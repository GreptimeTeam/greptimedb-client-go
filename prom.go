package greptime

import (
	"fmt"
	"time"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

// RangePromql helps to construct query_range Prometheus request, you can visit
// [query_range](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) for detail.
type RangePromql struct {
	Query string
	Start time.Time
	End   time.Time
	Step  string
}

func (rp *RangePromql) WithStartSecond(second int64) *RangePromql {
	rp.Start = time.Unix(second, 0)
	return rp
}

func (rp *RangePromql) WithEndSecond(second int64) *RangePromql {
	rp.End = time.Unix(second, 0)
	return rp
}

func (rp *RangePromql) check() error {
	if IsEmptyString(rp.Query) {
		return ErrEmptyQuery
	}

	if IsEmptyString(rp.Step) {
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
