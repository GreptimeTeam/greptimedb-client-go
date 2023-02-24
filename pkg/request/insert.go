package request

import (
	"errors"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"GreptimeTeam/greptimedb-client-go/pkg/model"
)

type InsertRequest struct {
	Header
	Series []model.Series
}

func (r *InsertRequest) WithSeries(series []model.Series) *InsertRequest {
	r.Series = series
	return r
}

func (r *InsertRequest) Build() (*greptime.GreptimeRequest, error) {

	return nil, errors.New("not implemented")
}
