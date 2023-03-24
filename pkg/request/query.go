package request

import (
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type QueryRequest struct {
	Header        // required
	Sql    string // required
	promQL *PromQL
}

func (r *QueryRequest) WithSql(sql string) *QueryRequest {
	r.Sql = sql
	return r
}

func (r *QueryRequest) WithPromQL(promQL *PromQL) *QueryRequest {
	r.promQL = promQL
	return r
}

func (r *QueryRequest) IsSqlEmpty() bool {
	return len(strings.TrimSpace(r.Sql)) == 0
}

// TODO(vinland-avalon): check each field
func (r *QueryRequest) IsPromQLEmpty() bool {
	return r.promQL == nil
}

func (r *QueryRequest) Build(catalog, database string) (*greptime.GreptimeRequest, error) {
	header, err := r.Header.buildRequestHeader(catalog, database)
	if err != nil {
		return nil, err
	}

	request := &greptime.GreptimeRequest_Query{
		Query: &greptime.QueryRequest{},
	}

	if !r.IsSqlEmpty() {
		request.Query.Query = &greptime.QueryRequest_Sql{Sql: r.Sql}
	} else {
		if !r.IsPromQLEmpty() {
			request.Query.Query = &greptime.QueryRequest_PromRangeQuery{
				PromRangeQuery: &greptime.PromRangeQuery{
					Query: r.promQL.Query,
					Start: r.promQL.Start,
					End:   r.promQL.End,
					Step:  r.promQL.Step,
				},
			}
		} else {
			return nil, ErrEmptyQuery
		}
	}

	return &greptime.GreptimeRequest{
		Header:  header,
		Request: request,
	}, nil
}
