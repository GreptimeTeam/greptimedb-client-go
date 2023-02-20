package query

import (
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type Request struct {
	req.Header        // required
	Sql        string // required
}

func (r *Request) WithSql(sql string) *Request {
	r.Sql = sql
	return r
}

func (r *Request) IsSqlEmpty() bool {
	return len(strings.TrimSpace(r.Sql)) == 0
}

func (r *Request) IntoGreptimeRequest() (*greptime.GreptimeRequest, error) {
	if r.IsDatabaseEmpty() {
		return nil, req.EmptyDatabaseError
	}

	if r.IsSqlEmpty() {
		return nil, req.EmptySqlError
	}

	header := &greptime.RequestHeader{
		Catalog: r.Catalog,
		Schema:  r.Datadase,
	}

	query := &greptime.GreptimeRequest_Query{
		Query: &greptime.QueryRequest{
			Query: &greptime.QueryRequest_Sql{
				Sql: r.Sql,
			},
		},
	}

	return &greptime.GreptimeRequest{
		Header:  header,
		Request: query,
	}, nil
}
