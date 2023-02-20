package query

import (
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	"GreptimeTeam/greptimedb-client-go/pkg/pb"
)

type Request struct {
	Catalog  string
	Datadase string
	Sql      string
}

func (r *Request) WithCatalog(catalog string) *Request {
	r.Catalog = catalog
	return r
}

func (r *Request) WithDatabase(database string) *Request {
	r.Datadase = database
	return r
}

func (r *Request) WithSql(sql string) *Request {
	r.Sql = sql
	return r
}

func (r *Request) IntoGreptimeRequest() (*greptime.GreptimeRequest, error) {
	if len(strings.TrimSpace(r.Catalog)) == 0 {
		return nil, pb.EmptyCatalogError
	}

	if len(strings.TrimSpace(r.Datadase)) == 0 {
		return nil, pb.EmptyDatabaseError
	}

	if len(strings.TrimSpace(r.Sql)) == 0 {
		return nil, pb.EmptySqlError
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
