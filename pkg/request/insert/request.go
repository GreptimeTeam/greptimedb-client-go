package insert

import (
	"strings"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type Request struct {
	req.Header
	Table string
	Data  any
}

func (r *Request) WithTable(table string) *Request {
	r.Table = table
	return r
}

func (r *Request) WithData(data string) *Request {
	r.Data = data
	return r
}

func (r *Request) IsTableEmpty() bool {
	return len(strings.TrimSpace(r.Table)) == 0
}

func (r *Request) IntoGreptimeRequest() (*greptime.GreptimeRequest, error) {

	return nil, nil
}
