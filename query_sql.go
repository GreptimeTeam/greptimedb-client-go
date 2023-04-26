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

// Sql helps to fire a request to greptimedb in SQL. It can not be used
// as Promql Query
type Sql struct {
	sql string
}

var (
	_ query = (*Sql)(nil)
)

func (s *Sql) buildGreptimeRequest(header *greptimepb.RequestHeader) (*greptimepb.GreptimeRequest, error) {
	if isEmptyString(s.sql) {
		return nil, ErrEmptySql
	}

	request := &greptimepb.GreptimeRequest_Query{
		Query: &greptimepb.QueryRequest{
			Query: &greptimepb.QueryRequest_Sql{Sql: s.sql},
		},
	}

	return &greptimepb.GreptimeRequest{
		Header:  header,
		Request: request,
	}, nil
}

func (s *Sql) buildPromqlRequest(header *greptimepb.RequestHeader) (*greptimepb.PromqlRequest, error) {
	return nil, ErrSqlInPromql
}
