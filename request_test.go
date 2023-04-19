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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	rb := &QueryRequest{}
	request, err := rb.Build(&Config{})
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyDatabase)

	rb.WithDatabase("disk_usage")
	request, err = rb.Build(&Config{})
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyQuery)

	// test Sql
	rb.WithSql("select * from monitor")
	request, err = rb.Build(&Config{})
	assert.NotNil(t, request)
	assert.Nil(t, err)

	// reset Sql to test RangePromql
	rb.WithSql("")
	rp := RangePromql{
		Query: "up == 0",
		Start: time.Now(),
		End:   time.Now(),
		Step:  "10s",
	}
	rb.WithRangePromql(rp)
	request, err = rb.Build(&Config{})
	assert.NotNil(t, request)
	assert.Nil(t, err)
}

func TestInsertBuilder(t *testing.T) {
	r := InsertRequest{}

	// empty database
	req, err := r.Build(&Config{})
	assert.Equal(t, ErrEmptyDatabase, err)
	assert.Nil(t, req)

	// empty table
	r.header = header{"public"}
	req, err = r.Build(&Config{})
	assert.Equal(t, ErrEmptyTable, err)
	assert.Nil(t, req)

	// empty series
	r.WithTable("monitor")
	req, err = r.Build(&Config{})
	assert.Equal(t, ErrNoSeriesInMetric, err)
	assert.Nil(t, req)
}
