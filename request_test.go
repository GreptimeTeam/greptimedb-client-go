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
