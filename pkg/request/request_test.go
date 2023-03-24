package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	rb := &QueryRequest{}
	request, err := rb.Build("", "")
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyDatabase)

	rb.WithDatabase("disk_usage")
	request, err = rb.Build("", "")
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyQuery)

	rb.WithSql("select * from monitor")
	request, err = rb.Build("", "")
	assert.NotNil(t, request)
	assert.Nil(t, err)
}

func TestInsertBuilder(t *testing.T) {
	r := InsertRequest{}
	// empty database
	req, err := r.Build("", "")
	assert.Equal(t, ErrEmptyDatabase, err)
	assert.Nil(t, req)

	// empty table
	r.Database = "public"
	req, err = r.Build("", "")
	assert.Equal(t, ErrEmptyTable, err)
	assert.Nil(t, req)

	// empty series
	r.WithTable("monitor")
	req, err = r.Build("", "")
	assert.Equal(t, ErrNoSeriesInMetric, err)
	assert.Nil(t, req)
}
