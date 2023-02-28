package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	rb := &QueryRequest{}
	request, err := rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptyDatabase)

	rb.WithDatabase("disk_usage")
	request, err = rb.Build()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, ErrEmptySql)

	rb.WithSql("select * from monitor")
	request, err = rb.Build()
	assert.NotNil(t, request)
	assert.Nil(t, err)
}
