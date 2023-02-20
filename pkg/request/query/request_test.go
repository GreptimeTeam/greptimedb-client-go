package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

func TestQueryBuilder(t *testing.T) {
	rb := &Request{}
	request, err := rb.IntoGreptimeRequest()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, req.EmptyDatabaseError)

	rb.WithDatabase("disk_usage")
	request, err = rb.IntoGreptimeRequest()
	assert.Nil(t, request)
	assert.ErrorIs(t, err, req.EmptySqlError)

	rb.WithSql("select * from monitor")
	request, err = rb.IntoGreptimeRequest()
	assert.NotNil(t, request)
	assert.Nil(t, err)
}
