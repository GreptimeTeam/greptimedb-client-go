package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"GreptimeTeam/greptimedb-client-go/pkg/pb"
)

func TestQueryBuilder(t *testing.T) {
	rb := &Request{}
	req, err := rb.IntoGreptimeRequest()
	assert.Nil(t, req)
	assert.ErrorIs(t, err, pb.EmptyDatabaseError)

	rb.WithDatabase("disk_usage")
	req, err = rb.IntoGreptimeRequest()
	assert.Nil(t, req)
	assert.ErrorIs(t, err, pb.EmptySqlError)

	rb.WithSql("select * from monitor")
	req, err = rb.IntoGreptimeRequest()
	assert.NotNil(t, req)
	assert.Nil(t, err)
}
