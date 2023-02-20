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
	assert.ErrorIs(t, err, pb.EmptyCatalogError)

	rb.WithCatalog("greptime")
	rb.WithDatabase("disk_usage")
	rb.WithSql("select * from monitor")
	req, err = rb.IntoGreptimeRequest()
	assert.NotNil(t, req)
	assert.Nil(t, err)
}
