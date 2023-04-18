package greptime

import (
	"testing"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/stretchr/testify/assert"
)

func TestHeaderBuild(t *testing.T) {
	h := &header{}

	gh, err := h.Build(&Config{})
	assert.ErrorIs(t, err, ErrEmptyDatabase)
	assert.Nil(t, gh)

	gh, err = h.Build(&Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptimepb.RequestHeader{
		Dbname: "database",
	}, gh)

	h.database = "db_in_header"
	gh, err = h.Build(&Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptimepb.RequestHeader{
		Dbname: "db_in_header",
	}, gh)
}
