package request

import (
	"testing"

	"github.com/stretchr/testify/assert"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func TestHeaderBuild(t *testing.T) {
	h := &Header{}

	gh, err := h.buildRequestHeader(&Config{})
	assert.ErrorIs(t, err, ErrEmptyDatabase)
	assert.Nil(t, gh)

	gh, err = h.buildRequestHeader(&Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptime.RequestHeader{
		Dbname: "database",
	}, gh)

	h.WithDatabase("b")
	gh, err = h.buildRequestHeader(&Config{Database: "database"})
	assert.Nil(t, err)
	assert.Equal(t, &greptime.RequestHeader{
		Dbname: "b",
	}, gh)
}
