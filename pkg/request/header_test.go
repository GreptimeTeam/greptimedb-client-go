package request

import (
	"testing"

	"github.com/stretchr/testify/assert"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

func TestHeaderBuild(t *testing.T) {
	h := &Header{}

	gh, err := h.buildRequestHeader("", "")
	assert.ErrorIs(t, err, ErrEmptyDatabase)
	assert.Nil(t, gh)

	gh, err = h.buildRequestHeader("catalog", "database")
	assert.Nil(t, err)
	assert.Equal(t, &greptime.RequestHeader{
		Catalog: "catalog",
		Schema: "database",
	}, gh)
	
	h.WithCatalog("a").WithDatabase("b")
	gh, err = h.buildRequestHeader("catalog", "database")
	assert.Nil(t, err)
	assert.Equal(t, &greptime.RequestHeader{
		Catalog: "a",
		Schema: "b",
	}, gh)
}