package request

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewCfgWithHost(t *testing.T) {
	cfg := NewCfgWithHost("localhost", "catalog", "database").WithUserName("user").WithPassword("pwd")
	expectedCfg := &Config{
		Address:  "localhost:4001",
		Catalog:  "catalog",
		Database: "database",
		UserName: "user",
		Password: "pwd",
	}
	assert.Equal(t, expectedCfg, cfg)
}
