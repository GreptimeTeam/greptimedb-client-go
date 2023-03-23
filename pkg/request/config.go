package request

import (
	"google.golang.org/grpc"
)

type Config struct {
	// Address string as host:port
	Address  string `json:"address"`
	UserName string `json:"username"`
	Password string `json:"password"`
	Catalog  string `json:"catalog"`
	Database string `json:"database"`
	Net      string `json:"net"`

	// DialOptions are passed to grpc.DialContext when a new gRPC connection
	// is created.
	DialOptions []grpc.DialOption `json:"-"`
}

// New init Config with addr only
func NewCfg(addr, catalog, database string) *Config {
	return &Config{
		Address:  addr,
		Catalog:  catalog,
		Database: database,
	}
}

func (c *Config) WithUserName(username string) *Config {
	c.UserName = username
	return c
}

func (c *Config) WithPassword(password string) *Config {
	c.Password = password
	return c
}

// AppendDialOption append one grpc dial option
func (c *Config) WithDialOptions(options ...grpc.DialOption) *Config {
	if c.DialOptions == nil {
		c.DialOptions = []grpc.DialOption{}
	}

	c.DialOptions = append(c.DialOptions, options...)

	return c
}
