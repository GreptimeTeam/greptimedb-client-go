package request

import "google.golang.org/grpc"

type Config struct {
	// Address string as host:port
	Address  string `json:"address"`
	Catalog  string
	Database string // the default database if not specified

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

// AppendDialOption append one grpc dial option
func (c *Config) WithDialOptions(options ...grpc.DialOption) *Config {
	if c.DialOptions == nil {
		c.DialOptions = []grpc.DialOption{}
	}

	c.DialOptions = append(c.DialOptions, options...)

	return c
}
