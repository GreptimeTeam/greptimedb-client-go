package greptime

import (
	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"
)

type Config struct {
	// Address format: <host:port>
	Address  string
	Username string
	Password string
	Database string // the default database for client

	// DialOptions are passed to grpc.DialContext
	// when a new gRPC connection is to be created.
	DialOptions []grpc.DialOption

	// CallOptions are passed to StreamClient
	CallOptions []grpc.CallOption
}

// NewCfg init Config with addr only
func NewCfg(addr string) *Config {
	return &Config{
		Address: addr,
	}
}

func (c *Config) WithDatabase(database string) *Config {
	c.Database = database
	return c
}

func (c *Config) WithUserName(username string) *Config {
	c.Username = username
	return c
}

func (c *Config) WithPassword(password string) *Config {
	c.Password = password
	return c
}

func (c *Config) WithDialOptions(options ...grpc.DialOption) *Config {
	if c.DialOptions == nil {
		c.DialOptions = []grpc.DialOption{}
	}

	c.DialOptions = append(c.DialOptions, options...)
	return c
}

func (c *Config) WithCallOptions(options ...grpc.CallOption) *Config {
	if c.CallOptions == nil {
		c.CallOptions = []grpc.CallOption{}
	}

	c.CallOptions = append(c.CallOptions, options...)
	return c
}

// BuildAuthHeader only supports `Basic` so far
func (c *Config) BuildAuthHeader() *greptimepb.AuthHeader {
	if IsEmptyString(c.Username) || IsEmptyString(c.Password) {
		return nil
	}

	return &greptimepb.AuthHeader{
		AuthScheme: &greptimepb.AuthHeader_Basic{
			Basic: &greptimepb.Basic{
				Username: c.Username,
				Password: c.Password,
			},
		},
	}

}
