package greptime

import (
	"fmt"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"
)

// Config is to define how the Client behaves.
//
//   - Addr is 127.0.0.1 in local environment. You can find the address in
//     the detail page if you create one service in GreptimeCloud
//   - Port default value is 4001
//   - Username and Password can be left to empty in local environment.
//     you can find them in GreptimeCloud service detail page.
//   - Database is the default database the client will operate on.
//     But you can change the database in InsertRequest or QueryRequest.
//   - DialOptions and CallOptions are for gRPC service.
//     You can specify them or leave them empty.
type Config struct {
	Addr     string // example: 127.0.0.1
	Port     int    // default: 4001
	Username string
	Password string
	Database string // the default database for client

	// DialOptions are passed to grpc.DialContext
	// when a new gRPC connection is to be created.
	DialOptions []grpc.DialOption

	// CallOptions are passed to StreamClient
	CallOptions []grpc.CallOption
}

// NewCfg helps to init Config with addr only
func NewCfg(addr string) *Config {
	return &Config{
		Addr: addr,
		Port: 4001,
	}
}

// WithPort set the Port field. Do not change it if you have no idea what it is.
func (c *Config) WithPort(port int) *Config {
	c.Port = port
	return c
}

// WithDatabase helps to specify the default database the client operates on.
func (c *Config) WithDatabase(database string) *Config {
	c.Database = database
	return c
}

// WithAuth helps to specify the Basic Auth username and password
func (c *Config) WithAuth(username, password string) *Config {
	c.Username = username
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

// BuildAuthHeader only supports Basic Auth so far
func (c *Config) BuildAuthHeader() *greptimepb.AuthHeader {
	if isEmptyString(c.Username) || isEmptyString(c.Password) {
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

func (c *Config) getGRPCAddr() string {
	return fmt.Sprintf("%s:%d", c.Addr, c.Port)
}
