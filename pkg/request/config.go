package request

import (
	"google.golang.org/grpc"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type Config struct {
	// Address string as host:port
	Address  string `json:"address"`
	UserName string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database`
	Net      string `json:"net"`

	// DialOptions are passed to grpc.DialContext when a new gRPC connection
	// is created.
	DialOptions []grpc.DialOption `json:"-"`
}

// New init Config with addr only
func NewCfg(addr, database string) *Config {
	return &Config{
		Address:  addr,
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

// so far, only support `Basic`, `Token` is not implemented
func (c *Config) buildAuth() *greptime.AuthHeader {
	if len(c.UserName) == 0 {
		return nil
	} else {
		return &greptime.AuthHeader{
			AuthScheme: &greptime.AuthHeader_Basic{
				Basic: &greptime.Basic{
					Username: c.UserName,
					Password: c.Password,
				},
			},
		}
	}
}
