package sql

import (
	"context"
	"database/sql/driver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	req "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connector struct {
	cfg *req.Config
}

// TODO(yuanbohan): auth(handshake), timeout, etc.
// method of driver.Connector interface
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// TODO(yuanbohan): move the options to be parameterr
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	c.cfg.WithDialOptions(options...)

	// TODO(yuanbohan): use connection pool
	client, err := req.NewClient(c.cfg)
	if err != nil {
		return nil, err
	}

	return &connection{client}, nil
}

// method of driver.Connector interface
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
