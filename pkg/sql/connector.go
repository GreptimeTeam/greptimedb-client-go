package sql

import (
	"context"
	"database/sql/driver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	req "GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connector struct {
	cfg *req.Config
}

// TODO(yuanbohan): auth(handshake), timeout, etc.
// method of driver.Connector interface
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {

	// FIXME(yuanbohan): move the options to be parameter
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	c.cfg.WithDialOptions(options...)

	client, err := req.NewClient(c.cfg)
	if err != nil {
		return nil, err
	}

	conn := &connection{client}

	return conn, nil
}

// method of driver.Connector interface
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
