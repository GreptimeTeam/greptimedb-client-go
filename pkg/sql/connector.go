package sql

import (
	"context"
	"database/sql/driver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	req "github.com/GreptimeTeam/greptimedb-client-go/pkg/request"
)

type connector struct {
	cfg  *req.Config
	conn *connection
}

// TODO(yuanbohan): auth(handshake), timeout, etc.
// method of driver.Connector interface
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.conn != nil {
		return c.conn, nil
	}

	// TODO(yuanbohan): move the options to be parameterr
	if c.cfg.DialOptions == nil {
		options := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// grpc.WithDefaultCallOptions(
			// 	grpc.MaxCallRecvMsgSize(1),
			// 	grpc.MaxCallSendMsgSize(1),
			//  grpc.MaxRetryRPCBufferSize(1)),
		}
		c.cfg.WithDialOptions(options...)
	}

	client, err := req.NewClient(c.cfg)
	if err != nil {
		return nil, err
	}

	c.conn = &connection{client}
	return c.conn, nil
}

// method of driver.Connector interface
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
