package greptime

import (
	"context"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"
)

// StreamClient is for inserting
type StreamClient struct {
	client greptimepb.GreptimeDatabase_HandleRequestsClient
	cfg    *Config
}

// NewStreamClient has better performance when inserting high cardinality data
func NewStreamClient(cfg *Config, opts ...grpc.CallOption) (*StreamClient, error) {
	conn, err := grpc.Dial(cfg.Address, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	client, err := greptimepb.NewGreptimeDatabaseClient(conn).HandleRequests(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	return &StreamClient{client: client, cfg: cfg}, nil
}

func (c *StreamClient) Send(ctx context.Context, req InsertRequest) error {
	request, err := req.Build(c.cfg)
	if err != nil {
		return err
	}

	return c.client.Send(request)
}

func (c *StreamClient) CloseAndRecv(ctx context.Context) (*greptimepb.AffectedRows, error) {
	resp, err := c.client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return resp.GetAffectedRows(), nil
}
