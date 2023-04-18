package greptime

import (
	"context"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"google.golang.org/grpc"
)

// StreamClient is only for inserting
type StreamClient struct {
	client greptimepb.GreptimeDatabase_HandleRequestsClient
	cfg    *Config
}

// NewStreamClient helps to create a stream insert client.
// If Client has performance issue, you can try the stream client.
func NewStreamClient(cfg *Config) (*StreamClient, error) {
	conn, err := grpc.Dial(cfg.getGRPCAddr(), cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	client, err := greptimepb.NewGreptimeDatabaseClient(conn).HandleRequests(context.Background(), cfg.CallOptions...)
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
