package request

import (
	"context"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type StreamClient struct {
	client greptime.GreptimeDatabase_HandleRequestsClient
	cfg    *Config
}

func (c *StreamClient) Send(ctx context.Context, req InsertRequest) error {
	request, err := req.Build(c.cfg)
	if err != nil {
		return err
	}
	request.Header.Authorization = c.cfg.buildAuth()

	return c.client.Send(request)
}

func (c *StreamClient) CloseAndRecv(ctx context.Context) (*greptime.AffectedRows, error) {
	resp, err := c.client.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return resp.GetAffectedRows(), nil
}
