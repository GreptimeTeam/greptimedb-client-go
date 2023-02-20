package client

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/flight"

	"GreptimeTeam/greptimedb-client-go/pkg/config"
	"GreptimeTeam/greptimedb-client-go/pkg/pb/query"
)

type Client struct {
	Client flight.Client
}

// New will create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func New(cfg *config.Config) (*Client, error) {
	// FIXME(yuanbohan): use real auth and middleware parameters
	client, err := flight.NewClientWithMiddleware(cfg.Address, nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	return &Client{client}, nil
}

// Write ...
func (c *Client) Insert(ctx context.Context) error {
	return errors.New("")
}

// Read ...
func (c *Client) Query(ctx context.Context, req query.Request) error {

	return errors.New("")
}
