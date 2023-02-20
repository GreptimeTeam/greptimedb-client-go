package client

import (
	"context"
	"errors"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/protobuf/proto"

	"GreptimeTeam/greptimedb-client-go/pkg/config"
	"GreptimeTeam/greptimedb-client-go/pkg/request/query"
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

// Query data from greptimedb via SQL.
//
// Release reduces the reference count for the reader.
//
// reader, err := client.Query(ctx, req)
// defer reader.Release()
func (c *Client) Query(ctx context.Context, req query.Request) (*flight.Reader, error) {
	request, err := req.IntoGreptimeRequest()
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}

	// TODO(yuanbohan): more options here
	sr, err := c.Client.DoGet(ctx, &flight.Ticket{Ticket: b})
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(sr)
	if err != nil {
		return nil, err
	}

	return reader, nil
}
