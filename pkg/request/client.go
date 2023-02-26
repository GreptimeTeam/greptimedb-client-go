package request

import (
	"context"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	Client flight.Client
	Cfg    *Config
}

// New will create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func NewClient(cfg *Config) (*Client, error) {
	// TODO(yuanbohan): use real auth and middleware parameters
	client, err := flight.NewClientWithMiddleware(cfg.Address, nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	return &Client{
		Client: client,
		Cfg:    cfg,
	}, nil
}

func (c *Client) Insert(ctx context.Context, req InsertRequest) error {
	request, err := req.Build()
	if err != nil {
		return err
	}

	_, err = proto.Marshal(request)
	if err != nil {
		return err
	}

	// TODO(yuanbohan): real logic here

	return nil
}

// Query data from greptimedb via SQL.
//
// Release reduces the reference count for the reader.
//
// reader, err := client.Query(ctx, req)
// defer reader.Release()
func (c *Client) Query(ctx context.Context, req QueryRequest) (*flight.Reader, error) {
	request, err := req.Build()
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
