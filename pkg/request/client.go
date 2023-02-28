package request

import (
	"context"
	"errors"
	"fmt"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
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

func (c *Client) Insert(ctx context.Context, req InsertRequest) (*greptime.AffectedRows, error) {
	request, err := req.Build()
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}

	sr, err := c.Client.DoGet(ctx, &flight.Ticket{Ticket: b})
	if err != nil {
		return nil, err
	}

	data, err := sr.Recv()
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, errors.New("the grpc response is empty")
	}

	metadata := greptime.FlightMetadata{}
	err = proto.Unmarshal(data.AppMetadata, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response, err: %+v", err)
	}

	affectedRows := metadata.GetAffectedRows()

	// TODO(vinland-avalon): Embed the function into database/sql framework and wrap the retuen value
	return affectedRows, nil
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
