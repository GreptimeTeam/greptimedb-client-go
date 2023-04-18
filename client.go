package greptime

import (
	"context"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	cfg *Config
	// For `query`, since unary calls have not been inplemented for query and only do_get helps
	flightClient flight.Client
	// For `insert`, unary calls are supported
	greptimeClient greptimepb.GreptimeDatabaseClient
}

// NewClient will create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func NewClient(cfg *Config) (*Client, error) {
	// TODO(yuanbohan): use real auth and middleware parameters
	flightClient, err := flight.NewClientWithMiddleware(cfg.Address, nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(cfg.Address, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	greptimeClient := greptimepb.NewGreptimeDatabaseClient(conn)

	return &Client{
		cfg:            cfg,
		flightClient:   flightClient,
		greptimeClient: greptimeClient,
	}, nil
}

func (c *Client) Insert(ctx context.Context, req InsertRequest) (*greptimepb.AffectedRows, error) {
	request, err := req.Build(c.cfg)
	if err != nil {
		return nil, err
	}

	response, err := c.greptimeClient.Handle(ctx, request)
	if err != nil {
		return nil, err
	}

	return response.GetAffectedRows(), nil
}

// Query data from greptimedb
func (c *Client) Query(ctx context.Context, req QueryRequest) (*Metric, error) {
	request, err := req.Build(c.cfg)
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}

	sr, err := c.flightClient.DoGet(ctx, &flight.Ticket{Ticket: b})
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(sr)
	if err != nil {
		return nil, err
	}

	return buildMetricFromReader(reader)
}
