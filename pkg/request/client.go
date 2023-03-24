package request

import (
	"context"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	greptime "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
)

type Client struct {
	Cfg *Config
	// For `query`, since unary calls have not been inplemented for query and only do_get helps
	FlightClient flight.Client
	// For `insert`, unary calls are supported
	DatabaseClient greptime.GreptimeDatabaseClient
}

// New will create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func NewClient(cfg *Config) (*Client, error) {
	// TODO(yuanbohan): use real auth and middleware parameters
	client, err := flight.NewClientWithMiddleware(cfg.Address, nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(cfg.Address, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	databaseClient := greptime.NewGreptimeDatabaseClient(conn)

	return &Client{
		FlightClient:   client,
		Cfg:            cfg,
		DatabaseClient: databaseClient,
	}, nil
}

func (c *Client) Insert(ctx context.Context, req InsertRequest) (*greptime.AffectedRows, error) {
	request, err := req.Build()
	if err != nil {
		return nil, err
	}
	request.Header.Authorization = c.buildAuth()

	response, err := c.DatabaseClient.Handle(ctx, request)
	if err != nil {
		return nil, err
	}

	return response.GetAffectedRows(), nil
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
	request.Header.Authorization = c.buildAuth()

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}

	// TODO(yuanbohan): more options here
	sr, err := c.FlightClient.DoGet(ctx, &flight.Ticket{Ticket: b})
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(sr)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

// Query data from greptimedb via SQL.
func (c *Client) QueryMetric(ctx context.Context, req QueryRequest) (*Metric, error) {
	request, err := req.Build()
	if err != nil {
		return nil, err
	}
	request.Header.Authorization = c.buildAuth()

	b, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}

	sr, err := c.FlightClient.DoGet(ctx, &flight.Ticket{Ticket: b})
	if err != nil {
		return nil, err
	}

	reader, err := flight.NewRecordReader(sr)
	if err != nil {
		return nil, err
	}

	return buildMetricWithReader(reader)
}

// so far, only support `Basic`, `Token` is not implemented
func (c *Client) buildAuth() *greptime.AuthHeader {
	if len(c.Cfg.UserName) == 0 {
		return nil
	} else {
		return &greptime.AuthHeader{
			AuthScheme: &greptime.AuthHeader_Basic{
				Basic: &greptime.Basic{
					Username: c.Cfg.UserName,
					Password: c.Cfg.Password,
				},
			},
		}
	}
}
