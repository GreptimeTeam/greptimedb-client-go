// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package greptime

import (
	"context"

	greptimepb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Client helps to Insert/Query data Into/From GreptimeDB. A Client is safe for concurrent
// use by multiple goroutines,you can have one Client instance in your application.
type Client struct {
	cfg *Config
	// For `query`, since unary calls have not been inplemented for query and only do_get helps
	flightClient flight.Client
	// For `insert`, unary calls are supported
	greptimeClient greptimepb.GreptimeDatabaseClient
}

// NewClient helps to create the greptimedb client, which will be responsible Write/Read data To/From GreptimeDB
func NewClient(cfg *Config) (*Client, error) {
	// TODO(yuanbohan): use real auth and middleware parameters
	flightClient, err := flight.NewClientWithMiddleware(cfg.getGRPCAddr(), nil, nil, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(cfg.getGRPCAddr(), cfg.DialOptions...)
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

// Insert helps to insert multiple rows into greptimedb
func (c *Client) Insert(ctx context.Context, req InsertRequest) (uint32, error) {
	request, err := req.Build(c.cfg)
	if err != nil {
		return 0, err
	}

	response, err := c.greptimeClient.Handle(ctx, request)
	if err != nil {
		return 0, err
	}

	return response.GetAffectedRows().Value, nil
}

// Query helps to retrieve data from greptimedb
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
