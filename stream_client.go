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
	request, err := req.build(c.cfg)
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
